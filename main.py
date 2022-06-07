from typing import List, Tuple
import pathlib
import logging
import datetime
from enum import Enum, auto
from dataclasses import dataclass
from dataclass_csv import DataclassReader, DataclassWriter, dateformat
import boto3
from boto3.s3.transfer import S3Transfer
from boto3.s3.transfer import TransferConfig
import os.path
import threading
from botocore.errorfactory import ClientError
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor

### ユーザ設定 ### noqa
# バックアップ対象の親ディレクトリ
target_top_path = pathlib.Path(r"\\192.168.0.20\10_Album")
# 親ディレクトリから何階層下をバックアップの単位とするか
target_level = 2
# スキップするファイル・フォルダ
skip_file_list = ["Thumbs.db", ".DS_store"]
skip_folder_list = ["#recycle", "90_Temp", "91_要整理"]
# aws設定
bucket_name = "album-backup"
# アップロードの並列数
upload_concurrency = 10
# debug mode
dry_run = False  # AWSへのアップロードを行わない
Glacier_off = False  # AWS S3 Glacierへのアップロードを行わず、通常のS3にアップロードする
### ユーザ設定ここまで ### noqa

# その他のグローバル変数
csv_path = r"status.csv"
s3_client = boto3.client("s3")
logging.basicConfig(
    filename="log.txt",
    filemode="a",
    level=logging.INFO,
    format="%(asctime)s,%(msecs)d %(levelname)s %(message)s",
)
# add the handler to the root logger
console = logging.StreamHandler()
logging.getLogger().addHandler(console)
logging.info("------------------Executed------------------")

# HACK:
# dataclass_csv　この問題とも関連
# https://github.com/dfurtado/dataclass-csv/issues/51
#
# datetime型の初期化時(new)、保存時(str)関数の動作を上書きするmydatetimeを作成
# new: 文字列からの初期化能にする
# str: デフォルトのstr関数ではmicrosecondが0のとき、"%Y-%m-%d %H:%M:%S.%f"の.%fが出力されないので、出力するようにする
class mydatetime(datetime.datetime):
    def __new__(cls, *args, **kwargs):
        if len(args) == 1 and type(args[0]) is str:
            # Must not conflict with superclass implementations.
            # https://github.com/python/cpython/blob/d174ebe91ebc9f7388a22cc81cdc5f7be8bb8c9b/Lib/datetime.py#L1563
            return super().strptime(args[0], "%Y-%m-%d %H:%M:%S.%f")
        else:
            return super().__new__(cls, *args, *kwargs)

    def __str__(self):
        result = super().__str__()
        if int(self.microsecond) == 0:
            # In the default str function, when microsecond is 0, the . %f" is not output, so make it output.
            return result + ".0"
        else:
            return result


# @accept_whitespaces # HACK: 有効にするとPylanceで補完が効かなくなる
@dataclass()
@dateformat("%Y-%m-%d %H:%M:%S.%f")
class Record:
    class Status(Enum):
        Check = auto()  # 初回またはローカルのファイル数か最終更新日に変更がある
        Check_NotExists = auto()  # ローカルのファイルパスが見つからない
        Check_FileUpload = auto()  # アップロード途中で終了している
        Upload = auto()  # Upload対象（ユーザーが手動で変更する）
        Synchronized = auto()  # アップロード完了

        """
        def __init__(self, val) -> None:
            # コンストラクタとしてstrを受け付ける
            self.val = Record.Status[val]
            super().__init__()
        """

    # NOTE: statusは本来、Status型にすべきだが、dataclass_csvライブラリのためstrとする
    status: str = Status.Check.name  # 状態を表す
    local_path: pathlib.Path = None  # バックアップの対象のパス
    local_file_count: int = None  # local_pathに含まれるファイルの数（Win,Macの管理ファイルを除く）
    local_total_size: int = None  # local_pathに含まれるファイルサイズの合計（Win,Macの管理ファイルを除く）
    local_last_modified: mydatetime = None  # local_pathに含まれるファイルのうち、最新の最終更新日
    sync_start: mydatetime = None  # アップロードを始めた時刻
    sync_end: mydatetime = None  # アップロードが完了した時刻
    aws_arn: str = None  # AWSのARN
    aws_file_count: int = None  # AWSのARNに含まれるファイルの数（Win,Macの管理ファイルを除く）
    aws_total_size: int = None  # AWSのARNに含まれるファイルサイズの合計（Win,Macの管理ファイルを除く）


class RMG(List):
    """
    Record Manager
    """

    csv_path: str = ""

    def __init__(self, csv_path: str):
        """
        読み込み、存在確認
        """
        try:
            with open(csv_path, "r", newline="", encoding="utf-8") as csvfile:
                reader = DataclassReader(csvfile, Record)
                row: Record
                temp: List = list()
                for row in reader:
                    # 存在していたフォルダがなくなっていないか確認
                    if not pathlib.Path(row.local_path).exists():
                        row.status = Record.Status.Check_NotExists.name
                    temp.append(row)

            # 都度保存が走ると嫌なのでまとめて更新
            self.extend(temp)
        except FileNotFoundError:
            pass

    def append(self, value):
        # 都度保存
        super().append(value)
        logging.debug("append")
        self.save()

    def __setitem__(self, key, value):
        # 都度保存
        super().__setitem__(key, value)
        logging.debug("setitem")
        self.save()

    def save(self):
        """
        書き出し
        """
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            w = DataclassWriter(f, self, Record)
            w.write()
        logging.debug("{0} is saved".format(csv_path))

    def __del__(self):
        # 最後にも保存
        # self.save()
        pass

    def get_index_by_path(self, file_path: pathlib.Path) -> int:
        row: Record
        for idx, row in enumerate(self):
            if row.local_path == file_path:
                return idx
        return None

    def addOrUpdate(self, new: Record):
        """
        更新されているかどうか確認し、更新または新規追加する
        """
        index = self.get_index_by_path(new.local_path)
        if index is None:
            # 新規
            self.append(new)
        elif (
            self[index].local_file_count != new.local_file_count
            or self[index].local_last_modified < new.local_last_modified
            or self[index].local_total_size != new.local_total_size
        ):
            # 更新
            # ファイル数が異なる or 最終更新日が新しい or トータルサイズが異なる
            logging.info("{0} バックアップ対象ファイルに変更があります。\n新 {1} \n旧 {2}".format(str(new.local_path), new, self[index]))
            self[index] = new
        else:
            logging.info("{0} No update".format(new.local_path))


def upload_aws_S3(rmg: RMG):
    """
    アップロード
    """
    row: Record
    for idx, row in enumerate(rmg):
        if row.status == Record.Status.Upload.name:
            logging.info("{0} アップロード開始".format(row.local_path))
            row.sync_start = mydatetime.today()
            row.status = Record.Status.Check_FileUpload.name
            rmg[idx] = row

            # マルチスレッドアップロード
            executor = ThreadPoolExecutor(max_workers=upload_concurrency)
            futures = []
            for path in [path for path in row.local_path.glob("**/*") if is_backup_file(path)]:
                aws_key = str(pathlib.Path(str(path.relative_to(target_top_path.parent))).as_posix())
                if not dry_run:
                    if Glacier_off:
                        futures.append(executor.submit(aws_upload, path, aws_key, {}))
                    else:
                        futures.append(executor.submit(aws_upload, path, aws_key, {"StorageClass": "DEEP_ARCHIVE"}))
            executor.shutdown(wait=True)
            for future in futures:
                future.result()  # catch exception

            # 検証
            aws_key_dir = str(row.local_path.relative_to(target_top_path.parent).as_posix())
            aws_file_count, aws_total_size = aws_get_info(bucket_name, aws_key_dir)
            if aws_file_count == row.local_file_count and aws_total_size == row.local_total_size:
                row.sync_end = mydatetime.today()
                row.status = Record.Status.Synchronized.name
                row.aws_arn = aws_key_dir
                row.aws_total_size = aws_total_size
                row.aws_file_count = aws_file_count
                rmg[idx] = row
                logging.info("{0} アップロード完了".format(row.local_path))
            else:
                logging.warning(
                    "{0} AWSのファイル数またはサイズが一致しません. ローカル:ファイル数: {1} ファイルサイズ: {2} AWS:ファイル数: {3} ファイルサイズ: {4}".format(
                        aws_key_dir, row.local_file_count, row.local_total_size, aws_file_count, aws_total_size
                    )
                )


def aws_upload(file_path, aws_key, extra_args):
    """
    AWSにアップロードする
    bucket_name,s3_clientはグローバルアクセスするので注意
    """

    # アップロードの準備
    GB = 1024**3
    config = TransferConfig(multipart_threshold=5 * GB)  # マルチパートアップロードを（ほぼ）しない設定
    s3_transfer = S3Transfer(s3_client, config)

    # 存在確認
    try:
        s3_client.head_object(Bucket=bucket_name, Key=str(aws_key))
    except ClientError as e:
        if e.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
            # AWSに既存していなければ、アップロード
            s3_transfer.upload_file(
                filename=str(file_path),
                bucket=bucket_name,
                key=str(aws_key),
                extra_args=extra_args,
                callback=ProgressPercentage(file_path),
            )
        else:
            # その他のClientError
            logging.warning("Key:{0} {1}".format(file_path, e))
    else:
        logging.warning("Key:{0} 同じkeyが既にS3に存在しているためスキップします。".format(file_path))


def aws_get_info(bucket, aws_key):
    """
    awsのkeyフォルダ以下にあるファイルの数、ファイルサイズの合計値を返す
    """
    s3 = boto3.resource("s3")
    my_bucket = s3.Bucket(bucket)
    total_size = 0
    total_count = 0
    # フォルダ以下を探索するようにキーを修正
    if not aws_key.endswith("/"):
        aws_key = aws_key + "/"

    for obj in my_bucket.objects.filter(Prefix=str(aws_key)):
        total_size += obj.size
        total_count += 1

    return total_count, total_size


def is_backup_file(path: pathlib) -> bool:
    """
    フォルダ、OSの管理ファイル以外がバックアップの対象
    """
    return not (path.is_dir() or path.name in skip_file_list)


def get_folder_info(root_path: pathlib) -> Tuple[int, mydatetime]:
    """
    root_path以下の総ファイルの数と、中身のファイルの最新の最終更新日を返す
    """
    file_count = 0
    total_size = 0
    last_modified = None
    for path in root_path.glob("**/*"):
        if is_backup_file(path):
            # 各ファイルの更新日時からもっとも新しいものを取得
            modified = mydatetime.fromtimestamp(path.stat().st_mtime)
            if last_modified is None or last_modified < modified:
                last_modified = modified
            file_count += 1
            total_size += path.stat().st_size

    return file_count, last_modified, total_size


def make_check_list(rmg: RMG):
    """
    アーカイブ単位の1つ上の階層のパスをもらい、チェックリストを作る
    """
    for tgt_path in target_top_path.glob("*/" * target_level):
        # アーカイブ単位のフォルダでぐるぐる回す
        if tgt_path.is_file():
            # この階層にはファイルはない想定
            logging.warning("想定外の場所にファイルが存在しているためスキップします:{0}".format(str(tgt_path)))
            continue
        if len([path for path in skip_folder_list if path in str(tgt_path)]) >= 1:
            logging.info("{0} スキップします".format(str(tgt_path)))
            continue

        file_count, last_modified, total_size = get_folder_info(tgt_path)

        row: Record = Record()
        row.status = Record.Status.Check.name
        row.local_path = pathlib.Path(tgt_path)
        row.local_file_count = file_count
        row.local_last_modified = last_modified
        row.local_total_size = total_size

        rmg.addOrUpdate(row)


class ProgressPercentage(object):
    """
    進捗表示
    https://zenn.dev/nowko/books/550bcd398f3122/viewer/2b77c2
    """

    def __init__(self, file_path):
        self._filename = file_path
        self._size = float(os.path.getsize(file_path))
        self._seen_so_far = 0
        self._lock = threading.Lock()
        self.bar = tqdm(total=self._size)
        self.bar.set_description("{0}".format(file_path))
        self.bar.bar_format = "{desc}| {percentage:.1f}%|{bar:25} | {rate_fmt}  "
        self.bar.unit = "B"
        self.bar.unit_scale = True
        self.bar.unit_divisor = 1024

    def __call__(self, bytes_amount):
        # To simplify we'll assume this is hooked up
        # to a single filename.

        with self._lock:
            self.bar.update(bytes_amount)


if __name__ == "__main__":
    rmg = RMG(csv_path)
    # リスト作成
    # make_check_list(rmg)
    # リストに基づいてzip化＆AWSアップロード
    upload_aws_S3(rmg)
    del rmg
    logging.info("------------------Finished------------------")
