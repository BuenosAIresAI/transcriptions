from pyannote.audio import Pipeline
import ffmpeg
from datetime import timedelta
import sys
import boto3
from pathlib import Path


sys.path.append('../')
sys.path.append('../core')
import core.database as DB
import core.config as config
LOCAL_DIR = config.LOCAL_DIR
AWS_S3_CREDS = config.AWS_S3_CREDS

sys.path.append('/usr/local/bin/ffmpeg')
db = DB.DatabaseUtils()


print('creating Pipeline')
pipeline = Pipeline.from_pretrained('pyannote/speaker-diarization', use_auth_token='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')
print('pipeline created')


def diarize(diarization_id):
  print(f"******* Starting diarization id:{diarization_id} ****************")
  db.updateJobStatus(diarization_id, "Diarization In Progress")
  downloadFileFromS3(diarization_id)
  diarization(diarization_id)
  uploadToS3(diarization_id)
  db.updateJobStatus(diarization_id,"DiarizationCompleted")
  print(f"******* END DIARIZATION ****************")

def getDiarizationJobs():
  
  for job in db.getPendingJobs():
    diarize(job['transcription-id'])

def downloadFileFromS3(diarization_id):
  object_name = f"diarizations/{diarization_id}/diarize.wav"
  print(f"downloading file {object_name}")
  s3 = boto3.client("s3", **AWS_S3_CREDS)
  # s3 = session.resource('s3')
  bucket_name = "baires"
  # your_bucket = s3.Bucket(bucket_name)
  # object_name = "diarize.wav"
  # file_name = os.path.join(pathlib.Path(__file__).parent.resolve(), "sample_file.txt")
  diarization_path =f'{LOCAL_DIR}/{diarization_id}'
  Path(diarization_path).mkdir(parents=True, exist_ok=True)
  s3.download_file(bucket_name, object_name, f'{diarization_path}/diarize.wav')
  print("file downloaded")


def diarization(diarization_id):
  print('Running pyannote.audio to generate the diarizations.')
  # db.updateJobStatus(diarization_id,"Diarization in progress")
  
  object_name = f"{diarization_id}/diarize.wav"
  DEMO_FILE = {'uri': 'blabla', 'audio': f'{LOCAL_DIR}/{object_name}'}
  dz = pipeline(DEMO_FILE)  

  diarizationFile = LOCAL_DIR / diarization_id / "diarization.csv"
  with open(diarizationFile, "w") as text_file:
    # text_file.write("demo")
    for turn, _, speaker in dz.itertracks(yield_label=True):
      text_file.write(f"{turn.start:.3f},{turn.end:.3f}, {speaker}\n")
  

def uploadToS3(diarization_id):
  diarizationFile = LOCAL_DIR / diarization_id / "diarization.csv"
  print(f"uploading file from local {diarizationFile}")
  s3 = boto3.client("s3", **AWS_S3_CREDS)
  bucket_name = "baires"
  object_name = f'{diarization_id}/diarization.csv'
  s3.upload_file(diarizationFile, bucket_name, object_name)
  # db.updateJobStatus(diarization_id,"Diarization finished")
  print("file uploaded")


getDiarizationJobs()