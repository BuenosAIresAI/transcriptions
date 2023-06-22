import boto3
import re
from pydub import AudioSegment
import whisper
from datetime import timedelta
import queue
import csv
import sys
from pathlib import Path

sys.path.insert(0,'..')
sys.path.append('../')
sys.path.append('../core')
import core.database as DB
import core.config as config
LOCAL_DIR = config.LOCAL_DIR
AWS_S3_CREDS = config.AWS_S3_CREDS

class TranscriptionSegment:
    def __init__(self, text, start, end):
      self.text = text
      self.start = start
      self.end = end

class Transcription:
  def __init__(self):
    model_size = "large"
    print("Loading whisper model "+ model_size)
    self.model = whisper.load_model(model_size)
    self.diarization_file = "diarization.csv"
    self.wav_file = "diarize.wav"
    self.speakers = {}

  
  def downloadFileFromS3(self, transcription_id):
    print("downloading files")
    s3 = boto3.client("s3", **AWS_S3_CREDS)
    bucket_name = "baires"
    job_path =f'{LOCAL_DIR}/{transcription_id}'
    Path(job_path).mkdir(parents=True, exist_ok=True)
    s3.download_file(bucket_name, f'{transcription_id}/{self.diarization_file}', f'{job_path}/{self.diarization_file}')
    s3.download_file(bucket_name, f'diarizations/{transcription_id}/{self.wav_file}', f'{job_path}/{self.wav_file}')
    print("files downloaded")

  def millisec(timeStr):
    spl = timeStr.split(":")
    s = (int)((int(spl[0]) * 60 * 60 + int(spl[1]) * 60 + float(spl[2]) )* 1000)
    return s

  groups = []

  def chunkAudioFiles(self, transcription_id):
    print("chunk audio files")
    def conv(s):
      try:
        s = float(s)
      except ValueError:
        pass
      return s
    audio = AudioSegment.from_wav(f'{LOCAL_DIR}/{transcription_id}/{self.wav_file}')
    gidx = 0
    with open(f'{LOCAL_DIR}/{transcription_id}/{self.diarization_file}', 'r') as file:
      csvreader = csv.reader(file)
      for row in csvreader:
        start = conv(row[0]) * 1000
        end = conv(row[1]) * 1000
        chunk_path = f'{LOCAL_DIR}/{transcription_id}/{str(gidx)}.wav'
        audio[start:end].export(chunk_path, format='wav')
        self.speakers[gidx]=row[2]
        gidx += 1

    return gidx

  

  def __uploadToS3(self, file, transcription_id):
    print("uploading file")
    s3 = boto3.client("s3", **AWS_S3_CREDS)
    # s3 = session.resource('s3')
    bucket_name = "baires"
    object_name = "transcription.txt"
    # file_name = os.path.join(pathlib.Path(__file__).parent.resolve(), "sample_file.txt")
    s3.upload_file(file, bucket_name, f'transcriptions/{transcription_id}/{object_name}')
    # self.db.updateJobStatus(self.transcription_id, "DiarizationPending")
    print("file uploaded")

  def transcribe(self, gidx, transcription_id):
    
    q = queue.LifoQueue()
    j = 0
    print("Transcribing chunks")
    transcription = f'{LOCAL_DIR}/{transcription_id}/transcription.txt'
    with open(transcription, "w") as text_file:
      end = 0
      last_time = 0
      for i in range(gidx):
        path = f'{LOCAL_DIR}/{transcription_id}/{str(i)}.wav'
        result = self.model.transcribe(path)
        # time += end 
        spi = 0
        last_time = end
        for segment in result['segments']:
          # print(f"{timedelta(seconds=segment['start'])} - {timedelta(seconds=segment['end'])}")
          start = last_time + segment['start']
          end =  start + segment['end']
          text = segment['text']
          #text_file.write(f'(i={i}, spi={spi}, last_time={timedelta(seconds=last_time)}) [{timedelta(seconds=start)} - {timedelta(seconds=end)}] ({self.speakers[spi]}):{text}\n')
          text_file.write(f'[{timedelta(seconds=start)} - {timedelta(seconds=end)}] ({self.speakers[spi]}):{text}\n')
          last_time = end
          spi += 1

        # q.put(TranscriptionSegment(
        #   segment['text'],
        #   segment['start'],
        #    segment['end']))
    
        j += 1
        # if j==5:
        #   break
    
      text_file.close()
      self.__uploadToS3(transcription, transcription_id)
      return q

  # def show(queue):
  #   spacermilli = 2000
  #   self.speakers = {'SPEAKER_00':('Dyson', 'white', 'darkorange'), 'SPEAKER_01':('Interviewer', '#e1ffc7', 'darkgreen') }
  #   def_boxclr = 'white'
  #   def_spkrclr = 'orange'
  #   gidx = -1
  #   for g in groups:
  #     shift = re.findall('[0-9]+:[0-9]+:[0-9]+\.[0-9]+', string=g[0])[0]
  #   shift = millisec(shift) - spacermilli #the start time in the original video
  #   shift=max(shift, 0)
  #
  #   gidx += 1
  #   # captions = [[(int)(millisec(caption.start)), (int)(millisec(caption.end)),  caption.text] for caption in webvtt.read(str(gidx) + '.wav.vtt')]
  #   captions = [[(int)(millisec(caption.start)), (int)(millisec(caption.end)),  caption.text] for caption in queue.get()]
  #
  #   if captions:
  #     speaker = g[0].split()[-1]
  #     boxclr = def_boxclr
  #     spkrclr = def_spkrclr
  #     if speaker in self.speakers:
  #       speaker, boxclr, spkrclr = self.speakers[speaker]
  #
  #     print(f'{speaker}:')
  #     # html.append(f'<div> class="e" style="background-color: {boxclr}">\n');
  #     # html.append(f'<span style="color: {spkrclr}">{speaker}</span><br>\n')
  #
  #     for c in captions:
  #       start = shift + c[0]
  #
  #       start = start / 1000.0   #time resolution ot youtube is Second.
  #       startStr = '{0:02d}:{1:02d}:{2:02.2f}'.format((int)(start // 3600),
  #                                               (int)(start % 3600 // 60),
  #                                               start % 60)
  #       #html.append(f'<div class="c">')
  #       #html.append(f'\t\t\t\t<a class="l" href="#{startStr}" id="{startStr}">#</a> \n')
  #       # html.append(f'\t\t\t\t<a href="#{startStr}" id="{startStr}" class="lt" onclick="jumptoTime({int(start)}, this.id)">{c[2]}</a>\n')
  #       print(f'{startStr}: {c[2]}')
  #       #html.append(f'\t\t\t\t<div class="t"> {c[2]}</div><br>\n')
  #       #html.append(f'</div>')
  def process(self):
    db = DB.DatabaseUtils()
    # import pdb; pdb.set_trace()
    jobs = db.getDiarizationCompletedJobs()
    for job in jobs:
      transcription_id = job['transcription-id']  
      db.updateJobStatus(transcription_id, "Transcription in Progress")
      print(f"transcription in progress: {job['video_name']}")
      self.downloadFileFromS3(transcription_id)
    # groupDiarizationSegments()
      # chunkAudioFiles(transcription_id)
      # self.chunkAudioFiles(transcription_id)
      q=self.transcribe(self.chunkAudioFiles(transcription_id),transcription_id)
      print("transcription done")
      db.updateJobStatus(transcription_id, "Completed")
      print("transcription job record updated")
      # show(q)
      # q=transcribe(20)
      #   time = 0
      #   i = 0
      #   for elem in list(q.queue):
      #     # start = timedelta(seconds=elem.start)
      #     start = time + elem.start
      #     end = start + elem.end
      #     print(f'[{timedelta(seconds=start)} - {timedelta(seconds=end)}] ({self.speakers[i]}):{elem.text}')
      #     time += elem.end
      #     i += 1



t=Transcription()
t.process()