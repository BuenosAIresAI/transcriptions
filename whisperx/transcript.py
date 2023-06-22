import whisperx
import gc 
from datetime import timedelta

HUGGING_FACE_ACCESS_TOKEN = "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"
device = "cuda" 
audio_file = "diarize.wav"
batch_size = 16 # reduce if low on GPU mem
# batch_size = 4 # te re cabio
compute_type = "float16" # change to "int8" if low on GPU mem (may reduce accuracy)
# compute_type = "int8" # te re cabio
model = "large-v2"
# model = "base"  

# 1. Transcribe with original whisper (batched)
model = whisperx.load_model(model, device, compute_type=compute_type, language="es")

audio = whisperx.load_audio(audio_file)
result = model.transcribe(audio, batch_size=batch_size, language="es")
print(result["segments"]) # before alignment

# delete model if low on GPU resources
# import gc; gc.collect(); torch.cuda.empty_cache(); del model

# 2. Align whisper output
model_a, metadata = whisperx.load_align_model(language_code=result["language"], device=device)
result = whisperx.align(result["segments"], model_a, metadata, audio, device, return_char_alignments=False)

print(result["segments"]) # after alignment

# delete model if low on GPU resources
# import gc; gc.collect(); torch.cuda.empty_cache(); del model_a

# 3. Assign speaker labels
diarize_model = whisperx.DiarizationPipeline(use_auth_token=HUGGING_FACE_ACCESS_TOKEN, device=device)

# add min/max number of speakers if known
diarize_segments = diarize_model(audio_file)
# diarize_model(audio_file, min_speakers=min_speakers, max_speakers=max_speakers)

result = whisperx.assign_word_speakers(diarize_segments, result)
print(diarize_segments)
print(result["segments"]) # segments are now assigned speaker IDs
transcription = "whisperx-transcription.txt"
with open(transcription, "w") as text_file:
    for segment in result['segments']:
        start = segment['start']
        end = segment['end']
        text = segment['text']
        speaker = segment['speaker']
        text_file.write(f'[{timedelta(seconds=start)} - {timedelta(seconds=end)}] ({speaker}):{text}\n')



