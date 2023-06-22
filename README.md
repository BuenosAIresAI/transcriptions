# transcriptions
# Diarization and Transcription Approaches

This repository contains two different approaches for diarization and transcription of audio files.

## Approach 1: Pyannote and Whisper

We use Pyannote for diarization and Whisper for transcription. Here is the process we follow:

1. Process the audio using Pyannote's pipeline.
2. Generate a CSV file with the correspondency between timestamps and speakers.
3. Chunk the original audio file based on Pyannote's diarization CSV output file.
4. Transcribe each chunk of audio.
5. Generate a single transcription file.

## Approach 2: Whisperx

We use Whisperx for this approach. Here is the process we follow:

1. Process an audio file using Whisperx.
2. Extract speakers data.

For more information, please refer to the individual README files in each approach's directory.
