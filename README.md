# data-pipeline

## Activate venv virtual environment:
python -m venv .venv
source .venv/Scripts/activate

## Installation:
pip install -r requirements.txt

## Steps to run:

### start ingesting to store inside delta lake:
py fetch-coinbase-trade-data.py

### following will create and upload delta lake to the backblaze buckets:
Copy it from data-pipeline repo

### following will do LLM analysis on the data inside delta lake:
py run_openai_analysis.py

### also, if you install any new package, save into requirements.txt
pip freeze >> requirements.txt