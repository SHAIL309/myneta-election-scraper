# MyNeta Lok Sabha 2024 Scraper

A Python scraper for extracting candidate-level data from
https://www.myneta.info for the 2024 Indian Lok Sabha elections.

## Features
- Scrape single candidates, constituencies, states, or all of India
- Extract:
  - Candidate details
  - Criminal cases (IPC sections & charges)
  - Immovable assets (self, spouse, total)
  - Liabilities
- Outputs CSV and JSON
- Polite crawling with retries and delays

## Usage

Single candidate:
```bash
python main.py --candidate_id 6163
