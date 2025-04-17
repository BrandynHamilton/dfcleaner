# `dfcleaner` Documentation

dfcleaner is a lightweight Python utility for cleaning, parsing, and preparing time series and tabular datasets.
It streamlines common DataFrame operations such as timezone normalization, date parsing, frequency inference, BOM removal, and value cleaning.

## Installation

```bash
pip install dfcleaner
```

Or clone locally for development:

```bash
git clone https://github.com/yourname/dfcleaner.git
cd dfcleaner
pip install -e .
```

## Usage Example

```python
from dfcleaner.core import DFCleaner

cleaner = DFCleaner(timezone="UTC")
df = cleaner.to_df("my_data.csv")
df, freq = cleaner.to_time(df)
df = cleaner.cleaning_values(df)
df = cleaner.clean_dates(df, time_freq=freq)
```

## Core Methods

### `to_df(file, delimiter=',')`

Load a CSV or Excel file into a clean pandas DataFrame.

- Handles BOM characters and whitespace.
- Removes rows that contain only invisible characters or whitespace.

### `apply_timezone(df)`

Applies or removes timezone from the DataFrame index depending on the initialized setting.

### `detect_time_col(df, custom_col=None)`

Scans DataFrame for common time-related column names. You can optionally pass a custom override.

### `to_time(df, time_col=None, dayfirst=False)`

- Converts a detected or specified datetime column to the index.
- Infers the frequency of the datetime index.

### `clean_dates(df, time_freq)`

Drops incomplete periods based on inferred time frequency:

- `W`: drops current week if incomplete
- `M`: drops current month
- `Q`: drops current quarter

### `cleaning_values(df)`

Cleans numeric object columns by:

- Removing symbols like `%`, `$`, `,`
- Replacing Excel artifacts like `#DIV/0!` with `NaN`
- Converting to proper numeric dtype

### `open_json(file_name)`

Loads a JSON file and parses into a Python dictionary.

## Project Structure

```
dfcleaner/
├── __init__.py
└── core.py
```

## License

MIT License
