import pandas as pd
import numpy as np
import json

import datetime as dt
import os

class DFCleaner:
    def __init__(self, timezone=None):
        """
        timezone: 'UTC', 'US/Eastern', etc.
        If None, will remove timezone awareness.
        """
        self.timezone = timezone

    def apply_timezone(self, df):
        """Apply or remove timezone."""
        if not pd.api.types.is_datetime64_any_dtype(df.index):
            return df

        if self.timezone:
            if df.index.tz is None:
                df.index = df.index.tz_localize(self.timezone)
            else:
                df.index = df.index.tz_convert(self.timezone)
        else:
            df.index = df.index.tz_localize(None)

        return df

    def to_df(self, file, delimiter=','):
        """Load CSV or Excel file into a DataFrame and clean BOM characters."""
        try:
            if file.endswith(('.xlsx', '.xls')):
                df = pd.read_excel(file)
            else:
                df = pd.read_csv(file, delimiter=delimiter, encoding='utf-8-sig')  # handles BOM automatically

            # Strip BOMs or invisible characters from column names
            df.columns = df.columns.str.replace('\ufeff', '', regex=False).str.strip()

            # Optionally: remove rows where any column has just a BOM or is empty/whitespace
            df = df[~df.apply(lambda row: row.astype(str).str.contains('\ufeff|^\s*$', regex=True)).any(axis=1)]

            return df

        except Exception as e:
            print(f"Error loading file {file}: {e}")

    def detect_time_col(self, df, custom_col=None):
        """Detect potential datetime column."""
        time_cols = ['date', 'dt', 'hour', 'time', 'day', 'month', 'year', 'week', 'timestamp',
                     'block_timestamp', 'ds', 'period', 'date_time', 'trunc_date', 'quarter', 
                     'block_time', 'block_date', 'date(utc)']
        if custom_col:
            time_cols.append(custom_col.lower())

        for col in df.columns:
            if col.lower() in time_cols:
                return col
        return None

    def to_time(self, df, time_col=None, dayfirst=False):
        """Convert time column to datetime index and infer frequency."""
        df = df.copy()
        col = time_col or self.detect_time_col(df)
        time_freq = 'D'  # Default

        if col:
            if col.lower() == 'year':
                df[col] = pd.to_datetime(df[col].astype(str), format='%Y').dt.year
            elif col.lower() == 'timestamp':
                df[col] = pd.to_datetime(df[col], unit='ms')
            else:
                df[col] = pd.to_datetime(df[col], dayfirst=dayfirst, errors='coerce')

            df.set_index(col, inplace=True)
            df = self.apply_timezone(df)

            try:
                if len(df.index) >= 3:
                    inferred = pd.infer_freq(df.index)
                    time_freq = inferred if inferred else 'D'
            except Exception as e:
                print(f"[Warning] Could not infer frequency: {e}")
                time_freq = 'D'

        return df, time_freq

    def clean_dates(self, df, time_freq):
        """Remove current incomplete periods based on frequency."""
        df = df.copy()

        today = pd.Timestamp.now(tz=df.index.tz) if df.index.tz else pd.to_datetime(dt.date.today())

        if time_freq == 'W':
            start_of_week = today - pd.to_timedelta(today.weekday(), unit='d')
            df = df[df.index < start_of_week]
        elif time_freq == 'M':
            start_of_month = today.replace(day=1)
            df = df[df.index < start_of_month]
        elif time_freq == 'Q':
            current_quarter = (today.month - 1) // 3 + 1
            quarter_start = pd.Timestamp(dt.date(today.year, (current_quarter - 1) * 3 + 1, 1))
            if df.index.tz:
                quarter_start = quarter_start.tz_localize(df.index.tz)
            df = df[df.index < quarter_start]
        else:
            df = df[df.index < today]

        return df.sort_index()

    def cleaning_values(self, df):
        """Clean numerical columns."""
        df = df.copy()
        for col in df.select_dtypes(include=['object', 'string']).columns:
            df[col] = (
                df[col]
                .str.replace('#DIV/0!', 'NaN', regex=False)
                .str.replace('.', 'NaN', regex=False)
                .str.replace('%', '', regex=False)
                .str.replace(',', '', regex=False)
                .str.replace('$', '', regex=False)
            )
            df[col] = pd.to_numeric(df[col], errors='coerce')
        return df

    def open_json(self, file_name, encoding='utf-8'):
        """Load JSON."""
        try:
            with open(file_name, 'r', encoding=encoding) as file:
                data = json.load(file)
            print("✅ JSON data loaded successfully!")
            return data
        except Exception as e:
            print(f"❌ Error loading JSON: {e}")
            return None
