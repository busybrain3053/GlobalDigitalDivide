import io
import re
import zipfile
from typing import Tuple
import requests
import pandas as pd

# World Bank indicator codes
INDICATORS = {
    # % of population using the internet
    "IT.NET.USER.ZS": ("internet_users_pct", "Internet users (% of population)"),
    # Fixed broadband subscriptions (per 100 people)
    "IT.NET.BBND.P2": ("broadband_subs_per100", "Fixed broadband subscriptions (per 100)"),
    # GDP per capita, current US$
    "NY.GDP.PCAP.CD": ("gdp_per_capita_usd", "GDP per capita (US$)"),
}

def fetch_wb_indicator(indicator: str, value_name: str) -> pd.DataFrame:
    """
    Download a World Bank indicator as a long, tidy dataframe:
      country, country_code, year, <value_name>
    """
    url = f"https://api.worldbank.org/v2/en/indicator/{indicator}?downloadformat=csv"
    r = requests.get(url, timeout=60)
    r.raise_for_status()

    z = zipfile.ZipFile(io.BytesIO(r.content))

    # pick the API_* file (the data file)
    api_csv_name = next(n for n in z.namelist() if re.match(r"API_.*_DS2_en_csv_v2_.*\.csv$", n))

    # WB CSV has 4 metadata lines before the real header
    with z.open(api_csv_name) as f:
        df = pd.read_csv(f, skiprows=4, engine="python")  # <-- key change

    # keep only useful columns (years are wide)
    id_cols = ["Country Name", "Country Code"]
    year_cols = [c for c in df.columns if re.fullmatch(r"\d{4}", str(c))]
    wide = df[id_cols + year_cols].copy()

    # melt to long
    long = wide.melt(id_vars=id_cols, var_name="year", value_name=value_name)

    # basic clean
    long = long.rename(columns={"Country Name": "country", "Country Code": "country_code"})
    long["year"] = pd.to_numeric(long["year"], errors="coerce").astype("Int64")
    long[value_name] = pd.to_numeric(long[value_name], errors="coerce")

    # drop aggregates; keep proper ISO3 codes only
    long = long[long["country_code"].str.fullmatch(r"[A-Z]{3}", na=False)]

    # reasonable window
    long = long[long["year"].between(2000, 2024, inclusive="both")]

    return long.dropna(subset=[value_name])


def main() -> None:
    frames = []
    for code, (vname, _) in INDICATORS.items():
        print(f"Downloading {code} …")
        frames.append(fetch_wb_indicator(code, vname))

    # Merge on country_code + year (safe against name variants)
    df = frames[0]
    for nxt in frames[1:]:
        df = df.merge(nxt, on=["country_code", "year"], how="inner")

    # Prefer a stable country display name from the first frame we downloaded
    # (names are identical after merge, but this is explicit)
    # Keep just one 'country' column
        # Prefer a stable country display name from the first indicator frame
    country_name = frames[0][["country_code", "country"]].drop_duplicates()

    # Drop any existing 'country' column to avoid merge conflicts
    df = df.drop(columns=["country"], errors="ignore")

    # Merge clean display name
    df = df.merge(country_name, on="country_code", how="left")


    # Reorder columns
    df = df[[
        "country", "country_code", "year",
        "internet_users_pct", "broadband_subs_per100", "gdp_per_capita_usd"
    ]].sort_values(["country", "year"]).reset_index(drop=True)

    # Final sanity filters
    # Drop rows with any missing values across the three indicators
    df = df.dropna(subset=["internet_users_pct", "broadband_subs_per100", "gdp_per_capita_usd"])

    out_path = "digital_divide_clean.csv"
    df.to_csv(out_path, index=False)
    print(f"\n✅ Created {out_path}")
    print(f"Rows: {len(df):,}")
    print(df.head(8))

if __name__ == "__main__":
    main()