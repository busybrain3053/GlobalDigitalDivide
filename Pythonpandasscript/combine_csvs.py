# combine_csvs.py  (place this file in the same folder as 2015.csv ... 2022.csv)

from pathlib import Path
import pandas as pd

HERE = Path(__file__).parent
print("Running from:", HERE)
csvs = sorted([p for p in HERE.glob("*.csv") if not p.name.startswith("world_happiness_")])
print("Found CSV files:", [p.name for p in csvs])
if not csvs:
    raise SystemExit("No CSV files found in this folder. Put 2015.csv … 2022.csv next to this script.")

TARGET_COLS = [
    "country","region","year",
    "happiness_score","gdp_per_capita","social_support",
    "healthy_life_expectancy","freedom","generosity","corruption"
]

CANON = {
    "country":"country","country name":"country","country_name":"country",
    "region":"region",
    "year":"year",
    "happiness score":"happiness_score","ladder score":"happiness_score","score":"happiness_score",
    "economy (gdp per capita)":"gdp_per_capita","logged gdp per capita":"gdp_per_capita","gdp per capita":"gdp_per_capita",
    "social support":"social_support","family":"social_support",
    "health (life expectancy)":"healthy_life_expectancy","healthy life expectancy":"healthy_life_expectancy",
    "freedom to make life choices":"freedom","freedom":"freedom",
    "generosity":"generosity",
    "perceptions of corruption":"corruption","perception of corruption":"corruption",
}

def norm(cols):
    return [str(c).strip().lower().replace("_"," ").replace("-"," ") for c in cols]

frames = []
for f in csvs:
    df = pd.read_csv(f, encoding="utf-8", low_memory=False)
    df.columns = norm(df.columns)
    df = df.rename(columns={c: CANON[c] for c in df.columns if c in CANON})

    # infer year from filename if missing
    if "year" not in df.columns:
        try:
            df["year"] = int(f.stem)
        except:
            df["year"] = pd.NA

    # ensure all target columns exist
    for col in TARGET_COLS:
        if col not in df.columns:
            df[col] = pd.NA

    df = df[TARGET_COLS]
    frames.append(df)
    print(f"Loaded {f.name}: {len(df)} rows")

out = pd.concat(frames, ignore_index=True)

# numeric coercion & clean
num_cols = ["happiness_score","gdp_per_capita","social_support",
            "healthy_life_expectancy","freedom","generosity","corruption","year"]
for c in num_cols:
    out[c] = pd.to_numeric(out[c], errors="coerce")

out = (out
       .dropna(subset=["country","year"])
       .drop_duplicates(subset=["country","year"])
       .sort_values(["country","year"])
)
out_path = HERE / "world_happiness_2015_2022.csv"
out.to_csv(out_path, index=False, encoding="utf-8")
print(f"\nSaved → {out_path}")
print(f"Rows: {len(out)}  Countries: {out['country'].nunique()}  Years: {int(out['year'].min())}-{int(out['year'].max())}")
