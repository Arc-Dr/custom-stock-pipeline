import yfinance as yf
import pandas as pd
import psycopg2
import os
from psycopg2.extras import execute_values

# =========================
# DB CONNECTION
# =========================
conn = psycopg2.connect(os.environ["DB_URL"])
cur = conn.cursor()

# =========================
# TICKERS
# =========================
tickers = ['UNP','PG','MMM','ITW','X']

# =========================
# CREATE TABLES
# =========================
cur.execute("""
CREATE TABLE IF NOT EXISTS custom_stock_prices (
    date DATE,
    value NUMERIC,
    ticker TEXT,
    ipo_price NUMERIC,
    pct_change NUMERIC
);

CREATE TABLE IF NOT EXISTS custom_stock_profile (
    symbol TEXT PRIMARY KEY,
    company_name TEXT,
    sector TEXT,
    industry TEXT,
    country TEXT,
    city TEXT,
    state TEXT,
    zip TEXT,
    address TEXT,
    phone TEXT,
    website TEXT,
    logo TEXT,
    ceo TEXT,
    employees BIGINT,
    business_summary TEXT,
    exchange TEXT,
    currency TEXT,
    quote_type TEXT
);

CREATE TABLE IF NOT EXISTS custom_stock_metrics (
    ticker TEXT PRIMARY KEY,
    price NUMERIC,
    volume BIGINT,
    market_cap BIGINT,
    day_high NUMERIC,
    day_low NUMERIC,
    year_high NUMERIC,
    year_low NUMERIC,
    pe_ratio NUMERIC,
    eps NUMERIC,
    shares BIGINT,
    updated_at DATE
);
""")

conn.commit()

# =========================
# 1. PRICE TABLE
# =========================
print("Loading prices...")

frames = []

for t in tickers:
    try:
        df = yf.download(
            t,
            start="1900-01-01",
            auto_adjust=True,
            threads=False,
            progress=False
        )

        # fallback for problematic tickers
        if df.empty:
            print(f"{t} failed, trying fallback...")
            df = yf.download(
                t,
                period="max",
                auto_adjust=True,
                threads=False,
                progress=False
            )

        if df.empty:
            print(f"No data for {t} ❌")
            continue

        print(f"{t} rows: {len(df)} ✅")

        temp = df['Close'].reset_index()
        temp['ticker'] = t
        temp.columns = ['date','value','ticker']

        frames.append(temp)

    except Exception as e:
        print(f"Error for {t}: {e}")
        continue

# Combine
if frames:
    price_df = pd.concat(frames, ignore_index=True).dropna()
    price_df['date'] = pd.to_datetime(price_df['date']).dt.date

    price_df = price_df.sort_values(['ticker','date'])
    price_df['ipo_price'] = price_df.groupby('ticker')['value'].transform('first')
    price_df['pct_change'] = (price_df['value'] - price_df['ipo_price']) / price_df['ipo_price']

    print(f"Total rows to insert: {len(price_df)}")

    execute_values(
        cur,
        """
        INSERT INTO custom_stock_prices (date, value, ticker, ipo_price, pct_change)
        VALUES %s
        ON CONFLICT DO NOTHING
        """,
        list(price_df[['date','value','ticker','ipo_price','pct_change']].itertuples(index=False, name=None))
    )

    conn.commit()
    print("Prices loaded ✅")

else:
    print("No price data fetched ❌")

# =========================
# 2. PROFILE TABLE
# =========================
print("Loading profiles...")

profile_rows = []

for t in tickers:
    try:
        info = yf.Ticker(t).info

        website = info.get("website")

        logo = info.get("logo_url")
        if not logo and website:
            clean_site = website.replace("https://", "").replace("http://", "")
            logo = f"https://logo.clearbit.com/{clean_site}"

        ceo_name = None
        officers = info.get("companyOfficers")
        if officers:
            ceo_name = officers[0].get("name")

        profile_rows.append((
            t,
            info.get("longName"),
            info.get("sector"),
            info.get("industry"),
            info.get("country"),
            info.get("city"),
            info.get("state"),
            info.get("zip"),
            info.get("address1"),
            info.get("phone"),
            website,
            logo,
            ceo_name,
            info.get("fullTimeEmployees"),
            info.get("longBusinessSummary"),
            info.get("exchange"),
            info.get("currency"),
            info.get("quoteType")
        ))

    except Exception as e:
        print(f"Profile error {t}: {e}")

if profile_rows:
    execute_values(
        cur,
        """
        INSERT INTO custom_stock_profile (
            symbol, company_name, sector, industry,
            country, city, state, zip, address,
            phone, website, logo, ceo, employees,
            business_summary, exchange, currency, quote_type
        )
        VALUES %s
        ON CONFLICT (symbol)
        DO UPDATE SET
            company_name = EXCLUDED.company_name,
            sector = EXCLUDED.sector,
            industry = EXCLUDED.industry,
            website = EXCLUDED.website,
            logo = EXCLUDED.logo,
            ceo = EXCLUDED.ceo
        """,
        profile_rows
    )

    conn.commit()
    print("Profiles loaded ✅")

# =========================
# 3. METRICS TABLE
# =========================
print("Loading metrics...")

metrics_rows = []

for t in tickers:
    try:
        obj = yf.Ticker(t)
        fast = obj.fast_info
        info = obj.info

        # ✅ FIXED INDENTATION + LOGIC
        price = fast.get("last_price")
        volume = fast.get("last_volume")

        # Correct shares
        shares = info.get("sharesOutstanding") or fast.get("shares")

        # Correct market cap
        market_cap = (
            price * shares
            if price and shares
            else info.get("marketCap")
        )

        metrics_rows.append((
            t,
            price,
            volume,
            market_cap,
            fast.get("day_high"),
            fast.get("day_low"),
            fast.get("year_high"),
            fast.get("year_low"),
            info.get("trailingPE"),
            info.get("trailingEps"),
            shares,
            pd.to_datetime("today").date()
        ))

    except Exception as e:
        print(f"Metrics error {t}: {e}")

# =========================
# CLOSE
# =========================
cur.close()
conn.close()

print("CUSTOM PIPELINE DONE 🚀")
