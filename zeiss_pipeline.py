import pandas as pd
import duckdb
import re

amz = 'https://docs.google.com/spreadsheets/d/1pP8jJDYPS1nHjHHDM1IycjtdFLLJIA8kRQ5AaC32Su0'
google = 'https://docs.google.com/spreadsheets/d/1oADSDDLjFmVs5tkc6KxJQW5B0VRg1qxaSqtQSV6EemA'

urls = [amz, google]

# Remove tables if already created
duckdb.sql(f"""DROP TABLE IF EXISTS amz""")
duckdb.sql(f"""DROP TABLE IF EXISTS google""")

val = 2

for n in range(len(urls)):
    name = 'amz'
    print(f'Starting run {n}...extracting data for:')
    print(name)

    u = f'{urls[n]}/export?format=xlsx'

    # Convert to XLSX to get sheet names
    xl = pd.ExcelFile(u)

    sheet_names = xl.sheet_names
    num_sheets = len(sheet_names)

    # Select desired sheet via index
    df = pd.read_excel(xl,val)

    # Convert to dataframe
    df = pd.DataFrame(df)
    
    # If Google Ads data => extract ASIN from URL column
    if n == 1:
        print('Run #2...running additional transformations...')
        
        name = 'google'
        print(f'Name updated to {name}')
        
        links = df.URL
        asin_url = []

        # Loop through URL column
        for l in links:
            asin = re.findall(r'(?<=\/)[A-Za-z0-9]{10}', l)
            asin_url.append(asin[0])

        # print('ASINs: ', asin_url)

        # Insert ASIN column
        df['ASIN'] = asin_url

    # Create table from df
    duckdb.sql(f"""CREATE TABLE IF NOT EXISTS {name} AS SELECT * FROM df""")

    # Edit value of 'val' to ensure targeting the desired sheet via index
    val += 1


# Create view 'google_agg' for Google data
duckdb.execute("""
                CREATE OR REPLACE VIEW google_agg AS
                SELECT DISTINCT
                    g.Campaign as campaign,
                    ROUND(SUM(g.Cost), 2) as cost,
                    SUM(g.Impressions) as impr,
                    SUM(g.Clicks) as clicks,
                    ROUND(AVG(g.CPC), 2) as cpc,
                FROM google as g
                GROUP BY g.campaign
                ORDER BY g.campaign ASC
""")

# Create view 'amz_asin' for AMZ ASIN data
duckdb.execute("""
                CREATE OR REPLACE VIEW asin_agg AS
                SELECT DISTINCT
                    a.ASIN,
                    a.Campaign,
                    ROUND(SUM(a.Sales), 2) as sales
                FROM amz as a
                GROUP BY a.ASIN, a.Campaign
                ORDER BY a.ASIN ASC
""")


# Join Google campaign data + AMZ sales data
duckdb.execute("""
                CREATE OR REPLACE VIEW all_data AS
                SELECT
                    g.campaign,
                    ROUND(g.cost, 2),
                    CAST(g.clicks as INT) clicks,
                    g.cpc,
                    CAST(SUM(a.DPV) as INT) as dpv,
                    CAST(SUM(a.ATC) as INT) as atc,
                    CAST(SUM(a.Purchases) as INT) as purchases,
                    ROUND(SUM(a.Sales), 2) as sales
               FROM google_agg g
               LEFT JOIN amz a
               ON g.Campaign = a.Campaign
               GROUP BY g.campaign, g.cost, g.clicks, g.cpc,
               ORDER BY g.campaign ASC
""")

# Check table values in console
duckdb.sql("SELECT * FROM google_agg").show()
duckdb.sql("SELECT * FROM asin_agg").show()
duckdb.sql("SELECT * FROM alL_data").show()
