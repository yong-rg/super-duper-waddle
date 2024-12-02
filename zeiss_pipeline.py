import pandas as pd
import duckdb
import re

amz = 'https://docs.google.com/spreadsheets/d/1pP8jJDYPS1nHjHHDM1IycjtdFLLJIA8kRQ5AaC32Su0'
google = 'https://docs.google.com/spreadsheets/d/1oADSDDLjFmVs5tkc6KxJQW5B0VRg1qxaSqtQSV6EemA'

urls = [amz, google]

# Remove tables if already created
duckdb.sql(f"""DROP TABLE IF EXISTS amz""")
duckdb.sql(f"""DROP TABLE IF EXISTS google""")

# Install DuckDB extensions
con = duckdb.connect()
con.install_extension("spatial")
con.load_extension("spatial")

val = 2

for n in range(len(urls)):
    name = 'amz'
    print(f'Starting run {n}...')

    u = f'{urls[n]}/export?format=xlsx'

    # Convert to XLSX to get sheet names
    xl = pd.ExcelFile(u)

    sheet_names = xl.sheet_names
    num_sheets = len(sheet_names)

    # Select desired sheet via index
    df = pd.read_excel(xl, val)

    # Convert to dataframe
    df = pd.DataFrame(df)
    
    # If Google Ads data => extract ASIN from URL column
    if n == 1:
        print('Run #2...running additional transformations...')
        
        name = 'google'
        print(f'Name updated to {name}')
        
        links = df.URL
        print(links)
        asin_url = []

        # Loop through URL column
        for l in links:
            asin = re.findall(r'(?<=\/dp\/)[A-Za-z0-9]{10}', l)
            asin_url.append(asin[0])

        # print('ASINs: ', asin_url)

        # Insert ASIN column
        df['ASIN'] = asin_url

    # Create table from df
    duckdb.sql(f"""CREATE TABLE IF NOT EXISTS {name} AS SELECT * FROM df""")

    # Edit value of 'val' to ensure targeting the desired sheet via index
    val += 1


# Create view for All Google Data
duckdb.execute("""
                CREATE OR REPLACE VIEW google_agg AS
                SELECT DISTINCT
                    g.Campaign as campaign,
                    g."Ad Group" as ad_group,
                    g."Ad Group Status" as ad_group_status,
                    ROUND(SUM(g.Cost), 2) as cost,
                    SUM(g.Impressions) as impr,
                    SUM(g.Clicks) as clicks,
                    ROUND(AVG(g.CPC), 2) as cpc,
                FROM google as g
                GROUP BY g.campaign, g."Ad Group", g."Ad Group Status"
                ORDER BY g.campaign ASC
""")

# Create view for AMZ Campaign Data
duckdb.execute("""
                CREATE OR REPLACE VIEW amz_campaign AS
                SELECT DISTINCT
                    a.Campaign,
                    ROUND(SUM(a.Sales), 2) as sales
                FROM amz as a
                GROUP BY a.Campaign
""")

# Create view for AMZ Ad Group Data
# duckdb.execute("""
#                 CREATE OR REPLACE VIEW amz_campaign AS
#                 SELECT DISTINCT
#                     *
#                 FROM "ad group" as a
#                 LIMIT 10
# """)

# Create view for All AMZ ASIN Data
duckdb.execute("""
                CREATE OR REPLACE VIEW asin_agg AS
                SELECT DISTINCT
                    a.ASIN,
                    ROUND(SUM(a.Sales), 2) as sales
                FROM amz as a
                GROUP BY a.ASIN
                ORDER BY a.ASIN ASC
""")

# Create view for All Data
duckdb.execute("""
                CREATE OR REPLACE VIEW all_data AS
                WITH DistinctCosts AS (
                    SELECT DISTINCT g.cost, g.Campaign
                    FROM google_agg g
                ),
                Summary AS (
                    SELECT
                        ROUND(SUM(DISTINCT DistinctCosts.cost), 2) as total_cost,
                        ROUND(SUM(DISTINCT a.Sales), 2) as total_sales
                FROM DistinctCosts
                LEFT JOIN amz a
                    ON DistinctCosts.Campaign = a.Campaign
                )
                SELECT 
                    total_cost,
                    total_sales,
                    ROUND((total_sales / total_cost), 2) as roas
               FROM Summary
               
""")

# Create view for All Campaign Data
duckdb.execute("""
                CREATE OR REPLACE VIEW campaign_data AS
                SELECT
                    g.campaign,
                    ROUND(SUM(DISTINCT g.cost), 2) as cost,
                    SUM(DISTINCT g.clicks) as clicks,
                    ROUND(SUM(DISTINCT g.cost) / SUM(DISTINCT g.clicks), 2) as cpc, -- not accurate
                    SUM(DISTINCT a.DPV) as dpv,
                    SUM(DISTINCT a.ATC) as atc,
                    SUM(DISTINCT a.Purchases) as purchases,
                    ROUND(SUM(DISTINCT a.Sales), 2) as sales,
                    ROUND((SUM(DISTINCT a.Sales) / SUM(DISTINCT g.cost)), 2) as ROAS
               FROM google_agg g
               LEFT JOIN amz a
                ON g.Campaign = a.Campaign
               GROUP BY g.campaign,
               ORDER BY g.campaign ASC
""")

# Create view for All Ad Group Data
duckdb.execute("""
                CREATE OR REPLACE VIEW ad_group_data AS
                WITH DistinctGoogle AS (
                    SELECT DISTINCT g.Cost, g.Campaign, g.ad_group, g.ad_group_status
                    FROM google_agg g
               ),
                Summary AS (
                    SELECT
                        DistinctGoogle.ad_group,
                        ROUND(MAX(DistinctGoogle.Cost), 2) as total_cost,
                        MAX(a.DPV) as dpv,
                        MAX(a.ATC) as atc,
                        MAX(a.Purchases) as purchases,
                        ROUND(MAX(a.Sales), 2) as sales,
                    FROM DistinctGoogle
                    LEFT JOIN amz a
                        ON DistinctGoogle.Campaign = a.Campaign
                    WHERE DistinctGoogle.ad_group_status == true
                    GROUP BY DistinctGoogle.ad_group
               )
                SELECT
                    *,
                    ROUND(Summary.sales / Summary.total_cost, 2) as ROAS
                FROM Summary 
""")

# Check table values in console
# print('All Data')
# duckdb.sql("SELECT * FROM all_data").show()
# duckdb.sql("SELECT SUM(sales) FROM amz").show()

# duckdb.execute("COPY (SELECT * FROM all_data) TO 'C:/Users/24G/Documents/Python Scripts/get_zeiss_data/export/all_data.csv' WITH (FORMAT 'CSV')")

# print('Google Agg')
# duckdb.sql("SELECT * FROM google_agg ").show()

# print('ASIN Agg')
# duckdb.sql("SELECT * FROM asin_agg").show()
# duckdb.sql("SELECT * FROM asin_agg WHERE ASIN = 'B0030E4UIQ'").show()

# print('Google Campaign Data')
# duckdb.sql("SELECT * FROM campaign_data").show()

# print('Google Ad Group Data')
# duckdb.sql("SELECT * FROM ad_group_data ORDER BY ad_group ASC").show()

# duckdb.sql('SELECT * FROM ad group').show()
# duckdb.sql('SELECT * FROM amz_campaign').show()