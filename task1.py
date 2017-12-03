import bigquery
import sql

q = """
    #standardSQL
    SELECT
      max_temp,
      min_temp,
      state
    FROM (
        SELECT
          max,
          (max-32)*5/9 max_temp,
          min,
          (min-32)*5/9 min_temp,
          mo,
          da,
          state,
          stn,
          name,
          ROW_NUMBER() OVER(PARTITION BY state ORDER BY max DESC) as rn
        FROM
         (SELECT * FROM
         `bigquery-public-data.noaa_gsod.gsod199*` UNION ALL
         SELECT * FROM
            `bigquery-public-data.noaa_gsod.gsod2000`) as a
        JOIN
         `bigquery-public-data.noaa_gsod.stations` as b
          ON
            a.stn=b.usaf
            AND a.wban=b.wban
        WHERE state is not null and state!="" and max<1000 and country='US'
        )
    WHERE rn=1;
"""

results = bigquery.run_query(q)
rows_to_insert = [{
    'max_temp': r.max_temp,
    'min_temp': r.min_temp,
    'state': r.state
} for r in results]

print("Rows to insert: ")
print(rows_to_insert)

sql_conn = sql.conn

sql_conn.execute("""CREATE TABLE IF NOT EXISTS max_min_temp_by_state (
                    min_temp DECIMAL(12,5),
                    max_temp DECIMAL(12,5),
                    state VARCHAR(255)
                );""")

table = 'max_min_temp_by_state'
sql_conn.execute("DELETE FROM {};".format(table))

sql_conn.execute(sql.text("""
    INSERT INTO `max_min_temp_by_state` (min_temp, max_temp, state) VALUES (:min_temp, :max_temp, :state);
"""), *rows_to_insert)

res = sql_conn.execute("""
    SELECT * FROM `max_min_temp_by_state`;
""").fetchall()

print("Inserted the rows: ")
print(res)

sql_conn.close()
