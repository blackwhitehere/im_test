"""
Regions are parts of countries
Cities belong to regions and countries.
"""

import os
import gzip
import json
import csv

import gcs
import sql

# I'm converting the files to json only because its a convenience that allows to separate data formatting logic
# from data insert logic. Since the files are small, its fine to duplicate the files, however for larger files this
# should be avoided

if not os.path.exists('regions.json'):
    if not os.path.exists('regions.csv'):
        region_blob = gcs.get_blob("regions.csv")
        region_blob.download_to_filename('regions.csv')
    with open('regions.csv', encoding='utf-8') as source:
        rows = csv.DictReader(source)
        region_rows = []
        for region in rows:
            region_rows.append(region)
        with open('regions.json', 'w') as out:
            json.dump(region_rows, out)

if not os.path.exists('cities.json'):
    if not os.path.exists('cities.gz'):
        cities_blob = gcs.get_blob("cities.gz")
        cities_blob.download_to_filename('cities.gz')
    cities_json = []
    with gzip.open('cities.gz') as source:
        cities_str = source.read().decode('utf-8')
        for city in cities_str.splitlines():
            city_json = json.loads(city)
            city_json.setdefault("id", None)
            city_json.setdefault("country_id", None)
            city_json.setdefault("region_id", None)
            city_json.setdefault("name", None)
            city_json.setdefault("iso_code", None)

            cities_json.append(city_json)
        with open('cities.json', 'w') as res:
            json.dump(cities_json, res)

if not os.path.exists('countries.json'):
    if not os.path.exists('countries.gzip'):
        countries_blob = gcs.get_blob("countries.gzip")
        countries_blob.download_to_filename('countries.gzip')
    countries_json = []
    with gzip.open('countries.gzip', 'r') as source:
        reader = csv.DictReader(iter(source.read().decode('utf-8').split('\n')))
        country_rows = []
        for row in reader:
            country_rows.append(row)
        with open('countries.json', 'w') as out:
            json.dump(country_rows, out)

create_db = """CREATE DATABASE IF NOT EXISTS solutions
                DEFAULT CHARACTER SET utf8
                DEFAULT COLLATE utf8_general_ci;
            """

drop_tables = """   SET SQL_SAFE_UPDATES = 0;
                    DROP TABLE IF EXISTS cities; DROP TABLE IF EXISTS regions; DROP TABLE IF EXISTS countries;
                    SET SQL_SAFE_UPDATES = 1;
"""

schema_countries = """
                    CREATE TABLE IF NOT EXISTS `countries` (
                          id INT AUTO_INCREMENT,
                          alpha2 CHAR(2),
                          alpha3 CHAR(3),
                          name VARCHAR(255),
                          targetable BOOL,
                          PRIMARY KEY (id)
                    );
"""

schema_regions = """
                    CREATE TABLE IF NOT EXISTS `regions` (
                        id INT AUTO_INCREMENT,
                        country_id INT NOT NULL,
                        name VARCHAR(255),
                        iso_code VARCHAR(100),
                        PRIMARY KEY (id),
                        FOREIGN KEY (country_id) REFERENCES countries(id)
                    );
"""

schema_cities = """
                    CREATE TABLE IF NOT EXISTS `cities` (
                        id INT AUTO_INCREMENT,
                        country_id INT NOT NULL,
                        region_id INT,
                        name VARCHAR(255) NOT NULL,
                        iso_code VARCHAR(100),
                        PRIMARY KEY (id),
                        FOREIGN KEY (country_id) REFERENCES countries(id),
                        FOREIGN KEY (region_id) REFERENCES regions(id)
                    );
"""
conn = sql.im_db_engine.connect()
conn.execute(create_db)
conn.execute("""USE solutions;""")
conn.execute(drop_tables)

conn.execute(schema_countries)
conn.execute(schema_regions)
conn.execute(schema_cities)


with open('countries.json') as fc:
    countries_json = json.load(fc)
    conn.execute(sql.text("""
        INSERT INTO `countries` (id, alpha2, alpha3, name, targetable) VALUES
        (:id, :alpha2, :alpha3, :name, :targetable);
    """), *countries_json)

with open('regions.json') as fr:
    regions_json = json.load(fr)
    conn.execute(sql.text("""
        INSERT INTO `regions` (id, country_id, name, iso_code) VALUES
        (:id, :country_id, :name, :iso_code);
    """), *regions_json)

with open('cities.json') as fct:
    cities_json = json.load(fct)
    conn.execute(sql.text("""
        INSERT INTO `cities` (id, country_id, region_id, name, iso_code) VALUES
        (:id, :country_id, :region_id, :name, :iso_code);
    """), *cities_json)

conn.close()
