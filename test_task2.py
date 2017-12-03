import sql
from sys import argv

q = """SELECT ct.*, r.*, c.*
        FROM cities as ct JOIN regions as r ON ct.region_id=r.id JOIN countries as c ON c.id=ct.country_id
        WHERE ct.name=:city_name;"""


def all_city_info(city_name):
    res = sql.conn.execute(sql.text(q), {'city_name': city_name}).fetchone()
    return dict(res)


if __name__ == '__main__':
    print(all_city_info(argv[1]))
