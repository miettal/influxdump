import click
import influxdb
import time
import datetime
import re


def rs2points(rs):
    points = []
    flag = True
    for (meta, rows) in rs.items():
        (measurement, tags) = meta
        for row in rows:
            t = row
            t = row['time']
            if flag:
                flag = False
            fields = {}
            for (k, v) in row.items():
                if k == 'time':
                    continue
                fields[k] = v
            point = {
                'measurement': measurement,
                'tags': tags if tags is not None else {},
                'time': t,
                'fields': fields,
            }
            points.append(point)
    return points


def parse_influxdb_url(url):
    m = re.match('(http(s)?)://([0-9a-zA-Z_.]+):([0-9a-zA-Z_.]+)@([0-9a-zA-Z_.]+)(:([0-9]+))?/([0-9a-zA-Z_.]+)/([0-9a-zA-Z_.]+)', url)

    if m:
        return {
            'schema': m.group(1),
            'username': m.group(3),
            'password': m.group(4),
            'host': m.group(5),
            'port': m.group(7),
            'database': m.group(8),
            'measurement': m.group(9),
        }
    else:
        return None


@click.command()
@click.argument('src')
@click.argument('dst')
def main(src, dst):
    src = parse_influxdb_url(src)
    dst = parse_influxdb_url(dst)
    print(f'src: {src}')
    print(f'dst: {dst}')

    influxdb_src = influxdb.InfluxDBClient(
        host=src['host'],
        port=src['port'],
        username=src['username'],
        password=src['password'],
        database=src['database'],
        ssl=src['schema'] == 'https',
        gzip=True,
    )
    influxdb_dst = influxdb.InfluxDBClient(
        host=dst['host'],
        port=dst['port'],
        username=dst['username'],
        password=dst['password'],
        database=dst['database'],
        ssl=dst['schema'] == 'https',
        gzip=True,
    )

    rs = influxdb_src.query('select * from "{:s}" ORDER BY time ASC LIMIT 1'.format(src['measurement']))
    for point in rs.get_points():
        s = datetime.datetime.fromisoformat(list(point.items())[0][1][:19])
    rs = influxdb_src.query('select * from "{:s}" ORDER BY time DESC LIMIT 1'.format(src['measurement']))
    for point in rs.get_points():
        e = datetime.datetime.fromisoformat(list(point.items())[0][1][:19])
    print(s, e)

    d = s.date()
    while d < e.date():
        print(d)
        q = "SELECT *::field FROM \"{:s}\" WHERE '{:s}' <= time AND time < '{:s}' GROUP BY *".format(src['measurement'], d.isoformat(), (d + datetime.timedelta(days=1)).isoformat())
        rs = influxdb_src.query(q)
        points = rs2points(rs)

        points = [{**point, 'measurement': dst['measurement']} for point in points]

        influxdb_dst.write_points(points)

        d += datetime.timedelta(days=1)


if __name__ == '__main__':
    # python influxdump.py https://username:password@localhost:8086/database/measurement https://username:password@localhost:8086/database/measurement
    main()
