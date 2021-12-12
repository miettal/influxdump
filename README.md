# influxdump

    $ pip install .
    $ python -m influxdump.influxdump --help
    Usage: python -m influxdump.influxdump [OPTIONS] SRC DST
    
    Options:
      --help  Show this message and exit.
    $ python -m influxdump.influxdump https://username:password@localhost:8086/database/measurement https://username:password@localhost:8086/database/measurement
