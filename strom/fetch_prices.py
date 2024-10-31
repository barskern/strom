import sys
import os
import logging
from typing import Optional

import pendulum
from pendulum.date import Date
import requests
from dotenv import load_dotenv

try:
    import requests_cache

    requests_cache.install_cache("strom")
except:
    pass

logger = logging.getLogger("strom")

DEFAULT_PRICE_AREA = "NO1"

BASE_URL = "https://www.hvakosterstrommen.no/api/v1/prices/{year}/{month:0>2}-{day:0>2}_{price_area}.json"

PROMSCALE_WRITE_URL = "https://promscale.service.ruud.cloud/write"
PROMSCALE_QUERY_URL = "https://promscale.service.ruud.cloud/api/v1/query"
PROMSCALE_CERT_PATH = None


def get_strom_timeseries(
    session: requests.Session,
    metric_name: str,
    price_area: str,
    from_time: Optional[Date] = None,
    to_time: Optional[Date] = None,
):
    start_date = from_time if from_time else pendulum.now("utc").date()
    end_date = to_time if to_time else pendulum.now("utc").date()

    datas = []
    date_range = pendulum.period(start_date, end_date).range("days")
    for date in date_range:
        url = BASE_URL.format(
            year=date.year, month=date.month, day=date.day, price_area=price_area
        )
        try:
            res = session.get(str(url), json=True)
            if res.status_code == 404:
                logger.warning(f"Data for '{date.to_date_string()}' is not ready yet")

            data = res.json()
            datas.extend(data)
        except Exception as e:
            logger.error(
                f"Unable to download data for day '{date.to_date_string()}': {e}"
            )

    samples = []
    for data in datas:
        sample = []

        sample.append(1000 * pendulum.parse(data["time_start"]).int_timestamp)
        sample.append(data["NOK_per_kWh"])

        samples.append(sample)

    timeseries = {
        "labels": {
            "__name__": metric_name,
            "area": price_area,
        },
        "samples": samples,
    }

    return timeseries


def get_last_timestamp_in_metric(metric_name: str, lookback: str = "1d"):
    res = requests.get(
        PROMSCALE_QUERY_URL,
        params={"query": f"max_over_time(timestamp({metric_name})[{lookback}:])"},
        verify=PROMSCALE_CERT_PATH if PROMSCALE_CERT_PATH else False,
    )
    if not 200 <= res.status_code < 300:
        raise ValueError(f"Unable to get last timestamp of '{metric_name}': {res.text}")

    query_response = res.json()
    query_result = query_response["data"]["result"]
    logger.debug(f"Query result from getting last timestamp: {query_result}")

    try:
        timestamp_s = query_result[0]["value"][1]
    except IndexError as e:
        if lookback != "30d":
            return get_last_timestamp_in_metric(metric_name, "30d")

        logger.warning("Query to get last metric timestamp returned nothing, using current month start..")
        return pendulum.now().start_of('month')

    last_metric_timestamp = pendulum.from_timestamp(float(timestamp_s))

    return last_metric_timestamp


def main():
    logging.basicConfig(level=os.getenv("STROM_LOG", "info").upper())
    load_dotenv()

    global PROMSCALE_CERT_PATH
    PROMSCALE_CERT_PATH = os.getenv("PROMSCALE_CERT_PATH")

    global PRICE_AREA
    PRICE_AREA = os.getenv("PRICE_AREA") or DEFAULT_PRICE_AREA

    if PROMSCALE_CERT_PATH:
        logger.info(f"Using '{PROMSCALE_CERT_PATH}' as promscale certificate")
    else:
        logger.info("Will not verify certificate")

    if len(sys.argv) >= 3:
        start_time = pendulum.parse(sys.argv[1])
        end_time = pendulum.parse(sys.argv[2])
    else:
        logger.info(
            "Did not get any timestamps, running from last metric (if exists) to now"
        )

        start_time = None
        end_time = pendulum.now(tz="UTC") + pendulum.duration(1)

    session = requests.session()

    metric_name = "price_electricity"

    try:
        start_time_ = (
            start_time if start_time else get_last_timestamp_in_metric(metric_name)
        )
        if end_time < start_time_:
            logger.warning("Have a future (?) value in promscale, something is off..")
            return

        logger.info(
            f"Fetching electricity prices from '{start_time_.to_date_string()}' to '{end_time.to_date_string()}'"
        )
        timeseries = get_strom_timeseries(
            session, metric_name, PRICE_AREA, start_time_, end_time
        )
        logger.info(f"Got {len(timeseries['samples'])} samples")

        logger.info(f"Sending samples to promscale")
        res = requests.post(
            PROMSCALE_WRITE_URL,
            json=timeseries,
            verify=PROMSCALE_CERT_PATH if PROMSCALE_CERT_PATH else False,
        )
        if 200 <= res.status_code < 300:
            logger.info(f"Successfully ingested electricity price samples")
        else:
            logger.error(
                f"Unable to ingest electricity price samples, got '{res.status_code}'"
            )
    except Exception as e:
        logger.error(f"Unable to fetch data for '{metric_name}': {e}")
        exit(1)


if __name__ == "__main__":
    main()
