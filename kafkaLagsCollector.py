#!/usr/bin/python
# coding=utf-8

from subprocess import Popen, PIPE
import logging
from influxdb import InfluxDBClient
import json
import argparse
import shlex

loglevel = logging.INFO
logging.basicConfig(level=loglevel,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("Kafka_lags_fetcher")

KAFKA_BIN_DIR = "/usr/hdp/current/kafka-broker/bin/"


parser = argparse.ArgumentParser(description='Tools to launch KPI report')
parser.add_argument("-c", '--config', help='Configuration file', required=True)
parser.add_argument('-v', '--verbose', help='Increase output verbosity',
                    action="store_true")



BROKER_LIST = "localhost:6667"

INFLUXDB_CONF = {
    "hostname":"hostname",
    "port": 8086,
    "username": "root",
    "password":"root",
    "db_name":"kafka"
}

def build_point(line, group, mesearment_name):
    elements = line.strip().split()
    topic = elements[1]
    partition = elements[2]
    lag = elements[5]
    tags = {
        "topic": topic,
        "consumer_group": group,
        "partition": partition
    }
    fields = {
        "lag": int(lag) if lag.isdigit() else None
    }
    metric = {
            "measurement": mesearment_name,
            "tags": tags,
            "fields": fields
    }
    return metric

def load_config(filename):
    try:
        config = json.loads(open(filename).read())
        return config
    except Exception as ex:
        logger.error("Error when loading config file {0}: {1}".format(filename, str(ex)))
        exit(1)

def getGroups(broker_list):
    cmd = "{0}/kafka-consumer-groups.sh --bootstrap-server {1} --list".format(KAFKA_BIN_DIR, broker_list)
    process = Popen(shlex.split(cmd), stdout=PIPE)
    (output, err) = process.communicate()
    ret = process.wait()
    if ret is not 0:
        logger.error("Error while trying to retrieve availible groups: {0} {1}".format(ret, err))
        exit(ret)
    groups = output.split("\n")
    logger.debug("Found groups: " + str(groups))
    return groups

def getGroupMetrics(mesearment_name, broker_list, group):
    cmd = "{0}/kafka-consumer-groups.sh --bootstrap-server {1} --describe --group {2}".format(KAFKA_BIN_DIR, broker_list, group)
    process = Popen(shlex.split(cmd), stdout=PIPE)
    (output, err) = process.communicate()
    ret = process.wait()
    if ret is not 0:
        logger.error("Error while trying to retrieve group {0} data: {1}".format(group, err))
        exit(ret)
    if str("is rebalancing.") in str(output):
        logger.debug("Group {0} is rebalancing, no data availible, skip.".format(group))
        return None
    lines = output.split('\n')[1:]
    metrics = [build_point(line, group, mesearment_name) for line in lines if line]
    metrics = filter(lambda metric: metric["fields"]["lag"] is not None, metrics)
    logger.debug("Sending metrics for group " + str(group))
    for metric in metrics:
        logger.debug(str(metric))
    return metrics


def main():
    global loglevel
    args = parser.parse_args()

    if args.verbose:
        loglevel = logging.DEBUG
        logger.setLevel(loglevel)
    config = load_config(args.config)
    broker_list = config.get("broker_list")

    groups = getGroups(broker_list)
    if len(groups) <= 0:
        logger.warn("No availible groups found, exit.")
        exit(0)

    measurement_name = config.get("measurement_name")
    influxdb_conf = config.get("influxdb")
    client = InfluxDBClient(influxdb_conf['hostname'], influxdb_conf['port'], influxdb_conf['username'],
                            influxdb_conf['password'], influxdb_conf['db_name'])

    for group in groups:
        if not group:
            continue
        logger.debug("Pulling data for group " + str(group))
        metrics = getGroupMetrics(measurement_name, broker_list, group)
        if metrics is not  None:
            client.write_points(metrics)


if __name__ == '__main__':
    main()
