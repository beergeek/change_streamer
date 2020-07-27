# MongoDB Change Streamer

This repo contains several Python scripts designed to retrieve changes from MongoDB databases and play them back (via change streams).

The following tools are in this repository:

* event_watcher (to retrieve change events from selected MongoDB deployment)

# Table of Contents

1. [Pre-reqs](#pre-reqs)
2. [Details](#details)
    * [event_watcher](#event_watcher)
3. [Permissions](#permissions)

# Details

## Pre-reqs

The required user permissions for each script are described in the [Permissions](#permissions) section.

The database and user(s) must be configured (with permissions) before the scripts will function.

## watcher

This script retrieve change events (insert, update, and delete operations) from the MongoDB deployment via a change stream on the deployment.

The script uses a configuration file (`watcher.conf`) that normally resides in the same location as the script.

The configuration file has the following format (__NOTE__: none of the string have quotes):

```shell
[DATA_DB]
connection_string=mongodb://<USERNAME>:<PASSWORD>@<HOST>:<PORT>/?replicaSet=<REPLICA_SET_NAME>&<OTHER_OPTIONS>
timeout=<TIMEOUT_VALUE>
ssl_enabled=<BOOLEAN_VALUE>
ssl_pem_path=<PATH_TO_PEM_FILE>
ssl_ca_cert_path=<PATH_TO_CA_CERT>
data_file=<ABSOLUTE_PATH_FOR_OUTPUT_FILE>
full_document=<default_OR_updateLookup>
event_pipeline=<AGGREGATION_PIPELINE_QUERY_FOR_FILTER>

[general]
debug=<BOOLEAN_VALUE>
```

An example:

```shell
[audit_db]
connection_string=mongodb://auditor%%40MONGODB.LOCAL@om.mongodb.local:27017/?replicaSet=repl0&authSource=$external&authMechanism=GSSAPI
timeout=2000
ssl_enabled=True
ssl_pem_path=/data/pki/mongod3.mongodb.local.pem
ssl_ca_cert_path=/data/pki/ca.cert
event_pipeline=[{'$match': {'fullDocument.un': {$in: ['ivan','vigyan','terry','loudSam']}}]
data_file=/home/ryder/data.json
full_document=default

[GENERAL]
debug=false
```

NOTE that URL encoded special characters require double `%`, e.g `@` would be `%%40`.

An example that is similar to this script can be found in the section below.

Both sections are mandatory, as well as the `connection_string` option, but the `timeout` and `debug` are option (having defaults of 10 seconds and `false` respectivetly). The optional `event_pipeline` is a change stream pipeline to filter events. SSL/TLS settings for both databases are optional, but if `ssl_enabled` is `True` then `ssl_pem_path` and `ssl_ca_cert_path` must exist. SSL/TLS default is `False`. The `full_document` can only be `default` or `updateLookup` (see MongoDB documentaiton for this.), `default` is the default.

### Setup

This script should reside on a single server, most likely one of the Audit DB nodes.

The following non-standard Python modules are required (and dependancies):

* [pymongo](https://pypi.org/project/pymongo/)
* [configparser](https://pypi.org/project/configparser/)

The script and config file can be located in the same directory, but if the config file is in a different location the `--config` command line argument can be used to change the location.

A systemD service file can be created to run these scripts automatically on start:

```shell
[Unit]
Description=Watcher Script for MongoDB Auditing
After=network.target

[Service]
User=mongod
Group=mongod
ExecStart=/bin/python /data/scripts/watcher.py
Type=simple

[Install]
WantedBy=multi-user.target
```

## Permissions

For the `watcher` script the user will need to have `read` privileges on all non-system databases and collections.

Example:

```JSON
{
  "role": "watcher",
  "roles": [],
  "privileges": [
    {
      "resource": {"db": "", "collection": ""},
      "actions": [ "find", "changeStream" ]
    }
  ]
}
```
