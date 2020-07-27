try:
 import argparse
 import ast
 import configparser
 import datetime
 import json
 import logging
 import os.path
 import pymongo
 import re
 import signal
 import sys
 import time
 from pymongo.errors import DuplicateKeyError, OperationFailure
 from bson.json_util import dumps
except ImportError as e:
 print(e)
 exit(1)

def write_resume_token(signum, frame):
 if resume_token:
   outfile = open(sys.path[0] + '/.resume_token', 'w')
   outfile.write(resume_token)
   outfile.close()
   logging.info("RESUME TOKEN: %s" % (resume_token))
 logging.info("TERMINATING PROCESSING: %s" % datetime.datetime.now())
 sys.exit(0)

# global variable
resume_token = None
signal.signal(signal.SIGINT, write_resume_token)
signal.signal(signal.SIGTERM, write_resume_token)

def get_cmd_args():
 parser = argparse.ArgumentParser(description='Script to process MongoDB change stream')
 parser.add_argument('--config','-c', dest='config_file', default=sys.path[0] + '/watcher.conf', required=False, help="Alternative location for the config file")
 parser.add_argument('--log','-l', dest='log_file', default=sys.path[0] + '/watcher.log', required=False, help="Alternative location for the log file")
 parser.add_argument('--token','-t', dest='token_file', default=sys.path[0] + '/.resume_token', required=False, help="Alternative location for the token file (make it hidden)")
 return parser.parse_args()

# Get config setting from `watcher.config` file
def get_config(args):
 LOG_FILE = args.log_file
 CONF_FILE = args.config_file
 if os.path.isfile(CONF_FILE) == False:
   logging.basicConfig(filename=LOG_FILE,level=logging.ERROR)
   logging.error('The `watcher.conf` file must exist in the same directory as the Python script')
   print('\033[93m' + 'The `watcher.conf` file must exist in the same directory as the Python script, exiting' + '\033[m')
   sys.exit(1)

 config = configparser.ConfigParser()
 config.read(CONF_FILE)
 config_options = {}
 try:
   config_options['DEBUG'] = config.getboolean('GENERAL','debug', fallback=False)
   config_options['DATA_DB_CONNECTION_STRING'] = config.get('DATA_DB','connection_string')
   config_options['DATA_DB_SSL'] = config.getboolean('DATA_DB','ssl_enabled',fallback=False)
   config_options['DATA_FILE'] = config.get('DATA_DB','data_file',fallback=(sys.path[0] + '/data_file.json'))
   config_options['DATA_DB_SSL_PEM'] = config.get('DATA_DB','ssl_pem_path',fallback=None)
   config_options['DATA_DB_SSL_CA'] = config.get('DATA_DB', 'ssl_ca_cert_path')
   config_options['DATA_DB_TIMEOUT'] = config.getint('DATA_DB','timeout', fallback=10)
   config_options['FULL_DOCUMENT'] = config.get('DATA_DB','full_document',fallback='default')
   temp_pipeline = config.get('DATA_DB','event_pipeline',fallback=None)
   if temp_pipeline is not None:
     config_options['PIPELINE'] = ast.literal_eval(temp_pipeline)
   else:
     config_options['PIPELINE'] = []
 except (configparser.NoOptionError,configparser.NoSectionError) as e:
   logging.basicConfig(filename=LOG_FILE,level=logging.ERROR)
   logging.error("The config file is missing data: %s" % e)
   print("""\033[91mERROR! The config file is missing data: %s.
It should be in the following format:
\033[92m
[DATA_DB]
connection_string=mongodb://auditor%%40MONGODB.LOCAL@data.mongodb.local:27017/?replicaSet=repl0&authSource=$external&authMechanism=GSSAPI
timeout=2000
ssl_pem_path=/data/pki/mongod3.mongodb.local.pem
ssl_ca_cert_path=/data/pki/ca.cert
event_pipeline=[{'$match': {'fullDocument.un': {$in: ['ivan','vigyan','terry','loudSam']}}] # aggregation pipeline if required
data_file=/opt/data.json # file to write change streams events to
full_document=None # choice of default or updateLookup

[GENERAL]
debug=false
\033[m""" % e)
   sys.exit(1)
 return config_options

# Get resume token, is exists
def get_resume_token():
 if os.path.isfile(token_file):
   token_handle = open(token_file,'r')
   retrieved_token = token_handle.readline().strip()
   token_handle.close()
 else:
   retrieved_token = None
 return retrieved_token

# Record our startup and config
def record_startup(config_array, debug=False):
 if debug == True:
   logging.info("STARTING PROCESSING: %s" % datetime.datetime.now())
   logging.debug("MONGODB CONNECTION STRING: %s" % re.sub('//.+@', '//<REDACTED>@', config_array['DATA_DB_CONNECTION_STRING']))
   logging.debug("RESUME TOKEN: %s" % resume_token)
   logging.debug("PIPELINE: %s" % config_array['PIPELINE'])
   print("MONGODB CONNECTION STRING: %s" % re.sub('//.+@', '//<REDACTED>@', config_array['DATA_DB_CONNECTION_STRING']))
   print("RESUME TOKEN: %s" % resume_token)
   print("PIPELINE: %s" % config_array['PIPELINE'])
 else:
   logging.info("STARTING PROCESSING: %s" % datetime.datetime.now())

# connection to the database
def db_client(db_data, debug=False):
 try:
   if db_data['DATA_DB_SSL_PEM'] is not None:
     deployment = pymongo.MongoClient(db_data['DATA_DB_CONNECTION_STRING'], serverSelectionTimeoutMS=db_data['DATA_DB_TIMEOUT'], ssl=True, ssl_certfile=db_data['DATA_DB_SSL_PEM'], ssl_ca_certs=db_data['DATA_DB_SSL_CA'])
   else:
     deployment = pymongo.MongoClient(db_data['DATA_DB_CONNECTION_STRING'], serverSelectionTimeoutMS=db_data['DATA_DB_TIMEOUT'], ssl=True, ssl_ca_certs=db_data['DATA_DB_SSL_CA'])
   result = deployment.admin.command('ismaster')
 except (pymongo.errors.ServerSelectionTimeoutError, pymongo.errors.ConnectionFailure) as e:
   logging.error("Cannot connect to DB, please check settings in config file: %s" %e)
   print("Cannot connect to DB, please check settings in config file: %s" %e)
   sys.exit(1)
 return deployment

def main():
 global resume_token
 global token_file
 # declare our log path
 LOG_FILE = sys.path[0] + '/watcher.log'

 # get our config
 args = get_cmd_args()
 token_file = args.token_file
 config_data = get_config(args)

 # retrieve and add our resume token to the config data
 # `resume_token` is a global variable so exit handlers can grab it easily
 config_data['resume_token'] = get_resume_token()
 resume_token = config_data['resume_token']

 # setup logging
 debug = config_data['DEBUG']
 if debug == True:
   logging.basicConfig(filename=LOG_FILE,level=logging.DEBUG)
 else:
   logging.basicConfig(filename=LOG_FILE,level=logging.INFO)

 # log our startup and the various settings
 record_startup(config_data, debug)

 # Connect to the mongodb database
 deployment = db_client(config_data, debug)


 # startAtOperationTime
 if resume_token:
   cursor = deployment.watch(resume_after={'_data': resume_token},pipeline=config_data['PIPELINE'],full_document=config_data['FULL_DOCUMENT'])
 else:
   cursor = deployment.watch(pipeline=config_data['PIPELINE'],full_document=config_data['FULL_DOCUMENT'])
 try:
   datafile = open(config_data['DATA_FILE'],'a')
   while True:
     document = next(cursor)
     resume_token = document.get("_id")['_data']
     if debug:
       logging.debug("RESUME_TOKEN: %s" % resume_token)
       print("RESUME_TOKEN: %s" % resume_token)
       print("DOCUMENT: %s" % dumps(document))
     datafile.write(dumps(document) + "\n")
   datafile.close()
 except OperationFailure as e:
   print(e.details)
   logging.error(e.details)


if __name__ == "__main__":
 logger = logging.getLogger(__name__)
 main()

