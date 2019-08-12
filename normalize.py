import os
import mysql.connector
from mysql.connector import errorcode
from re import sub
from decimal import Decimal
import re
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy import exc
from dotenv import load_dotenv
from sqlalchemy.exc import IntegrityError
import logging 
import sys
import time
import uuid
import random
import hashlib
import re

logging.basicConfig(filename='companies.log', filemode='w', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASS = os.getenv("MYSQL_PASS")
MYSQL_DB   = os.getenv("MYSQL_DB")

DB_URI = "mysql+mysqlconnector://gennadii_turutin:25sfxc9ifjs67gflmnbas8d7yfsdf@35.193.109.28:3306/external_data"

engine = create_engine(DB_URI) 
session = scoped_session(sessionmaker()) 
session.remove()
session.configure(bind=engine, autoflush=False, expire_on_commit=False)

def fetch_records():
    print("Fetching ")
    global session

    current_page = 0;
    offset = 0;
    page_size = 100;

    while True:

        results = session.execute("""
                                    SELECT name, address1, address2 
                                    FROM `NycAcrisParty`
                                    WHERE address1 LIKE '%C/O%'
                                     OR address1 LIKE '%C/O:%'
                                     OR address2 LIKE '%C/O%'
                                     OR address2 LIKE '%C/O:%'
                                    LIMIT :offset, :page_size;
                                    """, 
                                    {
                                    'offset':offset,
                                    'page_size':page_size,
                                    }
                                )
        results = list(results)

        for result in results:
            add_record_db(result)

        if len(results) < page_size:
            break;

        current_page += 1;
        offset = current_page * page_size;

def create_db():
    global session
    try:
        session.execute("""CREATE TABLE IF NOT EXISTS `Companies`(
            companyId CHAR(64), 
            companyName VARCHAR(200), 
            parentCompanyName VARCHAR(200),
            parentCompanyAddress VARCHAR(200),
            PRIMARY KEY (companyId)
            )
            ENGINE=InnoDB
        """)

        session.commit()
        print("The table has been created")

    except Exception as e:
        print("Error: ", e)

def clean(data):

    data = data.replace(':', ' ').replace(' COMPANY', ' CO').replace(' ESQUIRE', ' ESQ').replace(' INCORPORATED', ' INC').replace(' MGMT', ' MANAGEMENT').replace(' & ', '&').replace('& ', '&').replace(' CORPORATION', ' CORP').strip()
    data = re.sub(r'[^\w\s\']', " ", data)
    data = re.sub(' +', ' ', data) 
    data = re.sub('^J P ', 'JP', data)
    data = re.sub('^JP ', 'JP', data)
    data = re.sub('^J P', 'JP', data)
    data = re.sub('^U S ', 'US ', data)
    data = re.sub('^M T ', 'MT ', data)
    data = re.sub(' L P ', ' LP ', data)
    data = re.sub(' L P$', ' LP', data)
    data = re.sub(' L L C ', ' LLC ', data)
    data = re.sub(' L L C$', ' LLC', data)
    data = re.sub(' N A ', ' NA ', data)
    data = re.sub(' N A$', ' NA', data)
    data = re.sub('^S L ', 'SL ', data).strip()

    return data


def divide(data):
    parentCompanyName = re.split('( [0-9]+)',  data)[0]
    parentCompanyAddress = ' '.join(re.split('( [0-9]+)', data)[1:]).strip()
    parentCompanyAddress = re.sub(' +', ' ', parentCompanyAddress) 

    return parentCompanyName, parentCompanyAddress

def fetch_variables(data):
    partitionedParentData = data.partition("C/O")[2] 
    cleanedParentData = clean(partitionedParentData)
    parentCompanyName, parentCompanyAddress = divide(cleanedParentData)

    return parentCompanyName, parentCompanyAddress

def add_record_db(data):
    global session
    
    companyName = clean(data[0])
    
    if "C/O" in data[2]:
        parentCompanyName, parentCompanyAddress = fetch_variables(data[2])
    else:
        parentCompanyName, parentCompanyAddress = fetch_variables(data[1])
    
    hashing = companyName + parentCompanyName 
    companyId = hashlib.md5(hashing.encode()) 

    try:
        session.execute("""INSERT INTO `Companies` VALUES (
                            :companyId,
                            :companyName, 
                            :parentCompanyName, 
                            :parentCompanyAddress 
                            )""", 
                            {
                            'companyId': companyId.hexdigest(),
                            'companyName':companyName, 
                            'parentCompanyName':parentCompanyName,
                            'parentCompanyAddress':parentCompanyAddress
                            }
                        )
        session.commit()
        print("The record has been added")

    except sqlalchemy.exc.IntegrityError:
        print("Error: ", sys.exc_info())
        #logging.warning(sys.exc_info())

    except Exception as e:
        print("Error: ", e)
        #logging.error(sys.exc_info())


def normalize(data):
    print("Normalization started")
    try:
        print("Success")
    except Exception as e:
        print("Error: ", e)
    pass


if __name__ == "__main__":
    print("Script started")
    #connect_db()
    create_db()
    fetch_records()
    print("Finished")