import threading
from pymongo import MongoClient
import os
import json
from urllib import request

import logging
logging.basicConfig(filename='log.log',level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class SingletonDecorator:
    def __init__(self,clase):
        self.clase = clase
        self.instancia = None
    def __call__(self,*args,**kwds):
        if self.instancia == None:
            self.instancia = self.clase(*args,**kwds)
        return self.instancia

class DataDB(object):
    def __init__(self):
        self.succes = False
        try : 
            logging.info("+ Building MongoDB connector...")
            db_connect = str(os.environ['CONNECT_STRING'])        
            client = MongoClient(db_connect)
            if len(client.list_database_names()) > 0:
                db = client.FX
                self.collection = db.FX
                self.succes = True
                logging.info("- Correctly builded MongoDB connector...")
            else:
                logging.error("!!! Hubo un error en la base de datos, no hay bases de datos.")
        except Exception as e:
            logging.error("!!! ERROR BUILD MONGODB : de construcción de la base de datos.",e)
            #print("ERROR DB BUILD:  ",e)

    
    # data : list of dictionaries for the database
    def append(self, data):
        result = False
        try:
            self.collection.insert_many(data)
            result = True
        except Exception as e:
            logging.warning("!!! ERROR APPEND MONGODB",e)
            #print("ERROR DB APPEND:  ",e)
        finally:
            return result


class FX(object):
    def __init__(self):
        logging.info("+ Building FX connector...")
        self.fx_token = str(os.environ['FX_TOKEN']) 
        if 'FX_PAIRS' in os.environ:
            self.pairs = str(os.environ['FX_PAIRS']).split(" ") 
            logging.info("* Using specific FX pairs : " + " ".join(self.pairs))
        else:
            self.pairs = ['EURUSD', 'USDJPY', 'GBPJPY', 'GBPUSD', 'EURGBP', 'EURJPY']
            logging.info("* Using default FX pairs : " + " ".join(self.pairs))
        logging.info("- Correctly builded FX connector...")


    def get(self):
        result = None
        try:
            result = json.loads(request.urlopen("http://forex.1forge.com/1.0.2/quotes?pairs=" + ','.join(self.pairs) + '&api_key=' + self.fx_token).read().decode())
        except Exception as e:
            logging.warning("!!! ERROR GET FX",e)
            #print("ERROR FX GET:  ",e)
        finally:
            return result


logging.info("BEGUIN FX FEED MONGODB")

# Transform them in singelton
DataDB = SingletonDecorator(DataDB)
FX = SingletonDecorator(FX)


def run():
    threading.Timer(120.0, run).start()
    result = FX().get()
    if not result is None:
        DataDB().append(result)


if __name__ == "__main__":
    database = DataDB()
    if not database.succes:
        logging.error("!!! ERROR in databse connection")
    else:
        fx = FX()
        run()