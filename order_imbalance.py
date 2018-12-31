from websocket import create_connection
import json
import requests
import hashlib
import hmac
from pprint import pprint
import logging
import logging.handlers
from datetime import datetime as dt

## order of columns when dumping trade data: ts, side, size, price

buyDict = {}
sellDict = {}

def setup_logger(x): # for loggin top 10 prices and sizes from order book
    if (x == 1):
        logger = logging.getLogger(str(x))
    else:
        logger = logging.getLogger(str(x))
    logger.setLevel(logging.INFO) # Change to DEBUG if you want some more info
   
    if (x == 1):
        ch = logging.handlers.TimedRotatingFileHandler('xbtusd.dump', when='h', backupCount=100)
    elif (x == 2):
        ch = logging.handlers.TimedRotatingFileHandler('orderBook.dump', when='h', backupCount=100)

    # create formatter
    formatter = logging.Formatter("%(message)s")
    # add formatter to ch
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger

logger = setup_logger(1)
logger2 = setup_logger(2)

def firstPacket(order_book):
    for order in order_book:
        if (order["side"] == "Buy"):
            buyDict[order["id"]] = {'price': order["price"], 'size': order["size"]}
        if (order["side"] == "Sell"):
            sellDict[order["id"]] = {'price': order["price"], 'size': order["size"]}

def updateOrder(order_book):
    shouldPrint = 0
    for order in order_book:
        if (order["side"] == "Buy"):
            buyDict[order["id"]].update({'size': order["size"]})
            top10buy = sorted(list(buyDict.keys()))[:10]
            if (order["id"] in top10buy):
                shouldPrint = 1
        if (order["side"] == "Sell"):
            sellDict[order["id"]].update({'size': order["size"]})
            top10sell = sorted(list(sellDict.keys()), reverse = True)[:10]
            if (order["id"] in top10sell):
                shouldPrint = 1

    return shouldPrint                

def cancelOrder(order_book):
    shouldPrint = 0
    top10buy = sorted(list(buyDict.keys()))[:10]
    top10sell = sorted(list(sellDict.keys()), reverse = True)[:10]
    top10 = top10buy + top10sell

    for order in order_book:
        if (order["id"] in top10):
            shouldPrint = 1
        if (order["side"] == "Buy"):
            del buyDict[order["id"]]
        if (order["side"] == "Sell"):
            del sellDict[order["id"]]

    return shouldPrint
            
def fireOrder(order_book):
    shouldPrint = 0
    for order in order_book:
        if (order["side"] == "Buy"):
            buyDict[order["id"]] = {'price': order["price"], 'size': order["size"]}
            top10buy = sorted(list(buyDict.keys()))[:10]
            if (order["id"] in top10buy):
                shouldPrint = 1
        if (order["side"] == "Sell"):
            sellDict[order["id"]] = {'price': order["price"], 'size': order["size"]}
            top10sell = sorted(list(sellDict.keys()), reverse = True)[:10]
            if (order["id"] in top10sell):
                shouldPrint = 1

    return shouldPrint

def run():
    ws = create_connection("wss://www.bitmex.com/realtime?subscribe=orderBookL2:XBTUSD,trade:XBTUSD")
    
   
    while(1):
        try:
            result = json.loads(ws.recv())
            logger2.info("{}, {}".format(dt.now(), result))
            if (result["table"] == "orderBookL2"):
                if result["action"] == "partial":
                    firstPacket(result["data"])
                    printToTerminal()
                elif result['action'] == "update":
                    if (updateOrder(result["data"]) == 1):
                        printToTerminal()
                elif result['action'] == "delete":
                    if (cancelOrder(result["data"]) == 1):
                        printToTerminal()
                elif result['action'] == "insert":
                    if (fireOrder(result["data"]) == 1):
                        printToTerminal()
            if (result["table"] == "trade"):
                for i in range(len(result["data"])):
                    ## order of columns when dumping trade data: ts, side, size, price
                    logger.info("{}, {}, {}, {}".format(dt.now(), result["data"][i]["side"], result["data"][i]["size"], result["data"][i]["price"])) 

        except:
            continue

def printToTerminal():
    top10buy = sorted(list(buyDict.keys()))[:10]
    top10sell = sorted(list(sellDict.keys()), reverse = True)[:10]
    top10 = top10buy + top10sell
    p10s = []
    s10s = []
    for i in range(10):
        p10s.append(buyDict[sorted(top10buy, reverse = True)[i]]["price"])
        s10s.append(buyDict[sorted(top10buy, reverse = True)[i]]["size"])
    for i in range(10):
        p10s.append(sellDict[top10sell[i]]["price"])
        s10s.append(sellDict[top10sell[i]]["size"])
    # print("{},{}".format(dt.now(), p10s + s10s))
    logger.info("{},{}".format(dt.now(), p10s + s10s))

if __name__ == "__main__":  
    run()
