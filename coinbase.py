import ssl
import websocket
import _thread as thread
import datetime
import csv

def ms_to_datetime(timestampms):
    res = datetime.datetime.fromtimestamp(timestampms/1000.0).isoformat()
    return res[0:13]

def ts_to_datetime(timestampms):
    res = datetime.datetime.fromtimestamp(timestampms).isoformat()
    return res[0:13]

class coinbase(object):
    def __init__(self):
        #initialzing websocket and use dictionary to store change of the order book
        self.logon_msg = '{"type": "subscribe","product_ids": ["BTC-USD","ETH-USD"],"channels": ["level2","heartbeat","ticker"]}'
        websocket.enableTrace(True)
        self.ws = websocket.WebSocketApp("wss://ws-feed.pro.coinbase.com",
                                    on_message=self.on_message,
                                    on_error=self.on_error,
                                    on_close=self.on_close,
                                    on_open=self.on_open)

        self.dict_change = {}
        self.last_date_hour = ''
        #prepare csv for storing the data
        with open(r"C:\Users\yuan\PycharmProjects\evisx_copy\changes_coinbase_btc.csv",'w+',newline = '') as self.coinbase_btc_changes:
            self.writer = csv.writer(self.coinbase_btc_changes)
            self.writer.writerow(['change','date_hour'])
        with open(r"C:\Users\yuan\PycharmProjects\evisx_copy\trades_coinbase_btc.csv",'w+',newline = '') as self.coinbase_btc_trades:
            self.writer2 = csv.writer(self.coinbase_btc_trades)
            self.writer2.writerow(['date_hour','price','volume'])
        with open(r"C:\Users\yuan\PycharmProjects\evisx_copy\trades_coinbase_eth.csv",'w+',newline = '') as self.coinbase_eth_trades:
            self.writer3 = csv.writer(self.coinbase_eth_trades)
            self.writer3.writerow(['date_hour','price','volume'])

        self.ws.on_open = self.on_open
        self.ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})


    def on_message(self, message):
        splited_mes = message[1:-1].split(',')
        #check if the data is trade data or order book changes
        if splited_mes[0] == '"type":"l2update"' and splited_mes[1] == '"product_id":"BTC-USD"':

            info_list = message[54:-40].split(',')
            change = float(info_list[2].strip('"'))
            date_hour = message[-29:-16]
            if date_hour != self.last_date_hour and self.last_date_hour in self.dict_change:
                #write the change data with the correct datetime
                with open(r"C:\Users\yuan\PycharmProjects\evisx_copy\changes_coinbase_btc.csv", 'a+',
                          newline='') as self.coinbase_btc_changes:
                    self.writer = csv.writer(self.coinbase_btc_changes)
                    self.writer.writerow([self.dict_change[self.last_date_hour], self.last_date_hour])
                self.last_date_hour = date_hour
            elif date_hour != self.last_date_hour and self.last_date_hour not in self.dict_change:
                self.last_date_hour = date_hour

            else:
                try:
                    #if the datetime is in the dict and is the same hour, add the volume to the current one
                    self.dict_change[date_hour] += change
                except:#if there is no datetime in the dict, initialize the first entry
                    self.dict_change[date_hour] = 0

        elif splited_mes[0] == '"type":"ticker"' and splited_mes[2] == '"product_id":"BTC-USD"':
            #if it is trade data of BTC-USD, write to the correct data set
            price = float((splited_mes[3].split(':'))[1].strip('"'))
            volumn = float((splited_mes[-1].split(':'))[1].strip('"'))
            date_hour = (splited_mes[-3])[8:21]

            with open(r"C:\Users\yuan\PycharmProjects\evisx_copy\trades_coinbase_btc.csv", 'a+',
                      newline='') as self.coinbase_btc_trades:
                self.writer2 = csv.writer(self.coinbase_btc_trades)
                self.writer2.writerow([date_hour,price,volumn])

        elif splited_mes[0] == '"type":"ticker"' and splited_mes[2] == '"product_id":"ETH-USD"':
            #if it is trade data of ETH-USD, write to the correct data set
            #this data set is not used since bthetc.py can handle the ETH-BTC data sucscription
            price = float((splited_mes[3].split(':'))[1].strip('"'))
            volumn = float((splited_mes[-1].split(':'))[1].strip('"'))
            date_hour = (splited_mes[-3])[8:21]

            with open(r"C:\Users\yuan\PycharmProjects\evisx_copy\trades_coinbase_eth.csv", 'a+',
                      newline='') as self.coinbase_eth_trades:
                self.writer3 = csv.writer(self.coinbase_eth_trades)
                self.writer3.writerow([date_hour, price, volumn])

    def on_error(self, error):
        print(error)

    def on_close(self):
        print("### closed ###")

    def on_open(self):
        def run(*args):
            self.ws.send(self.logon_msg)
        thread.start_new_thread(run, ())
