import pandas as pd
import strat_config
import time

from sql import sqlDF,df_to_sql
from datetime import datetime
from log_notify import notify
from imp import reload
reload(strat_config)
from strat_config import config_df

class create_orders:
    
    def __init__(self):
        self.notify = notify()
        
    def read_sql(self,path) -> pd.DataFrame:
        file = open(path,'r')
        query : str = file.read()
        data : pd.DataFrame = sqlDF(query)
        return data
    
        
    def write(self):
        #need to extened this to multiprocessing once there is more than 1 strat
        while True:
            current_orders = self.read_sql(r'current_orders.txt')
            current_orders = current_orders.drop(['created_ts'],axis = 1)               
            for index,(strategy,direction,size) in config_df.iterrows():
                start = datetime.now()
                data = self.read_sql(rf'{strategy}.txt')
                end = datetime.now()
                print(f'Query executed in {(end-start).total_seconds()} seconds')
                if 'price' not in list(data.columns):
                    price = 'Last'
                    
                data['size'] = size
                data['price'] = price
                data['direction'] = direction
                data['strategy'] = strategy
                
                
                order = pd.merge(data,current_orders,left_on = list(data.columns),right_on = \
                                 list(data.columns),how = 'left',indicator='Exist')
                order['created_ts'] = datetime.now()
                order = order.loc[order['Exist'] == 'left_only']
                order = order.drop(['Exist'],axis = 1)
                time.sleep(5)
                if len(order)>0:
                    df_to_sql('tennis_orders',order)
                    print('Order written to table')
                    
                
        

if __name__ == '__main__':
    orders = create_orders()
    try:
        orders.write()
    except Exception as e:
        orders.notify.send_message(e,'strat script')
    
        
        
    


