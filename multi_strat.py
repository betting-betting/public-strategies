from threading import Thread
from datetime import datetime
import time

def func(word1,word2,word3):
    time.sleep(5)
    print(f'{word3[-1]} started at {datetime.now()}')
    print(f'{word1} {word2} {word3}')
    
    
t1 = Thread(target = func,args = ['Hello','World','This is func 1'])
t2 = Thread(target = func,args = ['Hello','World','This is func 2'])

t1.start()
print(f't1 started at {datetime.now()}')
t2.start()
print(f't2 started at {datetime.now()}')

t1.join()
print('t1 done')
t2.join()
print('t2 done')





    
    
from strat_config import config_df
def strat_func(strategy,direction,size):
    print(f'{strategy} thread started')
    while True:
        try:
            current_orders = self.read_sql(r'current_orders.txt')
            current_orders = current_orders.drop(['created_ts'],axis = 1) 
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
            
            
            order = pd.merge(data,current_orders,left_on = list(data.columns),right_on = list(data.columns),how = 'left',indicator='Exist')
            order['created_ts'] = datetime.now()
            order = order.loc[order['Exist'] == 'left_only']
            order = order.drop(['Exist'],axis = 1)
            time.sleep(5)
            if len(order)>0:
                df_to_sql('tennis_orders',order)
                print('Order written to table')
        except Exception as e:
            print(f'{strategy} thread died with error: {e}') #this should be a send message
            
    
for index,(strategy,direction,size) in config_df.iterrows():
    Thread(target = strat_func,args = [strategy,direction,size]).start()



