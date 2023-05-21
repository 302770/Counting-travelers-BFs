import pandas as pd
import numpy as np
import math
import mmh3
from datetime import datetime
from bitarray import bitarray
# from hashlip import sha256
import warnings
warnings.filterwarnings("ignore")
BFLEN =14378
NHASH=10

################################
##     Bloom Filter class     ##
################################

class BloomFilter ( object ):

    '''
        Class for Bloom filter, using murmur3 hash function
    '''
    def __init__(self, items_count, fp_prob):
        
        '''
        items_count : int
            Number of items expected to be stored in bloom filter
        fp_prob : float
            False Positive probability in decimal
        '''
        # False possible probability in decimal
        self.fp_prob = fp_prob
        # Size of bit array to use
        self.size = self.get_size(items_count, fp_prob)
        # number of hash functions to use
        self.hash_count = self.get_hash_count(self.size, items_count)
        # Bit array of given size
        self.bit_array = bitarray(self.size)
        # initialize all bits as 0
        self.bit_array.setall(0)
        
        
    def add(self, item):
        '''
        Add an item in the filter
        '''
        digests = []
        for i in range(self.hash_count):
            # create digest for given item.
            # i work as seed to mmh3.hash() function
            # With different seed, digest created is different
            digest = mmh3.hash(item, i) % self.size
            digests.append(digest)
            # set the bit True in bit_array
            self.bit_array[digest] = True
        return self.bit_array

    
    def check(self, item):
        '''
        Check for existence of an item in filter
        '''
        for i in range(self.hash_count):
            digest = mmh3.hash(item, i) % self.size
            if self.bit_array[digest] == False:
                return False
        return True
    
    
    def get_size(self, n, p):
        ''' 
        Return the size of bit array(m) to used using
        following formula
        '''
        m = 14378
        # m = int(-(n * math.log(p))/(math.log(2)**2))
        # print(m)
        return int(m)
    
    
    def get_hash_count(self, m, n):
        '''
        Return the hash function(k) to be used using
        following formula
        '''
        k = 10
        # k = int((m/n) * math.log(2))
        # print(k)
        return int(k)

###################################################
########           Helper Methods         #########
###################################################
'''
Mergig several dataframes that have been extracted in different time windows(Epochs)

'''

def merge_dfs(df_list):

    final_df = df_list[0]
    # final_df.to_csv("check-in-1-k5.csv")
    print(final_df)
    cntr = 0
    for df in df_list[1:]:
        # df.to_csv(r"check-out-1-k5 {}.csv".format(cntr))
        # print(df)
        final_df = final_df.append(df)

    return final_df

'''
time window method to use as Epoch in the Bloom filter at the first point of check in and check out counter

returns:
        the trips that happend during each Epoch, like all trips between (5:00, 6:00)
'''
def get_time_frames(time_window = 120):
    timestamps = []
    if time_window <= 60:
        end_time = 2300 + time_window
        for i in range(500, end_time, 100):
            cnt = int(60/time_window)
            for j in range(cnt):
                temp_time = i + j*time_window
                if temp_time <= 2400:
                    timestamps.append(temp_time)

    if time_window > 60:
        base_time_window = 60
        end_time = 2300 + base_time_window
        for i in range(500, end_time, 100):
            cnt = int(60/base_time_window)
            for j in range(cnt):
                temp_time = i + j*base_time_window
                if temp_time <= 2400:
                    timestamps.append(temp_time)

        step = int(time_window / 60)
        timestamps = timestamps[::step]


    timeframes = [(timestamps[i], timestamps[i+1]) for i in range(len(timestamps)) if i!=len(timestamps)-1]

    # print(*timeframes, sep="\n")

    return timeframes


###################################################
########           Epochs length         #########
###################################################

'''
Extracting trips during epochs for each station
'''
def divide_with_time_window(df, time_col_name="check_in", station_name_col="in_p_gis", time_window=60):
    temp_timeframes = get_time_frames(time_window)
    timeframes = []
    for tf in temp_timeframes:
        tf0 = datetime.strptime(str(tf[0]),"%H%M")
        tf1 = datetime.strptime(str(tf[1]),"%H%M")
        timeframes.append((tf0, tf1))
    
    output_dfs = []  ##### contains several dataframes of time windows
    for time_frame in timeframes:
        stations = set(list(df[station_name_col]))

        for station in stations:
            output_df = pd.DataFrame()
            for index,row in df.iterrows():
                ##### finding all trips for the every single station in a specific time frame to assign correction
                if row[time_col_name] >= time_frame[0] and row[time_col_name] < time_frame[1] and row[station_name_col] == station:
                    output_df = output_df.append(row)
            output_dfs.append(output_df)
    '''
    the output is a list of epochs dataframe for each station, which later Bloom filter will be applied on each of them
    '''
    return output_dfs
    #####################################################
    # inserting card identifiers intp Bloom Filters #####
    #####################################################
def bloom_filter(dfs):
    bloomfilters_in = []
    for df in dfs:
        if len(df)>0:
            n = 1000### number of identifiers during each epoch
            p = 0.001 ### false positive
            bloomf_in = BloomFilter(n,p)
            for bid in df["binary_ids"]:
                bloomf_in.add(str(bid))
            bloomfilters_in.append(bloomf_in.bit_array)
        else:
            bloomfilters_in.append(bitarray([0]*BFLEN))
    return(bloomfilters_in)

###################################
##########    Main      ############
####################################
from bfs import BloomFilter

if __name__ == "__main__":

    print(" Loading Data . . . ")
    
    smart_card_df = pd.read_csv('gt1000.csv',";")
    smart_card_df["check_in"] = pd.to_datetime(smart_card_df["check_in"], format="%H%M")
    smart_card_df["check_out"] = pd.to_datetime(smart_card_df["check_out"], format="%H%M")
    ########################################
    #####         Epoch length        ######
    ########################################
    print("Bloom filter is loading ----------------")
    check_ins_dfs = divide_with_time_window(smart_card_df, time_col_name="check_in", station_name_col="in_p_gis",  time_window=5)##time window is epoch length
    check_outs_dfs = divide_with_time_window(smart_card_df, time_col_name="check_out", station_name_col="out_p_gis", time_window=5)##time window is epoch length
    
    ########################################
    #####     Calling Bloom Filters   ######
    ########################################
    
    bloomfilters_in = bloom_filter(check_ins_dfs)
    bloomfilters_out = bloom_filter(check_outs_dfs)
    ########################################
    #####     Intersection operation  ######
    ########################################
    inter_ab=[]
    for i,dep_a in zip(range(len(bloomfilters_in)), bloomfilters_in):
        dep_b_list = bloomfilters_out[i+1:i+10]
        for dep_b in dep_b_list:          
            multiply=[a & b for a,b in zip(dep_a,dep_b)]
            inter_ab.append(multiply)
            
    union_all = inter_ab[0]
    for i in range(1, len(inter_ab)):
        union_all = [a | b for a, b in zip(union_all, inter_ab[i])]
    tm = sum(union_all)   
    ##########################################
    ########          accuracy      ########## 
    ##########################################

    c=int(-(BFLEN/NHASH)*np.log(1- (tm/BFLEN)))
    ct=1000
    accuracy = max(1 - (abs(c - ct) / ct), 0)
    print("Accuracy:",accuracy)

    
    
        