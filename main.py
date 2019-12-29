from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col

def Dist(s, t, td):
    i = 0
    if s == t:
        return 0
    tmp = [s]
    while i < td:
        i+=1
        N = df.select('to').where(df['from'].isin(tmp))
        if N.select('to').where(N['to'].isin([t])).count():
            return i 
        tmp = [item['to'] for item in N.collect()]
    return td

def ALG1(S, k):
    total = 0
    f = df.select('to').where(df['from'].isin([]))
    for i in range(k):
        SS_HDC = high_degree_centrality.filter(col('from').isin(S))
        max_value = S_HDC.select(F.max('count')).collect()[0][0]
        s = S_HDC.select('from').where(S_HDC['count'] == max_value).limit(1).collect()[0][0]
        S.remove(s)
        N1 = df.select('to').where(df['from'] == s)
        N2 = df.select('to').where(df['from'].isin( [item['to'] for item in N1.collect()]))
        tmp =N1.union(N2)
        total += tmp.count()
        f.union(tmp)
    unique = f.count()
    COV = 100 - (unique/total)*100
    return COV

def ALG3(k):
    return ALG2(K,2)

def ALG4(k, td, theta):
    S = []
    L = []
    while len(S) < k:
        tmp = high_degree_centrality.where(~high_degree_centrality['from'].isin(L))
        max_value = tmp.select(F.max('count')).collect()[0][0]
        s = tmp.select('from').where(tmp['count'] == max_value).limit(1).collect()[0][0]
        sel = True
        L.append(s)
        for item in S:
            if Dist(s,item, td) < td:
                N1 = df.select('to').where(df['from'] == s)
                N2 = df.select('to').where(df['from'] == item)
                NO = N1.intersect(N2).count()
                if NO >= theta:
                    sel = False
                    break
        if sel:
            S.append(s)
    return S

def ALG5(k, td, theta, beta):
    S = []
    L = []
    while len(S) < k:
        tmp = high_degree_centrality.where(~high_degree_centrality['from'].isin(L))
        max_value = tmp.select(F.max('count')).collect()[0][0]
        s = tmp.select('from').where(tmp['count'] == max_value).limit(1).collect()[0][0]
        sel = True
        L.append(s)
        for item in S:
            d = Dist(s,item, td)
            if d < td:
                N1 = df.select('to').where(df['from'] == s)
                N2 = df.select('to').where(df['from'] == item)
                NO = N1.intersect(N2).count()
                if d == 1:
                    inf = 0.01 + ((0.01)**2 * NO )
                else:
                    inf = (0.01)**2 * NO
                if NO >= theta and inf >= beta:
                    sel = False
                    break
        if sel:
            S.append(s)
    return S


if __name__ = '__main__':
    dataf = open('./facebook_combined.txt','r')
    data = [[int(x.split(' ')[0]),int(x.split(' ')[1])] for x in dataf.readlines()]
    df = spark.createDataFrame(data, ['from', 'to'])
    high_degree_centrality = df.groupBy('from').count().sort(col('count'))

