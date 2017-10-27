# coding: utf-8
import re
import datetime
from pyspark.sql import Row
from pyspark import SparkConf, SparkContext

#Pattern em REGEX para delimitacao dos campos nas linhas do log
LOG_PATTERN = '^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)" (\d{3}) (\S+)'

#Mapa para conversao de dias na data /em ingles por causa dos registros em ingles
mapMes = {'Jan': 1, 'Feb': 2, 'Mar':3, 'Apr':4, 'May':5, 'Jun':6, 'Jul':7,'Aug':8, 'Sep': 9, 'Oct':10, 'Nov': 11, 'Dec': 12}

def parseLogData(data):
    #Converte em formato de data a date do log	
    return datetime.datetime(int(data[7:11]),
                             mapMes[data[3:6]],
                             int(data[0:2]),
                             int(data[12:14]),
                             int(data[15:17]),
                             int(data[18:20]))


def parseLogLinhaNasa(linha):
    #Converte uma linha do log em objeto python	
    match = re.search(LOG_PATTERN, linha)
    if match is None:
        return (linha, 0)
    sizeLog = match.group(9)
    if sizeLog == '-':
        size = long(0)
    else:
        size = long(match.group(9))
    return (Row(
		host          = match.group(1),
        timestamp     = parseLogData(match.group(4)),
        path          = match.group(6),
        status = int(match.group(8)),
        contentSize  = size
    ), 1)


def parseDeLogs():
    conf = SparkConf().setMaster("local").setAppName("TotalErroPorDia")
    sc = SparkContext(conf = conf)
    
    convertido_logs = (sc.textFile("hdfs://quickstart/user/semantix/NASA_ACCESS_LOG_TOTAL").map(parseLogLinhaNasa).cache())

    # Separa os logs completos de linhas incompletas
    dados_logs = (convertido_logs.filter(lambda log: log[1] == 1).map(lambda log: log[0]).cache())

    erro_logs = (convertido_logs.filter(lambda log: log[1] == 0).map(lambda log: log[0]))
    erro_logs_count = erro_logs.count()
    
    if erro_logs_count > 0:
        print 'Numero de linhas invalidas no log: %d' % erro_logs.count()
        for linha in erro_logs.take(20):
            print 'Linha Invalida: %s' % linha

    print 'Total de linhas lidas %d, Total de linhas convertidas com sucesso %d, Total de falhas na conversao %d' % (convertido_logs.count(), dados_logs.count(), erro_logs.count())
    return dados_logs

if __name__ == "__main__":

    dados_logs = parseDeLogs()

    registroErro = dados_logs.filter(lambda log: log.status==404).cache()
    registroErroTupla = registroErro.map(lambda log: (log.timestamp.day, 1))
    registroErroSomaData = registroErroTupla.reduceByKey(lambda a, b: a+b)
    registroErroOrdenado = registroErroSomaData.sortByKey().cache()
    print 'Quantidade de errors 404 por dia: %s' % registroErroOrdenado.collect()








