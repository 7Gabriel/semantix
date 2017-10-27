# semantix

Esse desafio foi realizado em uma maquina virtual Cloudera

No desenvolvimento não foi levado em consideração as melhores praticas de OO.

1 - concatenar os dois arquivos para que fique mais fácil o processamento
Para a verificação de cada arquivo não é necessario a concatenação

cat NASA_access_log_Aug95  NASA_access_log_Jul95 > NASA_ACCESS_LOG_TOTAL

2 Criar um diretorio dentro do hdfs

hdfs dfs -mkdir /user/semantix

3) enviar o dataset criado anteriormente com os log para o hdfs

hdfs dfs -copyFromLocal NASA_ACCESS_LOG_TOTAL /user/semantix

4) verificar se o arquivo esta no hdfs

hdfs dfs -ls /user/semantix


Foram criados 5 arquivos python, cada arquivo responde a uma análise do desafio

comando para executar os programas

spark-submit nomeprograma.py

Respostas 

Qual o objtivo do comando cache em Spark?
Armazena os objetos na memória para usopara que sejam acessados mais rapidamente.

O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em
MapReduce. Por quê?
No Spark a carga de dados ocorre de uma  vez e são levados para memoria, onde serão mais frequentemente utilizados e ficam acessiveis mais rapidamente 
o MapReduce é dependente de CPU. Embora seja mais rapido o Spark é preciso avaliar diferenças arquiteturais e quais tipos de operações serão realizadas.


Qual é a função do SparkContext ?
A função é conectar o Spark ao programa que está sendo desenvolvido, pode ser acessado como uma variavel para o uso dos recursos.

Explique com suas palavras o que é Resilient Distributed Datasets (RDD)
 RDD são estruturas de dados em memória e que são utilizadas para armazenar em cache os dados existentes entre os nós de um cluster,
 é o principal componente do Spark.
 
 GroupByKey é menos eficiente que reduceByKey em grandes dataset. Por quê?
 Por que o Spark combina a saida com uma chave comum em cada partição antes de fazer a coleta dos dados
 
 Explique o que o código Scala abaixo faz
 
 Lê um arquivo do HDFS no hadoop e cria um RDD chamado textFile, em seguida faz uma tranformação dos dados para gerar uma contagem de palavras,
 e por fim salva o resultado no HDFS. 


