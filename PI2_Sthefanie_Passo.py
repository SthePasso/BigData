from pyspark import SparkConf, SparkContext
import pprint
import statistics
import math

def mapWord(word):
    produto = {
        'Id': -1,
        'ASIN': '',
        'title': '',
        'group': '',
        'salesrank': math.inf,
        'similar': '',
        'categories': [],
        'reviews': []
    }
    vet = word[1].split('\r\n')
    i=0
    while i < len(vet):
        if vet[i][0:3]=="Id:":
            produto['Id'] = vet[i][6:]
        if vet[i][0:5]=="ASIN:":
            produto['ASIN'] = vet[i][6:]
        if vet[i][0:9]=='  title: ': #
            produto['title'] = vet[i][9:]
        if vet[i][0:9]=="  group: ":
            produto['group'] = vet[i][9:]
        if vet[i][0:13]=='  salesrank: ':
            n = int(vet[i][13:])
            produto['salesrank'] = n if n != -1 else math.inf #
        if vet[i][0:11]=='  similar: ':
            produto['similar'] = vet[i][11:].split()[1:]
        if vet[i][0:14]=="  categories: ":
            num = int(vet[i][14:])
            produto['categories'] = []
            i+=1
            for j in range(0,num):
                produto['categories'].append(vet[i].strip())
                i+=1
        if vet[i][0:11]=='  reviews: ':
            num = int(vet[i][11:].split()[1])
            produto['reviews'] = []
            i+=1
            while i < len(vet):
                v = vet[i].strip().split()
                produto['reviews'].append([v[0], v[2], v[4], v[6], v[8]])
                i+=1
        i+=1
    return produto


def produtoBestWorst(word):
    return sorted(word['reviews'], key=lambda x: (x[2], x[4]))        
 
def MediaDeDias(word):
    d = {}
    for i in word['reviews']:
        if i[0] in d:
            d[i[0]].append(int(i[2]))
        else:
            d[i[0]] = [int(i[2])]
    #print(d)
    for i in d:
        d[i] = statistics.mean(d[i])
    return d

def MediaComentario(reviews):
    cont = 0
    positivo = 0
    for i in reviews:
        if i[2] == '5':
            cont += int(i[4])
            positivo +=1
    return cont/positivo if positivo !=0 else 0

def categoriaComAv(categories):
    cont = 0
    positivo = 0
    for i in categories['reviews']:
        if i[2] == '5':
            cont += int(i[4])
            positivo +=1
    v = []
    for i in categories['categories']:
        v.append( (i, (cont, positivo) ) )
    return v

def ClienteComentando(word):
    d = {}
    for i in word['reviews']:
        if i[1] in d:
            d[i[1]].append(int(i[2]))
        else:
            d[i[1]] = [int(i[2])]
    v = []
    for i in word['reviews']:
        v.append( (i[1] , d[i[1]]) )
    return v

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)
pp = pprint.PrettyPrinter(indent=1)
text_file = sc.newAPIHadoopFile(
    'amazon-meta.txt', "org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
    "org.apache.hadoop.io.LongWritable", "org.apache.hadoop.io.Text",
    conf={"textinputformat.record.delimiter": '\r\n\r\n'}) # '\r\n\r\n'
amazon = text_file.map(mapWord)
#pp.pprint(amazon.take(5))
'''
a = amazon
    .filter(lambda x: "0312982178" == x['ASIN']) \#retorna um boleando
    .map(produtoBestWorst) \#retorna para todos e muda valor
    .take(1)[0] \ #pegar somente o primeiro elemento do vetor pois se não da erro, ele quer pegar todo o vet
# print(vet)
# best = vet[0:5]
# rate = vet[-5:]
# pp.pprint(best)
# pp.pprint(rate)
pp.pprint(a[0:5])
pp.print(a[-5:])

b = amazon \
    .filter(lambda x: "0827229534" == x['ASIN']) \
    .take(1)[0] \
    .amazon.filter(lambda x: x['ASIN'] in b['similar'] and x['salesrank'] < b['salesrank']) \#pergunta se o x[] == b[]
    .take(1)
pp.pprint(b)

c = amazon \
        .filter(lambda x: "B000007R0T" == x['ASIN']) \
        .map(MediaDeDias) \
        .take(1)[0]
pp.pprint(c)

d = amazon \
    .map(lambda p: (p['group'], {'salesrank': p['salesrank'], 'ASIN': p['ASIN']})) \
    .groupByKey() \
    .map(lambda p: (p[0], sorted(list(p[1]), key=lambda x: x['salesrank'])[0:10]) )\
    .collect()
pp.pprint(d)

e = amazon \
    .map(lambda p: (p['ASIN'], MediaComentario(p['reviews']))) \
    .sortBy(lambda p: p[1], ascending=False) \
    .take(10)
pp.pprint(e)

f = amazon \
    .flatMap(categoriaComAv) \
    .reduceByKey(lambda x, y: (x[0]+y[0] , x[1]+y[1])) \
    .map(lambda x: (x[0], x[1][0]/(x[1][1] if x[1][1] != 0 else 1) )  ) \
    .sortBy(lambda x: x[1], ascending=False) \
    .take(5)
pp.pprint(f)
'''

g = amazon \
    .flatMap(ClienteComentando) \
    .reduceByKey(lambda x, y: (x[0]==y[0] , x[1]+y[1])) \
    .map(lambda x: (x[0], x[1][0]/(x[1][1] if x[1][1] != 0 else 1) )  ) \
    .sortBy(lambda x: x[1], ascending=False) \
    .take(5)
pp.pprint(g)

'''
#ClienteComentando(p['reviews']))) \
g = amazon \
    .map(lambda p: (p['group'], {'reviews': p['reviews'], 'repet': ClienteComentando(p['reviews'])})) \
    .groupByKey() \
    .map(lambda p: (p[0], sorted(list(p[1][1]), key=lambda x: x['repet'])[0:10]) )\
    .collect()
    # .map(lambda p: (p['group'], {'client': p['reviews'][1]}) \
    # .groupByKey() \
    # .map(lambda p: (p[0], sorted(list(p[1]), key=lambda x: x['client'])[0:10]) )\
    # .collect()
    # .sortBy(lambda p: p[1], ascending=False) \
    # .take(10)
pp.pprint(g)
'''

# O dashboard a ser implementado deve dar suporte a pelo menos as seguintes consultas, as quais devem ser implementadas todas em linguagem Python com Spark.

# (a) Dado produto, listar os 5 comentários mais úteis e com maior avaliação e os 5 comentários mais úteis e com menor avaliação
# (b) Dado um produto, listar os produtos similares com maiores vendas do que ele
# (c) Dado um produto, mostrar a evolução diária das médias de avaliação ao longo do intervalo de tempo coberto no arquivo de entrada
# (d) Listar os 10 produtos lideres de venda em cada grupo de produtos
# (e) Listar os 10 produtos com a maior média de avaliações úteis positivas por produto
# (f) Listar a 5 categorias de produto com a maior média de avaliações úteis positivas por produto
# (g) Listar os 10 clientes que mais fizeram comentários por grupo de produto


# messages.filter(lambda s: “foo” in s).count()

# Id:   77
# ASIN: 0312982178
#   title: Water Touching Stone
#   group: Book
#   salesrank: 27012
#   similar: 5  0312978340  0312330898  0312277598  1569473412  1569472424
#   categories: 2
#    |Books[283155]|Subjects[1000]|Mystery & Thrillers[18]|Mystery[10457]|General[10466]
#    |Books[283155]|Subjects[1000]|Mystery & Thrillers[18]|General[605116]
#   reviews: total: 11  downloaded: 11  avg rating: 4.5
#     2001-5-12  cutomer:    AFVQZQ8PW0L  rating: 5  votes:  13  helpful:  12
#     2001-6-14  cutomer: A3MUJDPC7D1NUQ  rating: 5  votes:  11  helpful:  11