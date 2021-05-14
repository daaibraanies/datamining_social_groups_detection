import pyspark
import findspark
from pyspark.sql import SQLContext
from graphframes import *
import sys
import os.path
import time
from itertools import combinations
import sys
from itertools import chain

findspark.find()
spark_context = pyspark.SparkContext()
sql_context = SQLContext(spark_context)
spark_context.setLogLevel("ERROR")


def dump_file(filename, data, mode='w'):
    with open(filename, mode=mode) as f:
        f.write("{}".format(data))

def get_user_pairs(users,business):
    result = []

    for pair in combinations(users, 2):
        pair = sorted(pair)
        result.append(((pair[0], pair[1]), [business]))

    return result

def generate_graph(user_file,threshold=7):
    users_businesses = sql_context.read.csv(user_file,header=True).rdd.groupByKey()
    users_businesses.persist()
    users_reviews = dict(users_businesses.collect())
    #This code emits pairs for every distinct user with every other user that has different user_id and have 7 or more mutual business reviews
    #then distinct eliminates possible duplicates (is not really needed here)
    edges = users_businesses.flatMap(lambda x: [(x[0],other_user) for other_user in users_reviews.keys() if\
                                                x[0] != other_user \
                                                and len(set(users_reviews[x[0]]).intersection(users_reviews[other_user])) >= threshold])\

    # "If the user node has no edge, we will not include that node in the graph."
    return [(k,) for k,v in edges.groupByKey().collect()],edges.collect()


if __name__ == '__main__':
    start_time = time.time()
    max_iter_num = 5
    filter_threshold = 7
    user_business_file = '../ub_sample_data.csv'
    output_file = 'out'

    g_vertices, g_edges = generate_graph(user_business_file, threshold=filter_threshold)
    g_vertices = sql_context.createDataFrame(g_vertices, ["id"])
    g_edges = sql_context.createDataFrame(g_edges, ["src", "dst"])
    print("Graph has been generated!")

    communities = GraphFrame(g_vertices, g_edges).labelPropagation(maxIter=max_iter_num).rdd \
        .map(lambda x: (x[1], x[0]))\
        .groupByKey() \
        .map(lambda x: list(x[1])) \
        .collect()

print("Communities have been determined.")

communities = [sorted(l) for l in communities]
communities = sorted(communities, key=lambda x: (len(x), x[0]), reverse=False)

with open(output_file, 'w') as out:
    for line in communities:
        for i, val in enumerate(line):
            out.write("'{}'".format(val))
            if i < len(line) - 1:
                out.write(',')
        out.write("\n")

print("Duration: {:.2f}".format(time.time() - start_time))