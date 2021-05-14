import pyspark
import findspark
import sys
import time
import random
from itertools import combinations
from queue import *
from itertools import combinations,chain

findspark.find()
spark_context = pyspark.SparkContext()
spark_context.setLogLevel("ERROR")



def get_user_pairs(users,business):
    result = []
    for pair in combinations(users, 2):
        result.append(((pair[0], pair[1]), [business]))
    return result

def dump_file(filename, data, mode='w'):
    with open(filename, mode=mode) as f:
        f.write("{}".format(data))

def get_users_and_users_groups(user_file,threshold=7):
    rdd = spark_context.textFile(user_file).map(lambda x: x.split(',')) \
                .filter(lambda x: x[0] != "user_id").groupByKey()
    rdd.persist()

    users_reviews = dict(rdd.collect())
    #This code emits pairs for every distinct user with every other user that has different user_id and have 7 or more mutual business reviews
    #then distinct eliminates possible duplicates (is not really needed here)
    edges = rdd.flatMap(lambda x: [(x[0],other_user) for other_user in users_reviews.keys() if\
                x[0] != other_user \
                and len(set(users_reviews[x[0]]).intersection(users_reviews[other_user])) >= threshold]) \
                .groupByKey()

    # "If the user node has no edge, we will not include that node in the graph."
    return [k for k,v in edges.collect()],edges.map(lambda x:(x[0],list(x[1]))).collect(),edges.map(lambda x:(x[0],list(x[1])))

def shortest_paths(root,network):
    levels = {}
    parents = {}
    weights = {}
    queue = Queue(maxsize=len(network.keys()))
    visited = []

    levels[root] = 0
    weights[root] = 1
    queue.put(root)
    visited.append(root)

    while not queue.empty():
        current_node = queue.get()
        children = network[current_node]

        for child in children:
            if child not in visited:
                visited.append(child)
                weights[child] = weights[current_node]
                parents[child] = [current_node]
                queue.put(child)
                levels[child] = levels[current_node] + 1
            else:
                if child != root:
                    parents[child].append(current_node)
                    if levels[current_node] == levels[child] - 1:
                        weights[child] += weights[current_node]

    return levels,parents,weights,visited

def get_betweenness(root,network):
    number = 0
    nodes_arrangement = []
    reversed_nodes = []
    node_values = {}
    betweenness = {}
    levels,\
    parents,\
    weights,\
    visited = shortest_paths(root,network)

    for visited_node in visited:
        nodes_arrangement.append((visited_node, number))
        number += 1

    backward_node_arrangement = sorted(nodes_arrangement, key=(lambda x: x[1]), reverse=True)

    for node in backward_node_arrangement:
        node_values[node[0]] = 1
        reversed_nodes.append(node[0])

    for node in reversed_nodes:
        if node != root:
            cum_weight = 0
            for child_node in parents[node]:
                if levels[child_node] == levels[node] - 1:
                    cum_weight += weights[child_node]

            for child_node in parents[node]:
                if levels[child_node] == levels[node] - 1:
                    pair = (node,child_node)
                    if pair in betweenness.keys():
                        betweenness[pair] += (node_values[pair[0]] * weights[pair[1]]) / cum_weight
                    else:
                        betweenness[pair] = (node_values[pair[0]] * weights[pair[1]]) / cum_weight
                    node_values[pair[1]] += node_values[pair[0]] * weights[pair[1]] / cum_weight
    return [tuple((sorted(k),v)) for k,v in betweenness.items()]

def compute_sorted_betweennes(user_rdd,users_groups):
    betweenness = user_rdd.flatMap(lambda x:get_betweenness(x[0],users_groups))\
                  .map(lambda x:(tuple(sorted(x[0])),x[1])) \
                  .reduceByKey(lambda x, y: x + y) \
                  .map(lambda x: (x[0], x[1] / 2)) \
                  .sortBy(lambda x: x[1], ascending=False) \
                  .collect()
    return betweenness


def remove_edges_by_btw(betweenness,graph_edges):
    edges_with_highest_btw = [betweenness[0][0]]
    current_highest_btw = betweenness[0][1]

    for edge in betweenness:
        if edge[1] == current_highest_btw:
            edges_with_highest_btw.append(edge[0])
        else:
            break

    for edge in edges_with_highest_btw:
        first = edge[0]
        second = edge[1]

        try:
            graph_edges[first].remove(second)
        except KeyError:
            pass
        except ValueError:
            pass

        try:
            graph_edges[second].remove(first)
        except KeyError:
            pass
        except ValueError:
            pass
    return graph_edges

def key_in_set(key,data):
    key = tuple(sorted(key))
    if key not in data:
        key = (key[1],key[0])
        if key not in data:
            return False
    return True


def get_community_modularity(graph_edges,vertices,adjacent_matrix,m):
    current_communities = bfs_over_graph(graph_edges,vertices)
    modularuty = 0
    for community in current_communities:
        for pair in combinations(community,2):
            pair_key = pair
            Aij = 0
            if key_in_set(pair_key,adjacent_matrix):
                Aij = 1
            ki = len(graph_edges[pair_key[0]])
            kj = len(graph_edges[pair_key[1]])
            modularuty += (Aij - (ki*kj)/(2*m))
    return current_communities,modularuty/(2*m)

#detection of communities
def bfs_over_graph(graph_edges,vertices):
    communities = []
    frontier = []
    community_candidates = set()
    visited = set()
    vertex = vertices[random.randint(0,len(vertices)-1)]
    frontier.append(vertex)
    community_candidates.add(vertex)
    while len(visited) < len(vertices):
        while len(frontier) > 0:
            current_node = frontier.pop(0)
            visited.add(current_node)
            community_candidates.add(current_node)

            for child in graph_edges[current_node]:
                if child not in visited:
                    frontier.append(child)
                    community_candidates.add(child)
                    visited.add(child)
        communities.append(sorted(community_candidates))
        community_candidates = set()
        if len(vertices) > len(visited):
            frontier.append(set(vertices).difference(visited).pop())
    return communities


def get_adjacent_matrix(graph_edges):
    matrix = set()
    for node_a,adjacent_nodes in graph_edges.items():
        for node_b in adjacent_nodes:
            pair_key = tuple(sorted((node_a, node_b)))
            matrix.add(pair_key)
    return matrix

def get_m(graph_edges):
    number_of_edges = 0
    visited = set()

    for node_a, adjacent_nodes in graph_edges.items():
        for node_b in adjacent_nodes:
            pair_key = tuple(sorted((node_a, node_b)))
            if pair_key not in visited:
                number_of_edges+=1
                visited.add(pair_key)
    return number_of_edges

def betweenness_second_phase(graph_edges,vertices):
    new_betweenness = []

    for vertex in vertices:
        new_betweenness.append(get_betweenness(vertex,graph_edges))

    new_betweenness = list(chain(*new_betweenness))
    btw_dict = {tuple(x):0 for x, _ in new_betweenness}
    for key, val in new_betweenness: btw_dict[tuple(key)] += val
    for key in btw_dict.keys(): btw_dict[key]/=2
    btw_dict = sorted(btw_dict.items(),key=lambda x: (-x[1],x[0][0]))
    return btw_dict



if __name__ == '__main__':
    start_time = time.time()
    user_business_file = '../ub_sample_data.csv'
    output_file = 'out'
    uniq_users,users_groups,user_rdd = get_users_and_users_groups(user_business_file,7)
    users_groups = dict(users_groups)
    m = get_m(users_groups)
    adjacent_matrix = get_adjacent_matrix(users_groups)
    betweenness = betweenness_second_phase(users_groups,uniq_users)

    with open(output_file,'w') as out:
        for pair in betweenness:
            out.write("('{}','{}'), {}\n".format(pair[0][0],pair[0][1],pair[1]))

    users_groups = remove_edges_by_btw(betweenness,graph_edges=users_groups)
    clusters,current_modularity = get_community_modularity(users_groups,uniq_users,adjacent_matrix,m)
    max_modularity = -float('inf')

    while current_modularity > max_modularity:
        max_modularity = current_modularity
        best_communities = clusters
        betweenness = betweenness_second_phase(users_groups,uniq_users)
        users_groups = remove_edges_by_btw(betweenness,graph_edges=users_groups)
        clusters, current_modularity = get_community_modularity(users_groups, uniq_users,adjacent_matrix,m)

    best_communities = [sorted(l) for l in best_communities]
    best_communities = sorted(best_communities, key=lambda x: (len(x), x[0]), reverse=False)

    with open(output_file, 'w') as out:
        for line in best_communities:
            for i, val in enumerate(line):
                out.write("'{}'".format(val))
                if i < len(line) - 1:
                    out.write(',')
            out.write("\n")


print("Duration: {:.2f}".format(time.time() - start_time))