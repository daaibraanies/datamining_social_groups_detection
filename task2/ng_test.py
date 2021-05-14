from queue import *



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
                    if pair not in betweenness.keys():
                        betweenness[pair] = (node_values[pair[0]] * weights[pair[1]]) / cum_weight
                    else:
                        betweenness[pair] += (node_values[pair[0]] * weights[pair[1]]) / cum_weight
                    node_values[pair[1]] += node_values[pair[0]] * weights[pair[1]] / cum_weight
    return [(k,v) for k,v in betweenness.items()]


sample_network = {
    'E': ['D','F'],
    'D': ['E', 'G','B','F'],
    'F': ['E', 'G','D'],
    'G': ['D', 'F'],
    'B': ['D', 'C','A'],
    'A': ['B','C'],
    'C': ['B','A']
}
betw = {}

for root in sample_network.keys():
    if list(betw.keys()) == []:
        betw = dict(get_betweenness(root,sample_network))
        for key in betw.keys():
            sk = tuple(sorted(key))
            val =  betw[key]
            del betw[key]
            betw[sk] = val
    else:
        new_betw = dict(get_betweenness(root,sample_network))
        stop = 1
        for key in new_betw.keys():
            key = tuple(sorted(key))
            try:
                betw[key] += new_betw[key]
            except KeyError:
                betw[key] = new_betw[key]

for key in betw.keys():
    betw[key]/=2


stop = 1