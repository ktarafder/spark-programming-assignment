import sys
import heapq
import collections
from pyspark import SparkContext


# Usage:
#   spark-submit --master local[*] final_dijkstra.py weighted_graph.txt 0 output.txt

def run_dijkstra(input_path, source_node, output_path):
    sc = SparkContext(master="local[*]", appName="DijkstraFast")
    lines = (sc
             .textFile(input_path)
             .filter(lambda L: L and len(L.split()) == 3))
    edges = lines.map(lambda L: tuple(map(int, L.split()))).collect()
    sc.stop()

    graph = collections.defaultdict(list)
    nodes = set()
    for u, v, w in edges:
        graph[u].append((v, w))
        nodes.add(u)
        nodes.add(v)

    dist = {node: float('inf') for node in nodes}
    dist[source_node] = 0
    heap = [(0, source_node)]
    while heap:
        d, u = heapq.heappop(heap)
        if d > dist[u]:
            continue
        for v, w in graph[u]:
            nd = d + w
            if nd < dist[v]:
                dist[v] = nd
                heapq.heappush(heap, (nd, v))

    with open(output_path, 'w') as f:
        for node in sorted(nodes):
            d_val = dist[node]
            f.write(f"Node {node}: {d_val if d_val < float('inf') else 'INF'}\n")

if __name__ == '__main__':
    if len(sys.argv) != 4:
        print("Usage: spark-submit final_dijkstra.py <input> <source_node> <output_file>")
        sys.exit(1)
    _, inp, src, out = sys.argv
    run_dijkstra(inp, int(src), out)
