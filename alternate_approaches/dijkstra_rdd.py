#!/usr/bin/env python3
import sys
from pyspark import SparkConf, SparkContext

# Parse a line of the edge list: "u v w"
def parse_edge(line):
    u, v, w = line.split()
    return int(u), (int(v), float(w))

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: dijkstra.py <input_path> <source_node>")
        sys.exit(1)

    input_path = sys.argv[1]
    source_node = int(sys.argv[2])

    # Initialize Spark context
    conf = SparkConf().setAppName("Dijkstra")
    sc = SparkContext(conf=conf)

    # Load the graph as (u, [(v, w), ...]) and cache
    edges = (
        sc.textFile(input_path)
        .filter(lambda L: not L.startswith("#"))
        .map(parse_edge)
        .groupByKey()
        .mapValues(list)
        .cache()
    )

    # Initialize distances RDD and frontier with the source node
    dist_rdd = sc.parallelize([(source_node, 0.0)])
    frontier = dist_rdd

    # Delta-iteration loop: relax only from the frontier each iteration
    while True:
        # Generate candidate distances by relaxing edges from the frontier
        candidates = (
            frontier.join(edges)
            .flatMap(lambda x: [(nbr, x[1][0] + w) for nbr, w in x[1][1]])
        )

        # Merge old distances with candidates, keeping minimum
        new_dists = (
            dist_rdd.union(candidates)
            .reduceByKey(lambda a, b: a if a < b else b)
        )

        # Build the new frontier: nodes whose distance improved
        frontier = (
            new_dists.join(dist_rdd)
            .filter(lambda x: x[1][0] < x[1][1])
            .map(lambda x: (x[0], x[1][0]))
        )

        # Terminate if no improvements
        if frontier.isEmpty():
            dist_rdd = new_dists
            break

        dist_rdd = new_dists

    # Collect and print final shortest distances
    results = dist_rdd.collect()
    for node, d in sorted(results, key=lambda x: x[0]):
        d_str = str(d) if d < float('inf') else "INF"
        print(f"Node {node}: {d_str}")

    sc.stop()
