// Execute:
// spark-submit --class DijkstraGraphX \
//     --master spark://<MASTER_PUBLIC_IP>:7077 \
//     target/scala-2.12/dijkstragraphx_2.12-0.1.jar \
//     weighted_graph.txt 0

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.rdd.RDD

object DijkstraGraphX {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: DijkstraGraphX <input> <sourceId>")
      System.exit(1)
    }
    val input = args(0)
    val sourceId = args(1).toLong

    val conf = new SparkConf().setAppName("GraphX Dijkstra")
    val sc = new SparkContext(conf)

    // Load edges: skip header if present
    val lines = sc.textFile(input)
    val header = lines.first()
    val data = lines.filter(_ != header)

    val edges: RDD[Edge[Double]] = data.map { line =>
      val parts = line.split("\\s+")
      Edge(parts(0).toLong, parts(1).toLong, parts(2).toDouble)
    }

    // Build graph with default distance = Infinity
    val graph = Graph.fromEdges(edges, Double.PositiveInfinity)

    // Run the built-in ShortestPaths algorithm (unweighted shortest paths)
    val result = ShortestPaths.run(graph, Seq(sourceId))

    // Print distances, converting to Double
    result.vertices.collect.foreach { case (vid, spMap) =>
      // spMap: Map[VertexId, Int], so convert to Double
      val dist: Double = spMap.get(sourceId).map(_.toDouble).getOrElse(Double.PositiveInfinity)
      val distStr = if (dist.isInfinite) "INF" else dist.toString
      println(s"Node $vid: $distStr")
    }

    sc.stop()
  }
}
