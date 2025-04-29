# Dijkstra Fast Implementation

## Prerequisites

- **Ubuntu 22.04 LTS**
- **OpenJDK 11.0.26**
- **Apache Spark 3.4.1** (downloaded and extracted to `~/spark`)
- **Python 3.8**
- **PySpark** (bundled with Spark)

## Setup Instructions

1. **Install Java & Python**
   ```bash
   sudo apt update
   sudo apt install -y openjdk-11-jdk python3 python3-pip
   ```

2. **Download & Install Spark**
   ```bash
   cd ~
   wget https://archive.apache.org/dist/spark/spark-3.4.1/spark-3.4.1-bin-hadoop3.tgz
   tar -xzf spark-3.4.1-bin-hadoop3.tgz
   mv spark-3.4.1-bin-hadoop3 spark
   ```

3. **Set Environment Variables**
   Append the following to `~/.bashrc`:
   ```bash
   export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
   export SPARK_HOME=~/spark
   export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
   ```
   Then reload:
   ```bash
   source ~/.bashrc
   ```

4. **Prepare Working Directory**
   ```bash
   mkdir -p ~/spark-dijkstra
   cp final_dijkstra.py weighted_graph.txt ~/spark-dijkstra/
   ```

## Execution Instructions

Run the optimized Dijkstra implementation with Spark handling file I/O and a local heap-based computation:
```bash
cd ~/spark-dijkstra
spark-submit --master local[*] final_dijkstra.py weighted_graph.txt 0 output.txt
```

- `--master local[*]`: use all local CPU cores on the master VM
- `weighted_graph.txt`: input file (header line + `u v w` entries)
- `0`: ID of the source node
- `output.txt`: file where shortest-path distances will be written

## Output

The `output.txt` file will contain lines in the format:
```
Node 0: 0
Node 1: 14
Node 2: 6
...
```

## Notes & Tips

- Ensure `weighted_graph.txt` and `final_dijkstra.py` are in the same directory when running.
- For very large graphs, you can adjust Spark’s driver memory:
  ```bash
  spark-submit --master local[*] --driver-memory 4g \
      final_dijkstra.py weighted_graph.txt 0 output.txt
  ```
- This approach parallelizes only the file read; the core algorithm runs in-memory on the driver for minimal overhead.

