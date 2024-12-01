# Scala Spark Submit Application

This README provides instructions to run a Scala Spark application locally using `spark-submit`.

## Prerequisites

Before you begin, ensure you have the following installed on your machine:

- Java (JDK 8 or later)
- Apache Spark 3.5.3
- Scala 2.3.10
- sbt (Scala Build Tool) 1.10.3
- MySQL Connector/J 8.0.33

### MySQL Connector/J Setup

1. **Download the MySQL Connector/J**: You can download it from
   the [official MySQL website](https://dev.mysql.com/downloads/connector/j/).
2. **Add to Your Project**: Ensure that the MySQL Connector/J JAR file is added to your project's dependencies.

#### Adding MySQL Connector/J to sbt

Add the following line to your `build.sbt` file to include the MySQL Connector/J dependency:

```scala 
libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.33"
```

## Setup

1. **Clone the Repository**

    ```bash
    git clone https://github.com/simonno/Baseball.git
    cd Baseball
    ```

2. **Build the Application**

   Use `sbt` to build your Scala application. This will create a JAR file that you can submit to Spark.

    ```bash
    sbt clean package
    ```

   The JAR file will be generated in the `target/scala-2.13/` directory.

## Running the Application (linux bash)

### AverageSalariesPerYear

To run the `AverageSalariesPerYear` class, use the following command:

```bash 
$SPARK_HOME/bin/spark-submit \
  --class AverageSalariesPerYear \
  --master local[*] \
  target/scala-2.13/baseball_2.13-0.1.0.jar
```

### Pitching

To run the `HallOfFameAllStarPitchers` class, use the following command:

```bash
$SPARK_HOME/bin/spark-submit \
  --class HallOfFameAllStarPitchers \
  --master local[*] \
  target/scala-2.13/baseball_2.13-0.1.0.jar

```

To run the `Pitching` class, use the following command:

```bash
$SPARK_HOME/bin/spark-submit \
  --class Pitching \
  --master local[*] \
  target/scala-2.13/baseball_2.13-0.1.0.jar

```

To run the `Rankings` class, use the following command:

```bash
$SPARK_HOME/bin/spark-submit \
  --class Rankings \
  --master local[*] \
  target/scala-2.13/baseball_2.13-0.1.0.jar

```