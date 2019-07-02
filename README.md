# Streamr Source and Sink Integrations to Apache Flink
[Apache Flink](https://flink.apache.org/) is an open source tool for stateful computations over data streams. Flink scales horizontally to any use case and has low latency, high troughput and does in-memory computing. Flink allows you to build event driven applications and to do stream and batch analysis over your real time streams. All of the attributes listed above and more make Flink an ideal tool to process data from Streamr. You can read more about Apache Flink in it's [documentation](https://ci.apache.org/projects/flink/flink-docs-release-1.8/).

## Setting up

If you haven't already install Apache Flink. Guide for the installation can be found [here](https://ci.apache.org/projects/flink/flink-docs-release-1.8/tutorials/local_setup.html).

### Creating a project

It's highly recommended to use Flinks quick start scripts to start up your projects as it requires you to do less configuration. (This repository is built and tested with Maven only but it's also possible to use [Gradle](https://ci.apache.org/projects/flink/flink-docs-release-1.8/dev/projectsetup/java_api_quickstart.html#gradle))

For Java:
```
mvn archetype:generate                               \
      -DarchetypeGroupId=org.apache.flink              \
      -DarchetypeArtifactId=flink-quickstart-java      \
      -DarchetypeVersion=1.8.0
```

For Scala:
```
mvn archetype:generate                               \
      -DarchetypeGroupId=org.apache.flink              \
      -DarchetypeArtifactId=flink-quickstart-scala     \
      -DarchetypeVersion=1.8.0
```
The rest of the set up process is the same for Java and Scala with Maven.

After you have started the project you need to add Streamr's client to your pom.xml file:

```	

 </dependencies>
    ...
    <dependency>
        <groupId>com.streamr</groupId>
        <artifactId>client</artifactId>
        <version>1.1.0</version>
    </dependency>
    ...
</dependencies>
```

You also need to add the Ethereum repository to the pom.xml file for Streamr's client to work:
```
<repositories>
    ...
    <repository>
        <id>ethereum</id>
        <url>https://dl.bintray.com/ethereum/maven</url>
    </repository>
    ...
</repositories>
```

Now the project should be configured correctly to run Streamr's client in Flink.

Now you can simply copy the [Java source code](./src/main/java/FlinkStreamrJava) or [Scala source code](./src/main/FlinkStreamrScala) in this repository and add your Streamr Api key and Stream Ids to the project's main class. You should use a different stream for subscribing and publishing. The Api key and and stream ids can be found in [Streamr's editor](www.streamr.com). You should create an account and the streams in the editor if you haven't done so already.

## Running

Simply run 
```
mvn compile
```
in the project and the deploy the project to your flink cluster with: 

```
flink run target/FlinkStreamr-1.0-SNAPSHOT.jar
```

More info incase you did not use Mac's homebrew to install Apache Flink, or you ran in to other issues in [Flink's Documentation](https://ci.apache.org/projects/flink/flink-docs-release-1.8/tutorials/local_setup.html#run-the-example)
