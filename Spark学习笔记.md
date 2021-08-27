# Spark

## Spark的基本概念

**spark是一直基于内存的快速，通用，可扩展的大数据分析计算引擎**

saprk框架计算比MR快的原因是：中间不落盘，注意Spark的shuffle也是需要落盘的

RDD：弹性分布式数据集

算子：就是将问题的初始化状态通过操作进行转换为中间状态，然后通过操作转换为完成状态，中间的转换操作的英文：Operator，翻    		    译为中文就是操作，算子。

## Spark的内置模块

![image-20210814212352635](./pic/image-20210814212352635.png)

## Spark的特点

①快：与hadoop的MapReducer相比，Spark基于内存的运算要快100倍以上，基于硬盘的运算也要快10倍以上，Spark实现了高效的DAG执行引擎，可以通过基于内存来高效处理数据流。计算的中间结果是存在于内存中的。

②易用：Spark支持Java，Python和Scala的API

③通用：Spark提供了统一的解决方案，Saprk可以用于交互式查询（Saprk SQL）实时流处理（Saprk Streaming），机器学习（Saprk MLlib）和图计算（GraphX）

④兼容性：Spark可以非常方便地与其他的开源产品进行融合

## Spark运行模式

部署Spark集群大体上分为两种模式：单机模式和集群模式

单机模式：Local模式

集群模式：Standalone模式：Standalone模式是Spark自带的资源调度引擎，构建一个由Master+Slave构成的Spark集群，Spark运行在													集群中。

​					Yarn模式：Spark客户端直接连接Yarn，不需要额外构建Spark集群。

​					Mesos模式：Spark客户端直接连接Mesos；不需要额外构建Spark集群。

## RDD

RDD:叫弹性**分布式******数据集****，是Spark中最基本的**数据抽象**，代码中是一个抽象类，它代表一个**弹性**，**不可变**，**可分区**，里面的元素**并行计算**的集合。

### RDD的特性

①一组分区

②一个计算每个分区的函数

③RDD之间的依赖关系

④一个partition，即RDD的分片函数，控制分区数据的流向（键值对）

⑤一个列表，存储存取每个partition的优先位置，移动数据不如移动计算，除非资源不够

### RDD算子

RDD的算子分为：转换算子（transformation）和行动算子（action）,在Saprk中只有遇到action，才会执行RDD计算（即延迟计算）

### RDD的创建

在Spark中创建RDD的创建方式分为三种：从集合中创建RDD，从外部文件中创建RDD，从其他方式创建RDD。

①根据集合创建RDD

```scala
val list: List[Int] = List(1, 2, 3, 4)
// 根据集合创建RDD 方式一
val rdd: RDD[Int] = sc.parallelize(list)
// 根据集合创建RDD 方式二
val rdd2: RDD[Int] = sc.makeRDD(list)
```

②从外部存储系统的数据集中创建

```scala
// 从本地文件读取数据，来创建RDD
val rdd: RDD[String] = sc.textFile("input")
// 从hdfs上读取数据
val rdd2: RDD[String] = sc.textFile("hdfs://hadoop104:8020/input")
```

③通过RDD的转换算子转换

## 第一个Spark代码

### pom.xml文件

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>org.example</groupId>
    <artifactId>Spark</artifactId>
    <packaging>pom</packaging>
    <version>1.0-SNAPSHOT</version>

    <properties>
        <maven.compiler.source>8</maven.compiler.source>
        <maven.compiler.target>8</maven.compiler.target>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-core_2.11</artifactId>
            <version>2.1.1</version>
        </dependency>
        <dependency>
            <groupId>mysql</groupId>
            <artifactId>mysql-connector-java</artifactId>
            <version>5.1.47</version>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql_2.11</artifactId>
            <version>2.1.1</version>
        </dependency>

        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-streaming_2.11</artifactId>
            <version>2.1.1</version>
        </dependency>

        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-streaming-kafka-0-8_2.11</artifactId>
            <version>2.1.1</version>
        </dependency>


    </dependencies>

    <build>
        <finalName>WordCount</finalName>
        <plugins>
            <plugin>
                <groupId>net.alchim31.maven</groupId>
                <artifactId>scala-maven-plugin</artifactId>
                <version>3.4.6</version>
                <executions>
                    <execution>
                        <goals>
                            <goal>compile</goal>
                            <goal>testCompile</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>

            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-assembly-plugin</artifactId>
                <version>3.0.0</version>
                <configuration>
                    <archive>
                        <manifest>
                            <mainClass>com.yue.spark.****</mainClass>
                        </manifest>
                    </archive>
                    <descriptorRefs>
                        <descriptorRef>jar-with-dependencies</descriptorRef>
                    </descriptorRefs>
                </configuration>
                <executions>
                    <execution>
                        <id>make-assembly</id>
                        <phase>package</phase>
                        <goals>
                            <goal>single</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>

        </plugins>
    </build>

</project>
```

### WordCount案例

```scala
package com.yue.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
 * TODO
 *  第一个wordcount案例
 *
 * @author 岳昌宏
 * @date 2021/7/27 16:49
 */
object WordCount {
    def main(args: Array[String]):Unit = {
        // 创建SparkConf配置文件
        val conf:SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

        // 创建SparkContext对象
        val sc:SparkContext = new SparkContext(conf)


        sc.textFile(args(0))
            .flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_).saveAsTextFile(args(1))

        // 关闭连接
        sc.stop()
    }

}
```

## 创建RDD的默认分区规则

### 从集合中创建RDD

从集合中创建RDD取于分配给应用的CPU核数

`val conf: SparkConf = new SparkConf().setAppName("Spark03_Partition_default").setMaster("local[*]")`

### 读取外部文件创建RDD

读取外部文件创建RDD，取决于分配给应用的CPU核数和2取最小值。

在textFile方法中的第二个参数minPartitions表示最小分区数，是最小，不是实际的分区个数，在实际计算分区个数的时候，会根据文件的总大小个最小分区数进行相除运算，如果余数为0，那么最小分区数就是最终实际的分区个数；如果余数不为0，实际分区个数要经过计算才能得到。

## 转换算子

### Value类型

#### map

**rdd.map(f:Int=>U)**

参数f是一个函数，它可以接受一个参数，当某个rdd执行map方法时，会遍历该rdd中的每一个数据项，并依次应用f函数，从而参数一个新的rdd，即这个新的rdd中的每一个元素都是原来rdd中每一个元素依次应用f函数而得到的（分区不变，数据类型可能改变）

```scala
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * TODO
 *
 * @author 岳昌宏
 * @date 2021/8/16 19:46
 */
object Test {
    def main(args : Array[String]) : Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Test").setMaster("local[*]")
        val sc = new SparkContext(conf)

        val listRDD: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5))

        val resRDD: RDD[Int] = listRDD.map(_ + 1) // 使集合中的每一个元素都加1

        resRDD.collect().foreach(println)

        sc.stop()
    }
}
```

#### mapPartitions

**rdd.mapPartitions(f:Iterator[U]=>Iterator[U])**

以分区为单位执行map，Map是一次处理一个元素，mapPartitions()是一次处理一个分区的数据

```scala
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * TODO
 *
 * @author 岳昌宏
 * @date 2021/8/16 19:46
 */
object Test {
    def main(args : Array[String]) : Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Test").setMaster("local[*]")
        val sc = new SparkContext(conf)

        val listRDD: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5))

        val resRDD: RDD[Int] = listRDD.mapPartitions(_.map(_+1)) // 使集合中的每一个元素都加1，注意：这里的map是方法不是算子

        resRDD.collect().foreach(println)

        sc.stop()
    }
}
```

注意：Iterator用完即丢，并且会持续更新迭代器的状态

```scala
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * TODO
 *
 * @author 岳昌宏
 * @date 2021/8/16 19:46
 */
object Text {
    def main(args : Array[String]) : Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Text").setMaster("local[*]")
        val sc = new SparkContext(conf)

        val listRDD: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5))

        val resRDD: RDD[Int] = listRDD.mapPartitions(it => {
            it.foreach(_+1) // foreach()返回值是void，map的返回值是Iterator，而且Iterator数据用完即丢
            println(">>>" + it.toString()) // // 此时迭代器中无数据
            it
        })

        resRDD.collect().foreach(println) // 注意是没有输出结果的

        sc.stop()
    }
}
/*
empty iterator
empty iterator
empty iterator
empty iterator
empty iterator
empty iterator
empty iterator
empty iterator
*/
```

#### map和mapPartitions的区别

map():每次处理一条数据，mapPartitions()每次处理一个分区的数据，这个分区的数据处理完后，原RDD中分区的数据才能释放，可能导致OOM。开发经验：当内存空间较大的时建议使用mapPartitions以提高效率。

#### mapPartitionsWithIndex

**rdd.mapPartitionsWithIndex(f:(Int,Iterator[U])=>Iterator[U])**

类似mapPartitions比mapPartitions多了一个分区的编号，可以单独对某一个分区做特殊的操作。参数是一个元组，元组第一个元素代表分区编号，第二个元素是当前分区的迭代器。

```scala
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * TODO
 *
 * @author 岳昌宏
 * @date 2021/8/16 19:46
 */
object Test {
    def main(args : Array[String]) : Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Test").setMaster("local[*]")
        val sc = new SparkContext(conf)

        val listRDD: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5))

        val resRDD: RDD[Int] = listRDD.mapPartitionsWithIndex((index, iter) =>{
            println("分区编号：" + index + ">>> " + iter.mkString(" "))
            iter
        }) // 打印listRDD中的数据，并且附带分区

        resRDD.collect()

        sc.stop()
    }
}
/*
输出结果：

分区编号：7>>>5
分区编号：1>>>1
分区编号：5>>>
分区编号：0>>>
分区编号：2>>>
分区编号：3>>>2
分区编号：6>>>4
分区编号：4>>>3
*/
```

#### flatMap

**rdd.flatMap(f:U=>TraversableOnce[U])**

与map操作类似，将RDD中的每一个元素通过应用f函数依次转换为新的元素，并封装到RDD中，区别在flatMap()操作中，f函数的返回值是一个集合。并且会将每一个集合中的元素拆分出来放到新的RDD中。

```scala
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * TODO
 *
 * @author 岳昌宏
 * @date 2021/8/16 19:46
 */
object Test {
    def main(args : Array[String]) : Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Test").setMaster("local[*]")
        val sc = new SparkContext(conf)

        val listRDD: RDD[List[Int]] = sc.parallelize(List(List(1, 2, 3, 4, 5), List(6, 7, 8)))

        val resRDD: RDD[Int] = listRDD.flatMap(f => f)

        resRDD.collect().foreach(println)

        sc.stop()
    }
}
/*
输出结果
1
2
3
4
5
6
7
8
*/
```

#### glom

**rdd.glom()**

该算子将RDD中每一个分区中的数据变成一个数组，并放置在新的RDD中。数组中元素的类型与原分区中的类型一致。

```scala
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * TODO
 *
 * @author 岳昌宏
 * @date 2021/8/16 19:46
 */
object Test {
    def main(args : Array[String]) : Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Test").setMaster("local[*]")
        val sc = new SparkContext(conf)

        val listRDD: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5))

        val resRDD: RDD[Array[Int]] = listRDD.glom()

        resRDD.collect().foreach(f => println(f.mkString(" ")))

        sc.stop()
    }
}
/*
输出结果

1

2
3

4
5

*/
```

#### groupBy

**rdd.groupBy(f:U=>K)**

分组，按照传入函数的返回值进行分组，将相同的key对应的值放到一个迭代器中，groupBy会存在shuffle的过程

```scala
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * TODO
 *
 * @author 岳昌宏
 * @date 2021/8/16 19:46
 */
object Test {
    def main(args : Array[String]) : Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Test").setMaster("local[*]")
        val sc = new SparkContext(conf)

        val listRDD: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5))

        val res: RDD[(Boolean, Iterable[Int])] = listRDD.groupBy(_ >= 2) // 将大于等于2的放到一个分区中

        res.collect().foreach(println)

        sc.stop()
    }
}
/*
输出结果
(false,CompactBuffer(1))
(true,CompactBuffer(2, 3, 4, 5))
*/
```

#### filter

**rdd.filter(f:U=>Boolean)**

filter算子接收一个返回值为布尔类型的函数作为参数，当某个RDD调用filter方法时，会对该RDD中每一个元素应用f函数，如果返回值类型为true，则该元素会被添加到新的RDD中

```scala
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * TODO
 *
 * @author 岳昌宏
 * @date 2021/8/16 19:46
 */
object Test {
    def main(args : Array[String]) : Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Test").setMaster("local[*]")
        val sc = new SparkContext(conf)

        val listRDD: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5))

        val res: RDD[Int] = listRDD.filter(_ > 3) // 取大于3的元素

        res.collect().foreach(println)

        sc.stop()
    }
}
/*
输出结果
4
5
*/
```

#### sample

**rdd.sample(withReplacement:Boolean,fraction:Double,seed:Long=Utils.random.nextLong)**

sample采样算子，withReplacement表示抽出的算子是否放回，true为有放回的抽样，false为不放回的抽样；fraction：当withReplacement = false时，选择每一个元素的概率取值在[0, 1]之间，当withReplacement = true时，选择每一个元素的期望次数，取值必须大于等于0；seed表示指定随机数生成器的种子，抽取算法初始值，一般不需要指定。

```scala
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * TODO
 *
 * @author 岳昌宏
 * @date 2021/8/16 19:46
 */
object Test {
    def main(args : Array[String]) : Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Test").setMaster("local[*]")
        val sc = new SparkContext(conf)

        val listRDD: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5))

        val res: RDD[Int] = listRDD.sample(false, 0.5) // 不放回的抽取，抽取每一个元素的概率是0.5

        res.collect().foreach(println)

        sc.stop()
    }
}
```

#### distinct

**rdd.distinct()**

distinct()去重，并将去重后的元素放回到新的RDD中，默认情况下，distinct会生成与原RDD分区个数相同的分区数

```scala
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * TODO
 *
 * @author 岳昌宏
 * @date 2021/8/16 19:46
 */
object Test {
    def main(args : Array[String]) : Unit = {
        val conf:SparkConf = new SparkConf().setMaster("local[*]").setAppName("Test")
        val sc:SparkContext = new SparkContext(conf)

        val sourceRDD:RDD[Int] = sc.parallelize(List(1, 1, 2, 2, 3, 3))

        val resRDD:RDD[Int] = sourceRDD.distinct()

        resRDD.collect().foreach(println)

        sc.stop()
    }
}
/*
输出结果
1
2
3
*/
```

#### coalesce和repartition

**rdd.coalesce(numPartitions:Int, shuffle:Boolean = false)**

**rdd.repartition(numpartitions:Int)**

coalesce()和repartition()都可以修改当前的分区数，coalesce()默认第二个参数为false，表示不进行shuffle，如果第二个参数不设置，coalesce是无法扩大分区的个数，只能缩减分区的个数。

```scala
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * TODO
 *
 * @author 岳昌宏
 * @date 2021/8/16 19:46
 */
object Test {
    def main(args : Array[String]) : Unit = {
        val conf:SparkConf = new SparkConf().setMaster("local[*]").setAppName("Test")
        val sc:SparkContext = new SparkContext(conf)

        val sourceRDD:RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5), 3) // 定义分区数为3

        val coalesceRDD:RDD[Int] = sourceRDD.coalesce(4,true) // 扩大分区
//        val coalesceRDD:RDD[Int] = sourceRDD.coalesce(2) // 缩小分区
//        val coalesceRDD:RDD[Int] = sourceRDD.repartition(4) // 扩大分区


        val resRDD:RDD[Int] = coalesceRDD.mapPartitionsWithIndex((num, iter) =>{
            println(num + "---->" + iter.mkString(" "))
            iter
        })

        resRDD.collect()

        sc.stop()
    }
}
```

#### sortBy

**rdd.sortBy(f:U=>K, ascending:Boolean = true)**

sortBy该操作用于排序数据，在排序之前，可以将数据通过f函数进行处理，之后按照f函数处理的结果进行排序，默认为正序排列。排列后产生的RDD的分区个与原分区数一致，底层有shuffle操作

```scala
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * TODO
 *
 * @author 岳昌宏
 * @date 2021/8/16 19:46
 */
object Test {
    def main(args : Array[String]) : Unit = {
        val conf:SparkConf = new SparkConf().setMaster("local[*]").setAppName("Test")
        val sc:SparkContext = new SparkContext(conf)

        val sourceRDD:RDD[String] = sc.parallelize(List("1", "3", "5", "2", "4"))

        val resRDD: RDD[String] = sourceRDD.sortBy(f => f.toInt, false)

        resRDD.collect().foreach(println)


        sc.stop()
    }
}
/*
输出结果
1
2
3
4
5
*/
```

#### pipe

**rdd.pipe(command:String)**

pipe()管道，针对每个分区，都调用一次的shell脚本，返回输出的RDD

### 双Value类型

#### union

**rdd.union(other:RDD[U])**

该算子用来求两个rdd的并集，该并集和数学上的并集并不相同，该并集并不去重

```scala
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * TODO
 *
 * @author 岳昌宏
 * @date 2021/8/16 19:46
 */
object Test {
    def main(args:Array[String]) : Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Test").setMaster("local[*]")
        val sc:SparkContext = new SparkContext(conf)

        val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5))
        val rdd2: RDD[Int] = sc.parallelize(List(2, 3, 4, 5, 6))

        val rdd3: RDD[Int] = rdd1.union(rdd2)

        rdd3.collect().foreach(println)

        sc.stop()
    }
}
/*
输出结果
1
2
3
4
5
2
3
4
5
6
*/
```

#### intersection

**rdd.intersection(other:RDD[U])**

该算子用来求两个RDD之间交集的部分

```scala
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * TODO
 *
 * @author 岳昌宏
 * @date 2021/8/16 19:46
 */
object Test {
    def main(args:Array[String]) : Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Test").setMaster("local[*]")
        val sc:SparkContext = new SparkContext(conf)

        val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5))
        val rdd2: RDD[Int] = sc.parallelize(List(2, 3, 4, 5, 6))

        val rdd3: RDD[Int] = rdd1.intersection(rdd2)

        rdd3.collect().foreach(println)

        sc.stop()
    }
}
/*
输出结果
2
3
4
5
*/
```

#### subtract

**rdd.subtract(other:RDD[U])**

该算子用来求两个RDD之间的差集

```scala
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * TODO
 *
 * @author 岳昌宏
 * @date 2021/8/16 19:46
 */
object Test {
    def main(args:Array[String]) : Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Test").setMaster("local[*]")
        val sc:SparkContext = new SparkContext(conf)

        val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5))
        val rdd2: RDD[Int] = sc.parallelize(List(2, 3, 4, 5, 6))

        val rdd3: RDD[Int] = rdd1.subtract(rdd2)

        rdd3.collect().foreach(println)

        sc.stop()
    }
}
/*
输出结果
1
*/
```

#### zip

**rdd.zip(other:RDD[U])**

对两个RDD做拉链操作，如果两个rdd元素个数不一致，就会报错，分区数不相同也会报错，要求：分区数必须一致，分区中的个数也要相同。

```scala
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * TODO
 *
 * @author 岳昌宏
 * @date 2021/8/16 19:46
 */
object Test {
    def main(args:Array[String]) : Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Test").setMaster("local[*]")
        val sc:SparkContext = new SparkContext(conf)

        val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5), 2)
        val rdd2: RDD[Int] = sc.parallelize(List(2, 3, 4, 5, 6), 2)

        val rdd3: RDD[(Int, Int)] = rdd1.zip(rdd2)

        rdd3.collect().foreach(println)

        sc.stop()
    }
}
/*
输出结果
(1,2)
(2,3)
(3,4)
(4,5)
(5,6)
*/
```

### key-Value类型

#### partitionBy

**rdd.partitionBy(partitioner:Partitioner)**

将RDD[K,V]中的K按照指定的Partitioner重新分区，如果原有的partitionRDD和现有的partitionRDD是一致的话，就不进行分区，否则会进行shuffle过程，RDD本身是没有partitionBy的，通过隐式转换动态给kv类型的RDD扩展的功能。

```scala
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}


/**
 * TODO
 *
 * @author 岳昌宏
 * @date 2021/8/16 19:46
 */
object Test {
    def main(args:Array[String]) :Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Test").setMaster("local[*]")
        val sc = new SparkContext(conf)

        val sourceRDD: RDD[(Int, String)] = sc.parallelize(List((1, "张三"), (2, "李四"), (3, "王二"), (4, "麻子")), 2)

        sourceRDD.mapPartitionsWithIndex((index, iter) => {
            println(index + "------>" + iter.mkString(" "))
            iter
        }).collect()

        println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>")

        val partitionByRDD: RDD[(Int, String)] = sourceRDD.partitionBy(new HashPartitioner(4))

        partitionByRDD.mapPartitionsWithIndex((index, iter) => {
            println(index + "------>" + iter.mkString(" "))
            iter
        }).collect()


        sc.stop()
    }
}
/*
输出结果
1------>(3,王二) (4,麻子)
0------>(1,张三) (2,李四)
>>>>>>>>>>>>>>>>>>>>>>>>>>>>
2------>(2,李四)
0------>(4,麻子)
1------>(1,张三)
3------>(3,王二)
*/
```

#### 自定义分区器

```scala
class MyPartition(partition: Int) extends Partitioner{
    override def numPartitions: Int = ???
    
    override def getPartition(key: Any): Int = ???
}
```
```scala
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}


/**
 * TODO
 *
 * @author 岳昌宏
 * @date 2021/8/16 19:46
 */
object Test {
    def main(args:Array[String]) :Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Test").setMaster("local[*]")
        val sc = new SparkContext(conf)

        val sourceRDD: RDD[(Int, String)] = sc.parallelize(List((1, "张三"), (2, "李四"), (3, "王二"), (4, "麻子")), 2)

        sourceRDD.mapPartitionsWithIndex((index, iter) => {
            println(index + "------>" + iter.mkString(" "))
            iter
        }).collect()

        println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>")

        val partitionByRDD: RDD[(Int, String)] = sourceRDD.partitionBy(new MyPartition(4))

        partitionByRDD.mapPartitionsWithIndex((index, iter) => {
            println(index + "------>" + iter.mkString(" "))
            iter
        }).collect()


        sc.stop()
    }


}

class MyPartition(partition: Int) extends Partitioner{
    override def numPartitions: Int = partition // 分区的个数

    override def getPartition(key: Any): Int = {
        val i: Int = key.asInstanceOf[Int]
        i match {
            case i if i == 1 => 0
            case i if i == 2 => 1
            case i if i == 3 => 2
            case i if i == 4 => 3
        }
    }
}
```

#### reduceByKey

**rdd.reduceByKey(func:(v,v) => V, numPartitions:Int)**

reduceByKey操作可以将RDD[(K,V)]中的元素按照相同的k对v进行聚合，其存在多种重载形式，还可以设置新的RDD分区数

```scala
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark21_Transformation_reduceByKey {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Spark21_Transformation_reduceByKey").setMaster("local[*]")
        val sc = new SparkContext(conf)

        val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 3), ("a", 5), ("b", 2)))

        rdd.reduceByKey((i, j) => i + j).collect.foreach(println)

        sc.stop()
    }

}
/*
输出结果
(a,6)
(b,5)
*/
```

#### groupByKey

**rdd.groupByKey()**

groupByKey对每个key进行操作，但只生成一个Seq，并不进行聚合，该操作可以指定分区器，或者分区数（默认使用HashPartitioner）

```scala
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}


/**
 * TODO
 *
 * @author 岳昌宏
 * @date 2021/8/16 19:46
 */
object Test {
    def main(args:Array[String]) :Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Test").setMaster("local[*]")
        val sc = new SparkContext(conf)

        val arrRDD: RDD[(Int, Any)] = sc.parallelize(Array((1, "张三"), (2, "李四"), (1, 20), (2, 18)))

        val resRDD: RDD[(Int, Iterable[Any])] = arrRDD.groupByKey()

        resRDD.collect().foreach(println)

        sc.stop()
    }
}
/*
输出结果
(1,CompactBuffer(张三, 20))
(2,CompactBuffer(李四, 18))
*/
```

#### reduceByKey和groupByKey的区别

reduceByKey：按照key进行聚合，在Shuffle之前，在combine（预聚合）操作，返回结果是RDD[K, V]

groupByKey：按照Key进行分组，直接进行shuffle

开发指导：在不影响业务逻辑的前提下，优先选用reduceByKey，求和操作不影响业务逻辑，求平均值影响业务逻辑

#### aggregateByKey

**rdd.aggregateByKey(zeroValue:U)(seqOp:(U,V) => U, combOp:(U, U) => U)**

**zeroValue（初始值）：给每一个分区中的每一种key一个初始值**

**seqOp（分区内）：函数用于在每一个分区中用初始值逐步迭代value（分区内的计算规则）**

**combOp（分区间）：函数用于分区合并每个分区中的结果**

```scala
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}


/**
 * TODO
 *	分区最大值求和
 *	分区内的最大值，然后分区间求和
 * @author 岳昌宏
 * @date 2021/8/16 19:46
 */
object Test {
    def main(args:Array[String]) :Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Test").setMaster("local[*]")
        val sc = new SparkContext(conf)

        val sourceRDD: RDD[(String, Int)] = sc.parallelize(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)

        sourceRDD.mapPartitionsWithIndex((index, iter) => {
            println("分区编号为：" + index + " >>>:" + iter.mkString(" "))
            iter
        }).collect()

        println("----------------------------------")

        // 需求：分区最大值求和
        val resRDD: RDD[(String, Int)] = sourceRDD.aggregateByKey(0)(math.max, (_ + _))

        resRDD.collect().foreach(println)

        sc.stop()
    }
}
/*
输出结果
分区编号为：0 >>>:(a,3) (a,2) (c,4)
分区编号为：1 >>>:(b,3) (c,6) (c,8)
----------------------------------
(b,3)
(a,3)
(c,12)

*/
```

#### foldByKey

**rdd.foldByKey(zeroValye:V)(func:(V, V) => V)**

**zeroValue：是一个初始值，它可以是任意类型**

**func：是一个函数，两个输入参数相同**

aggregateByKey的简化操作，seqOp和combOp相同，即分区内逻辑和分区间逻辑相同

```scala
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

/**
 * TODO
 *
 * @author 岳昌宏
 * @date 2021/8/16 19:46
 */
object Test {
    def main(args:Array[String]) :Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Test").setMaster("local[*]")
        val sc = new SparkContext(conf)

        val sourceRDD: RDD[(String, Int)] = sc.parallelize(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)

        sourceRDD.mapPartitionsWithIndex((index, iter) => {
            println("分区编号为：" + index + " >>>:" + iter.mkString(" "))
            iter
        }).collect()

        println("----------------------------------")

        // 每个k的初始值为6，求最大值
        val resRDD: RDD[(String, Int)] = sourceRDD.foldByKey(6)(math.max)

        resRDD.collect().foreach(println)

        sc.stop()
    }
}
/*
输出结果
分区编号为：0 >>>:(a,3) (a,2) (c,4)
分区编号为：1 >>>:(b,3) (c,6) (c,8)
----------------------------------
(b,6)
(a,6)
(c,8)
*/
```

#### reduceByKey，arrgegateByKey和foldByKey的使用区别

如果分区内和分区间的计算规则一样，并且不需要指定初始值，那么优先使用reduceByKey

如果分区内和分区间计算规则一样，并且需要指定初始值，那么优先使用foldByKey

如果分区内和分区间计算规则不一样，并且需要指定初始值，那么优先使用aggregateByKey

#### combineByKey

**rdd.combineByKey[C]{**

​	**createCombiner : V => C**	

​	**mergeValue : (C,V) => C**

​	**mergeCombiner : (C, C) => C**

**}**

**createCombiner : V => C ”分组内的创建组合函数的函数,通俗点就是对读进来的数据进行初始化，把当前的值作为参数，可以对该值做一些转换操作，转换我我们想要的数据格式“**

**mergeValue : (C,V) => C ”该函数主要是分区内的合并函数，作用在每一个分区内部。其功能主要是将V合并到之前的元素C上，这里的C指的是上一函数转换之后的数据格式，而这里的V指的是原始数据格式“**

**mergeCombiners : (C, C) => R ”该函数主要是进行多分区合并，此时是将两个C合并为一个C“**

```scala
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * TODO
 *
 * @author 岳昌宏
 * @date 2021/7/31 20:35
 */
object Spark25_Transformation_combineByKey {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Spark25_Transformation_combineByKey").setMaster("local[*]")
        val sc = new SparkContext(conf)

        // 求出每一个学生的平均成绩
        val scoreRDD: RDD[(String, Int)] = sc.makeRDD(List(("jingjing", 90), ("yuechanghong", 98), ("jingjing", 88), ("yuechanghong", 86), ("jingjing", 92), ("yuechanghong", 96)))


        val rdd: RDD[(String, (Int, Int))] = scoreRDD.combineByKey(
            			   (_, 1), 
                           (tup1: (Int, Int), v) => (tup1._1 + v, tup1._2 + 1),
                           (tup2: (Int, Int),tup3: (Int, Int)) => (tup2._1 + tup3._1, tup2._2 + tup3._2)
        )

        rdd.map{
           case (name, date) => (name,date._1 / date._2)
        }.collect.foreach(println)


        sc.stop()

    }
}
/*
输出结果
(jingjing,90)
(yuechanghong,93)
*/
```

#### sortByKey

**rdd.sortByKey(ascending:Boolean = true, numPartitions : Int = self.partitions.length)**

sortByKey按照k进行排序，在一个(K, V)的RDD上调用，K必须实现**Ordered**接口，返回一个按照key进行排序的（K， V）的RDD

```scala
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * TODO
 *
 * @author 岳昌宏
 * @date 2021/8/1 8:25
 */
object Spark16_Transformation_sortByKey {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Spark16_Transformation_sortByKey").setMaster("local[*]")
        val sc = new SparkContext(conf)


        val rdd: RDD[(Int, String)] = sc.makeRDD(Array((3, "a"), (6, "c"), (2, "bb"), (1, "dd")))

        rdd.sortByKey().collect.foreach(println)

        sc.stop()
    }

}
/*
输出结果
(1,dd)
(2,bb)
(3,a)
(6,c)
*/
```

```scala
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}


/**
 * TODO
 *
 * @author 岳昌宏
 * @date 2021/8/16 19:46
 */
object Test {
    def main(args:Array[String]) :Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Test").setMaster("local[*]")
        val sc = new SparkContext(conf)

        val sourceRDD: RDD[(Student, String)] = sc.parallelize(Array((new Student(1, "岳昌宏"), "**"), (new Student(3, "李四"), "**"), (new Student(2, "张三"), "**")))

        sourceRDD.collect().foreach(println)

        println("-------------------------------")

        val resRDD: RDD[(Student, String)] = sourceRDD.sortByKey()
        resRDD.collect().foreach(println)


        sc.stop()
    }

    class Student(val id:Int, val name:String) extends Ordered[Student] with Serializable {

        override def compare(that: Student): Int = {
            this.id.compareTo(that.id)
        }

        override def toString: String = {
            s"${id} + ${name}"
        }
    }
}
/*
输出结果
(1 + 岳昌宏,**)
(3 + 李四,**)
(2 + 张三,**)
-------------------------------
(1 + 岳昌宏,**)
(2 + 张三,**)
(3 + 李四,**)
*/
```

#### mapValues

**rdd.mapValues(f : U => U)**

mapValues针对于(k,v)形式的类型只对v进行操作

```scala
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}


/**
 * TODO
 *
 * @author 岳昌宏
 * @date 2021/8/16 19:46
 */
object Test {
    def main(args:Array[String]) :Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Test").setMaster("local[*]")
        val sc = new SparkContext(conf)

        val sourceRDD: RDD[(Int, String)] = sc.makeRDD(List((1, "张三"), (2, "李四"), (3, "王二"), (4, "麻子")))

        val resRDD: RDD[(Int, String)] = sourceRDD.mapValues(_ + "**")
        
        resRDD.collect().foreach(println)
        
        sc.stop()
    }
}
/*
输出结果
(1,张三**)
(2,李四**)
(3,王二**)
(4,麻子**)
*/
```

#### join

**rdd.join(other:RDD)**

join连接操作，将相同key对应的多个value关联在一起，在类型（K, V）和（K，W）的RDD上调用，返回一个相同key对应的所有元素对在一起的（K, (V, W)）的RDD，如果key只是某一个RDD有，这个key不会关联

```scala
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}


/**
 * TODO
 *
 * @author 岳昌宏
 * @date 2021/8/16 19:46
 */
object Test {
    def main(args:Array[String]) :Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Test").setMaster("local[*]")
        val sc = new SparkContext(conf)

        val sourceRDD1: RDD[(Int, String)] = sc.makeRDD(List((1, "张三"), (2, "李四"), (3, "王二"), (4, "麻子"), (6, "学院")))
        val sourceRDD2: RDD[(Int, String)] = sc.makeRDD(List((1, "商丘"), (2, "师范"), (3, "学院"), (4, "信息"), (5, "技术")))

        val resRDD: RDD[(Int, (String, String))] = sourceRDD1.join(sourceRDD2)

        resRDD.collect().foreach(println)

        sc.stop()
    }
}
/*
输出结果
(1,(张三,商丘))
(2,(李四,师范))
(3,(王二,学院))
(4,(麻子,信息))
*/
```

#### cogroup

**rdd.cogroup(other1:RDD[(K, V)], other1:RDD[(K, V)])**

在类型为（K， V）和（K,  W）的RDD上调用，返回一个(K, (Iterable<V>,  Iterable<W>))类型的RDD

```scala
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}


/**
 * TODO
 *
 * @author 岳昌宏
 * @date 2021/8/16 19:46
 */
object Test {
    def main(args:Array[String]) :Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Test").setMaster("local[*]")
        val sc = new SparkContext(conf)

        val sourceRDD1: RDD[(Int, String)] = sc.makeRDD(List((1, "张三"), (1, "李四"), (2, "王二"), (2, "麻子"), (3, "学院")))
        val sourceRDD2: RDD[(Int, String)] = sc.makeRDD(List((1, "商丘"), (1, "师范"), (2, "学院"), (2, "信息"), (4, "技术")))

        val resRDD: RDD[(Int, (Iterable[String], Iterable[String]))] = sourceRDD1.cogroup(sourceRDD2)

        resRDD.collect().foreach(println)

        sc.stop()
    }
}
/*
输出结果
(1,(CompactBuffer(张三, 李四),CompactBuffer(商丘, 师范)))
(2,(CompactBuffer(王二, 麻子),CompactBuffer(学院, 信息)))
(3,(CompactBuffer(学院),CompactBuffer()))
(4,(CompactBuffer(),CompactBuffer(技术)))
*/
```

## Action行动算子

行动算子是触发了整个作业的执行，转换算子都是懒加载，并不会立即执行

### reduce

**rdd.reduce(f : (U,U) => U)**

f函数聚集RDD中的所有元素，先聚合分区内数据，再聚合分区间的数据

```scala
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

object Test {
    def main(args : Array[String]) : Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Text")
        val sc = new SparkContext(conf)

        val sourceRDD: RDD[Int] = sc.parallelize(List(1, 2, 3, 4))

        val res: Int = sourceRDD.reduce(_ + _)

        println(res)

        sc.stop()
    }
}
/*
输出结果
10
*/
```



### collect

**rdd.collect()**

在驱动程序中，以数组Array的形式返回数据集的所有元素，将每一个Excutor中的数据收集到Driver端

```scala
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

object Test {
    def main(args : Array[String]) : Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Text")
        val sc = new SparkContext(conf)

        val sourceRDD: RDD[Int] = sc.parallelize(List(1, 2, 3, 4))

        val res: Array[Int] = sourceRDD.collect()

        res.foreach(println)

        sc.stop()
    }
}
/*
输出结果：
1
2
3
4
*/
```

### foreach

**rdd.foreach(f : T => Unit)**

遍历RDD中的每一个元素，并依次应用f函数

```scala
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

object Test {
    def main(args : Array[String]) : Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Text")
        val sc = new SparkContext(conf)

        val sourceRDD: RDD[Int] = sc.parallelize(List(1, 2, 3))

        sourceRDD.foreach(println)

        sc.stop()
    }
}
/*
输出结果
1
2
3
*/
```

### count

**rdd.count():Long**

返回RDD中的元素

### first

**rdd.first():T**

返回RDD中的第一个元素

### take

**rdd.take(num:Int):Array[T]**

返回由RDD前n个元素组成的数组

### takeOrdered

**rdd.takeOrdered(num : Int)(implicit ord : Ordering[T]) : Arrya[T]**

返回该RDD排序后前n个元素组成的数组

```scala
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

object Test {
    def main(args : Array[String]) : Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Text")
        val sc = new SparkContext(conf)

        val sourceRDD: RDD[Int] = sc.parallelize(List(1, 6, 1, 23, 4))

        val res: Array[Int] = sourceRDD.takeOrdered(4)

        res.foreach(println)


        sc.stop()
    }
}
/*
输出结果
1
1
4
6
*/
```

### aggregate

**rdd.aggregate(zeroValue : U)(SeqOp : (U, T)=>U, CombOp : (U, U) => U) : U**

arrregate函数将每个分区里面的元素通过分区内逻辑和初始值进行聚合，然后用分区间逻辑和初始值进行操作，注意：分区间逻辑再次使用初始值和arrregateByKey是有区别的

```scala
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

object Test {
    def main(args : Array[String]) : Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Text")
        val sc = new SparkContext(conf)

        val sourceRDD: RDD[Int] = sc.parallelize(List(1, 6, 1, 23, 4))


        sourceRDD.mapPartitionsWithIndex((index, iter) => {
            println(index + ">>>>>" + iter.mkString(" "))
            iter
        }).collect().foreach(println)

        println("-----------------------------------------------")

        val res: Int = sourceRDD.aggregate(10)(Math.max, _ + _)

        println(res)

        sc.stop()
    }
}
/*
输出结果
5>>>>>
7>>>>>4
0>>>>>
1>>>>>1
6>>>>>23
3>>>>>6
2>>>>>
4>>>>>1
-----------------------------------------------
103
*/
```

### fold

**rdd.fold(zeroValue : T)(Op : (T, T) => T)**

折叠操作，aggregate的简化操作，即分区内逻辑和分区间逻辑相同

### countByKey()

**rdd.countByKey()**

统计每种key的个数

### saveAsTextFile(path)

将数据集的元素以textfile的形式保存到HDFS文件系统或者其他支持的文件系统，对于每个元素，spark将会调用toString方法，将它转换成文件中的文本

### saveAsSequenceFile(path)

将数据集中的元素以Hadoop Sequencefile的格式保存到指定目录下，可以使用HDFS或其他Hadoop支持的文件（只有kv类型RDD有该操作，单值没有）

### saveAsObjectFile(path)

用于将RDD中的元素序列化成对象，存储到文件中
