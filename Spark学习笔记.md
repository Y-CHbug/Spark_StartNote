#  Spark

[TOC]



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

## 序列化

因为在Spark程序中，**算子相关的操作在Excutor上执行，算子之外的代码在Driver上执行**，在执行有些算子的时候，需要使用Driver里定义的数据，这就涉及到了跨进程或者跨节点之间的通讯，所以要求传递给Excutor中的数据，所属的类型必须实现Serializable接口。

```scala
package test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Version 1.0
 * @Author:岳昌宏
 * @Date:2021/8/31
 * @Content:
 */
object TestSerializable {
    def main(args:Array[String]) : Unit = {
        val conf: SparkConf = new SparkConf().setAppName("TestSerializable").setMaster("local[*]")
        val sc = new SparkContext(conf)

        val user1 = new User()
        user1.name = "张三"

        val user2 = new User()
        user2.name = "李四"

        /*
        算子相关的代码在Excutor执行，一下代码在Excutor端执行，上面的代码在Dirver端执行
        */
        
        val rdd: RDD[User] = sc.makeRDD(List(user1, user2))

        rdd.foreach(println)

        sc.stop()

    }

}

class User extends Serializable {
    var name : String = _

    override def toString: String = {
        name
    }
}
```

### 闭包检查

如果算子的参数是函数的形式，都会存在闭包检查，在作业job提交之前，其中有一行代码 var cleanF = sc.clean(f) ,用于进行闭包检查，之所以叫闭包检查，是因为在当前函数的内部访问了外部函数的变量，属于闭包的形式。

```scala
def foreach(f : T => Unit): Unit = withScope {
  val cleanF = sc.clean(f)
  sc.runJob(this, (iter: Iterator[T]) => iter.foreach(cleanF))
}
```

例如：上一个代码在执行rdd.foreach(println)时，非简化版是rdd.foreach(user => println(user)) 此时user => println(user)属于内部函数，访问了外部的变量user

### Kryo序列化框架

java的序列化能够序列化任何类，但是比较重，序列化后对象的提交也比较大。Spark出于性能的考虑，Spark2.0开始支持另外一种Kryo序列化机制。Kryo速度是Serializable的10倍。当RDD在Shuffle数据的时候，简单数据类型，数组和字符串类型已经在Spark内部使用Kryo类序列化。

## RDD依赖关系

### 查看血缘关系

RDD只支持粗粒度转换，即在大量记录上执行的单个操作，将创建RDD的一系列Lineage（血统）记录下来，以便恢复丢失的分区。RDD的Lineage会记录RDD的元数据信息和转换行为。当该RDD的部分分区数据丢失时，它可以根据这些信息来重新运算和恢复丢失的数据分区。

通过toDebugString方法，查看RDD血缘关系

```scala
package test

import org.apache.spark.rdd.RDD
import org.apache.spark.{Dependency, SparkConf, SparkContext}

/**
 * @Version 1.0
 * @Author:岳昌宏
 * @Date:2021/8/31
 * @Content:
 */
object toDebugStringTest {
    def main(args: Array[String]): Unit = {
        val conf:SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

        val sc:SparkContext = new SparkContext(conf)

        val listRDD: RDD[String] = sc.makeRDD(List("hello", "spark", "hello Spark"), 2)

        val string: String = listRDD.toDebugString
        println(string)
        println("----------------------------")
        
        val flatMapRDD: RDD[String] = listRDD.flatMap(_.split(" "))

        val string1: String = flatMapRDD.toDebugString
        println(string1)
        println("----------------------------")

        val mapRDD: RDD[(String, Int)] = flatMapRDD.map((_, 1))

        val string2: String = mapRDD.toDebugString
        println(string2)
        println("----------------------------")

        val resRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

        val string3: String = resRDD.toDebugString
        println(string3)
        println("----------------------------")

        resRDD.collect()

        sc.stop()
    }
}

/*
输出结果
(2) ParallelCollectionRDD[0] at makeRDD at toDebugStringTest.scala:22 []
----------------------------
(2) MapPartitionsRDD[1] at flatMap at toDebugStringTest.scala:28 []
 |  ParallelCollectionRDD[0] at makeRDD at toDebugStringTest.scala:22 []
----------------------------
(2) MapPartitionsRDD[2] at map at toDebugStringTest.scala:34 []
 |  MapPartitionsRDD[1] at flatMap at toDebugStringTest.scala:28 []
 |  ParallelCollectionRDD[0] at makeRDD at toDebugStringTest.scala:22 []
----------------------------
(2) ShuffledRDD[3] at reduceByKey at toDebugStringTest.scala:40 []
 +-(2) MapPartitionsRDD[2] at map at toDebugStringTest.scala:34 []
    |  MapPartitionsRDD[1] at flatMap at toDebugStringTest.scala:28 []
    |  ParallelCollectionRDD[0] at makeRDD at toDebugStringTest.scala:22 []
----------------------------
*/
```

### 查看依赖关系

通过dependencies方法查看RDD依赖关系

```scala
package test

import org.apache.spark.rdd.RDD
import org.apache.spark.{Dependency, SparkConf, SparkContext}

/**
 * @Version 1.0
 * @Author:岳昌宏
 * @Date:2021/8/31
 * @Content:
 */
object toDebugStringTest {
    def main(args: Array[String]): Unit = {
        val conf:SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

        val sc:SparkContext = new SparkContext(conf)

        val listRDD: RDD[String] = sc.makeRDD(List("hello", "spark", "hello Spark"), 2)

        val list = listRDD.dependencies
        println(list)
        println("----------------------------")

        val flatMapRDD: RDD[String] = listRDD.flatMap(_.split(" "))

        val list1 = flatMapRDD.dependencies
        println(list1)
        println("----------------------------")

        val mapRDD: RDD[(String, Int)] = flatMapRDD.map((_, 1))

        val list2 = mapRDD.dependencies
        println(list2)
        println("----------------------------")

        val resRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

        val list3 = resRDD.dependencies
        println(list3)
        println("----------------------------")

        resRDD.collect()

        sc.stop()
    }

}
/*
输出结果：
List()
----------------------------
List(org.apache.spark.OneToOneDependency@110844f6)
----------------------------
List(org.apache.spark.OneToOneDependency@511816c0)
----------------------------
List(org.apache.spark.ShuffleDependency@22d9c961)
----------------------------
                                                                                
Process finished with exit code 0

*/
```

### 宽依赖和窄依赖

RDD之间的关系可以从两个维度来理解，一个是RDD是从哪些RDD转换而来，也就是RDD的ParentRDD(s)是什么；另一个就是RDD依赖于ParentRDD(s)的哪些Partition(s)这种关系就是RDD之间的依赖。

#### 窄依赖

窄依赖表示每一个父RDD的Partition最多被一个子RDD的一个Partition使用，可以理解成独生子女

#### 宽依赖

宽依赖表示同一个父RDD的Partition被多个子RDD的Partition依赖，会引起Shuffle。可以理解成超生。

具有宽依赖的transformations包括sort，reduceByKey，groupByKey，join和调用rePartition函数的任何操作，宽依赖对Spark去评估一个transformations有非常重要的影响。

### Spark中Job调度

一个**Saprk应用**包含一个驱动进程(Driver Process，在这个进程中写Spark的逻辑代码) 和多个执行器进程(Executor Process, 跨越集群中的多个节点) Spark程序是自己运行在驱动节点，然后发送指令到执行器节点。（也就是说，算子相关的代码在Excutor端执行，算子之外的代码在Driver端执行）

**一个Spark集群可以同时运行多个Spark应用**，这些应用是由**集群管理器**（cluster manager）来调度。

**Spark应用可以并发运行多个job**，job对应着给定的应用内的在RDD上的每个action操作。

### Spark应用

一个Spark应用可以包含多个Spark job,Spark job是在驱动程序中由SparkContext来定义的，当启动一个SparkContext的时候，就开启了一个Spark应用，然后一个驱动程序被启动了，多个执行器在集群中的多个工作节点（worker nodes）也被启动了，一个执行器就是一个JVM，一个执行器不能跨越多个节点，但是一个节点可以包括多个执行器。

一个RDD会跨多个执行器并行计算，每一个执行器可以有这个RDD的多个分区，但是一个分区不能跨越多个执行器。

#### Spark job的划分

由于Spark的懒执行，在驱动程序调用一个action之前，Spark应用不会做任何事情，针对每个action，Spark调度器就会创建一个**执行图**（execution graph）和启动一个Spark job。

每个job由多个stages组成，这些**stages**就是实现最终的RDD所需的数据转换的步骤，一个**宽依赖划分一个stage**，每个stage由多个**tasks**组成，这些tasks就表示每个并行计算，并且会在多个执行器上执行，每一个阶段的最后一个RDD的分区数就是当前阶段的Task的个数。

#### DAG

有向无环图DAG是由点和线组成的拓扑图形，该图形具有方向，不会闭环，原始的RDD通过一系列的转换就成形成了DAG，根据RDD之间的依赖关系的不同，将DAG划分成不的stage，对于窄依赖，partition的转换处理在Stage中完成计算，对于宽依赖由于有shuffle的存在，只能在parentRDD处理完成之后，才能开始接下来的计算，因此宽依赖是划分Stage的依据。

------

Application：初始化一个SparkContext即生成一个Application

Job：一个Action算子就会生成一个Job

Stage：Stage的数量等于宽依赖的个数加1

Task：一个Stage阶段最后一个RDD的分区个数就是Task的个数

## RDD持久化

### RDD Cache缓存

RDD Cache缓存是通过Cahe或者Persist方法将前面的计算结果缓存，默认情况下会把数据以序列化缓存在JVM的堆内存中，但是并不是这两个方法被调用时立即缓存，而是**触发后面的action时**，该RDD将会被缓存在计算节点的内存中，并供后面重用。

Cache会增加血缘关系，但不会改变血缘关系，Cache底层调用的是Persist，Psersist底层默认缓存在内存中，Persist可以接收参数，指定缓存的位置。

虽然叫做持久化，但是当应用程序执行结束后，缓存的目录也会被删除。

```scala
package test

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Version 1.0
 * @Author:岳昌宏
 * @Date:2021/9/1
 * @Content:
 */
object TestCache {
    def main(args : Array[String]) : Unit = {
        val conf: SparkConf = new SparkConf().setAppName("TestCache").setMaster("local[*]")
        val sc = new SparkContext(conf)

        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5))
        
//        rdd.persist(StorageLevel.MEMORY_ONLY)
        
        rdd.cache()
        
        rdd.collect().foreach(println)
        
        sc.stop()
    }
}
```

**自带缓存算子**

Spark会自动对一些Shuffle操作的中间数据做持久化操作（比如reduceByKey）这样做的目的是为了当一个节点Shuffle失败了，避免重新计算整个输入，但是在实际使用的时候，如果想重用数据，任然建议调用persist或者cache

### RDD checkPoint检查点

检查点：是通过将RDD中间结果写入磁盘中

为什么要做检查点：由于血缘依赖过长会导致容错成本过高，这样就不如在中间阶段做检查点容错，如果检查点之后的节点有问题，可以从检查点开始重做血缘，减小开销，

检查点存储路径：checkpoint的数据通常存储在HDFS等容错，高可用的文件系统

检查点的存储格式：二进制文件

检查点切断血缘：在checkpoint的过程中，该RDD的所有依赖于父RDD中的信息将全部被移出。

检查点触发的时间：对RDD进行checkpoint操作并不会马上执行，必须执行Action操作才能触发，但是检查点为了数据安全，会从血缘关系的最开始执行一遍。

```scala
package com.yue.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * TODO
 *
 * @author 岳昌宏
 * @date 2021/8/3 11:18
 */
object Spark32_checkpoint {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Spark31_cache").setMaster("local[*]")
        val sc = new SparkContext(conf)

        // 设置检查点目录
        sc.setCheckpointDir("D:\\IDEA_project\\Spark\\output")

        // 开发环境应该将检查点设置在HDFS上
//        System.setProperty("HADOOP_USER_NAME", "yue")
//        sc.setCheckpointDir("hdfs://hadoop104:8020/output")


        val rdd: RDD[String] = sc.parallelize(List("python", "java", "scala"), 2)

        val flatMapRDD: RDD[String] = rdd.flatMap(_.split(" "))

        val mapRDD: RDD[(String, Long)] = flatMapRDD.map {
            word => {
                (word, System.currentTimeMillis())
            }
        }

        // 在开发环境，一般检查点和缓存配合使用
        mapRDD.cache()
        mapRDD.checkpoint()

        mapRDD.collect().foreach(println)
        println("_______")
        mapRDD.collect().foreach(println)
        println("_______")
        mapRDD.collect().foreach(println)
    }
}
/*
输出结果
(python,1630468314145)
(java,1630468314145)
(scala,1630468314146)
_______
(python,1630468314145)
(java,1630468314145)
(scala,1630468314146)
_______
(python,1630468314145)
(java,1630468314145)
(scala,1630468314146)

Process finished with exit code 0

------------------------如果把mapRDD.cache()注释掉输出结果为------------------

(python,1630468396555)
(java,1630468396555)
(scala,1630468396555)
_______
(python,1630468396690)
(java,1630468396690)
(scala,1630468396690)
_______
(python,1630468396690)
(java,1630468396690)
(scala,1630468396690)

Process finished with exit code 0
*/
```

### 缓存和检查点的区别

1. cahe缓存只是将数据保存起来，不切断血缘依赖，checkpoint检查点切断血缘依赖

2. cache缓存的数据通常存储在磁盘上，内存等地方，可靠性低，checkpoint的数据通常存储在HDFS等容错高可用的文件系统

3. 建议对checkpoint()的RDD使用cache缓存，这样checkpoint的job只需要从cache缓存中读取数据即可，否则需要再从新计算一遍

4. 如果使用了缓存，可以通过unpersist()方法释放缓存

    

### 检查点存储到HDFS集群中

```scala
// 开发环境应该将检查点设置在HDFS上
// System.setProperty("HADOOP_USER_NAME", "yue")
// sc.setCheckpointDir("hdfs://hadoop104:8020/output")`
```


## 键字对RDD数据分区

Spark目前支持Hash分区，Range分区和用户自定义分区

①只有key-value类型的RDD才有分区器，非key-value类型的RDD分区值是None

②每个RDD分分区ID范围： 0 ~numPartitions-1 ,决定这个值是属于那个分区的

**HashPartitioner**分区原理：对于给定的key，计算其hashCode，并除以分区的个数取余，最后返回值就是这个key所属的分区ID。HashPartitioner分区的弊端：可能导致每个分区中数据量的不均匀，极端情况下，会导致某些分区拥有RDD的全部数据。

**RangePartitioner**分区原理：将一定范围的数据映射到某一个分区内，尽量保证每一个分区中的数据量均匀，而且分区与分区之间是有序的，一个分区中的元素肯定都比另一个分区内的元素小或者是大，但是分区内的元素是不能保证顺序的，简单的是说就是将一定范围内的数据映射到某一个分区内

Range分区实现过程：一，首先从整个RDD中采用水塘抽样算法，抽取出样本数据，将样本数据排序，计算出每个分区的最大值key值，形成一个Array[key]类型的数组变量rangBounds，二，在判断key在rangBounds中所处的范围，给出该key值在下一个RDD中的分区id下标；该分区器要求RDD中的key类型必须是可以排序的。

## 累加器Accumulator

**分布式共享只写变量**

为什么需要累积器？

```scala
package test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * TODO
 * @author 岳昌宏
 * @date 2021/9/1 16:50
 */
object TestAccumulator {
    def main(args : Array[String]) : Unit = {
        val conf: SparkConf = new SparkConf().setAppName("TestAccumulator").setMaster("local[*]")
        val sc = new SparkContext(conf)

        val rdd: RDD[(String, Int)] = sc.parallelize(List(("a", 1), ("b", 2), ("c", 3)))

        var sum = 0 // 该变量在Driver端生成

        val resRDD: RDD[Any] = rdd.map{case (str, i) => {
            sum += i
            (str, i)
        }}


        println(sum)
        println("--------------------------")

        resRDD.collect().foreach(println)

        sc.stop()
    }
}
/*
输出结果
0
--------------------------
(a,1)
(b,2)
(c,3)
*/
```

如果定义一个普通的变量，则该变量是在Driver端创建的，当Excutor端使用该变量时，**会在Excutor端创建出该变量的副本，算子上的操作都是对副本进行操作的，Driver端的变量不会更新。**

如果需要通过Excutor对Driver端定义的变量进行更新，需要定义累积器，累积器和普通变量相比，会将Excutor端的结果，收集到Driver端进行汇总。

①工作节点上的任务不能相互访问累积器的值，从这些任务的角度来看，累加器是一个只写变量

②对于要在行动操作中使用的累积器，Spark只会把每个任务对个累加器的修改应用一次。因此如果想要一个无论在失败还是重复计算时都绝对可靠的累加器，我们必须把它放在foreach()这样的行动操作中，转化操作中的累积器可能会发生不止一次的更新。也就是说累加器只能在**行动算子**中使用

```scala
package test

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object TestAccumulator {
    def main(args : Array[String]) : Unit = {
        val conf: SparkConf = new SparkConf().setAppName("TestAccumulator").setMaster("local[*]")
        val sc = new SparkContext(conf)

        val sum: LongAccumulator = sc.longAccumulator("myaccumulator")  // 定义一个累加器

        val rdd: RDD[(String, Int)] = sc.parallelize(List(("a", 1), ("b", 2), ("c", 3)))

        val resRDD: RDD[Any] = rdd.map{case (str, i) => { // 在非行动算子中使用累加器
            sum.add(i)
            (str, i)
        }}
        
        println(sum.value)
        println("--------------------------")

        resRDD.collect().foreach(println)

        sc.stop()
    }
}
/*
输出结果
0
--------------------------
(a,1)
(b,2)
(c,3)
*/
```

------



```scala
package test

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object TestAccumulator {
    def main(args : Array[String]) : Unit = {
        val conf: SparkConf = new SparkConf().setAppName("TestAccumulator").setMaster("local[*]")
        val sc = new SparkContext(conf)

        val sum: LongAccumulator = sc.longAccumulator("myaccumulator")

        val rdd: RDD[(String, Int)] = sc.parallelize(List(("a", 1), ("b", 2), ("c", 3)))

        rdd.foreach{case (str, i) => {
            sum.add(i)
            (str, i)
        }}

        println(sum.value)
        println("--------------------------")

        sc.stop()
    }
}
/*
输出结果
6
--------------------------

Process finished with exit code 0
*/
```

### 自定义累积器

```scala
package test

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
/**
 * TODO
 * @author 岳昌宏
 * @date 2021/9/1 20:17
 */
object TestMyAccumulator {
    def main(args : Array[String]) : Unit = {
        val conf = new SparkConf().setAppName("TestMyAccumulator").setMaster("local[*]")
        val sc = new SparkContext(conf)

        val rdd: RDD[String] = sc.makeRDD(List("Hello", "Hello Python", "Scala"))

        var myacc = new MyAccumulator

        sc.register(myacc) // 注册累加器

        rdd.flatMap(_.split(" ")).foreach(str =>{
            myacc.add(str)
        })

        println(myacc.value)
    }

}

// 定义输入类型和输出类型
class MyAccumulator extends AccumulatorV2[String, mutable.Map[String, Int]]{
    
    // 用来存放单词和单词个数
    var map = mutable.Map[String, Int]()

    // 判断当前的map是否为空
    override def isZero: Boolean = map.isEmpty


    // 拷贝
    override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = {
        var newacc = new  MyAccumulator
        newacc.map = this.map
        newacc
    }

    // 重置
    override def reset(): Unit = map.clear()

    // 添加
    override def add(v: String): Unit = {
        map(v) = map.getOrElse(v, 0) + 1
    }

    // 合并
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
        var map1 = map
        var map2 = other.value

        map = map1.foldLeft(map2){
            (mm, kv) =>{
                val k: String = kv._1
                val v: Int = kv._2

                mm(k) = mm.getOrElse(k, 0) + v
                mm
            }
        }
    }

    override def value: mutable.Map[String, Int] = map
}
```

## 广播变量BroadCast

**分布式共享只读变量**

在多个并行操作中(Excutor)使用同一个变量，Spark默认会认为每个任务(Task)分别发送，这样如果共享比较大的对象，会占用很大的工作节点内存，广播变量用来高效分发较大的对象，向所有工作节点(Excutor)发送一个较大的只读值，以供一个或多个Spark操作使用。

①通过对一个类型T的对象调用SparkContext.broadcast创建出一个BroadCast[T]对象，任何**可序列化**的类型都可以这么实现

②通过value属性访问该对象的值

③变量只会被发到各个节点，应作只读变量处理（修改这个值不会影响到别的节点）

```scala
package com.yue.spark

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * TODO
 *
 * @author 岳昌宏
 * @date 2021/8/4 17:52
 */
object Spark38_Broadcast {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Spark47_Broadcast").setMaster("local[*]")
        val sc = new SparkContext(conf)

        // 实现类似join的效果 ("a",(1, 4)),("b", (2, 5)),("c", (3, 6))
        val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
        val list = List(("a", 4), ("b", 5), ("c", 6))

        val broadcastList: Broadcast[List[(String, Int)]] = sc.broadcast(list)

        val resRDD: RDD[(String, (Int, Int))] = rdd.map {
            case (k, v) => {
                var v3 = 0
                for ((k2, v2) <- broadcastList.value) {
                    if (k == k2) {
                        v3 = v2
                    }
                }
                (k, (v, v3))
            }
        }

        resRDD.collect.foreach(println)

        sc.stop()

    }

}
```

