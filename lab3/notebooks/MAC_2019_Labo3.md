---
jupyter:
  jupytext:
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.1'
      jupytext_version: 1.2.4
  kernelspec:
    display_name: Scala 2.12
    language: scala
    name: scala212
---

# Labo 3: Spark

Authors: Christopher MEIER and Guillaume HOCHET

Based on the work of: Gary MARIGLIANO and Miguel SANTAMARIA

For MAC course given by Nastaran FATEMI

Date: November 2019

```scala
import $ivy.`org.apache.spark::spark-sql:2.4.4`
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._

import org.apache.log4j.{Level, Logger}
Logger.getLogger("org").setLevel(Level.WARN)
```

```scala
// Create a spark session
// To have better integration with Jupyter, we use a wrapper class provided by almond-spark
import org.apache.spark.sql._
val spark = {
  NotebookSparkSession.builder()
    .master("local[*]")
    .getOrCreate()
}
import spark.implicits._
```

```scala
// Retrieve the Spark context
def sc = spark.sparkContext
```

```scala
case class Movie(id: Int, title: String, genres: Seq[String],
                 description: String, director: String, actors: Seq[String],
                 year: Int, rating: Float, votes: Int)
```

```scala
  def parseRow(row: Row): Movie = {
    val id = row.getInt(0)
    val title = row.getString(1)
    val genres = row.getString(2).split(",").toList
    val description = row.getString(3)
    val director = row.getString(4)
    val actors = row.getString(5).split(",").toList
    val year = row.getInt(6)
    val rating = row.getDouble(8).toFloat
    val votes = row.getInt(9)

    Movie(id, title, genres, description, director, actors, year, rating, votes)
  }
```

```scala
val filename = "../data/IMDB-Movie-Data.csv"
val moviesDF = spark.read.format("csv")
    .option("sep", ",")
    .option("inferSchema", "true")
    .option("header", "true")
    .load(filename)
val rddMovies = moviesDF.rdd.map(parseRow)
```

```scala
// Print the title of the first 10 movies to see if they were correctly added.
rddMovies.take(10).map(m => m.title).foreach(println)
```

## Part 1 - Playing with the movies using RDD functions

The goal of this part is to play (i.e. query, filter and transform the data) with the movies.

<!-- #region -->
### Ex1 - Print the movies whose title contains "City" 

Goal: 

* use `map()` and `filter()` methods to get the title of the movies that contains "City" in their title
 
Output example:

```plain
City of Tiny Lights
The Mortal Instruments: City of Bones
```

Steps:

* Use `filter()` to only keep the movies that contains "City" in their title
* Use `map()` to retrieve the titles of these filtered movies
* Use `foreach()` to pretty print the results

<!-- #endregion -->

```scala
// TODO students
```

<!-- #region -->
### Ex2- Print the title of the movies rated between `rateMin` and `rateMax`. Take the 10 worst ratings.

Goal:
    
* Take the titles of the movies that were rated between `rateMin` and `rateMax` (exclusing `rateMin` and including`rateMax`).
* This list is sorted by rating ASC
    
Output example:

```plain
...
3.5 - Wrecker
3.7 - The Last Face
...
```
    
Steps:

* Use `filter()` to only keep the movies released between `rateMin` and `rateMax`
* Sort the filtered movies by decreasing rating
* Use `map()` to keep only the relevant attributes (i.e. rating and title)
* Use `foreach()` to pretty print the results
<!-- #endregion -->

```scala
// TODO student
```

<!-- #region -->
### Ex3 - Print the 10 top genres

Goals:

* Print the list of the genres that appears the most
* Use `flatMap()`

Output example:

```plain
Drama (513)
Action (303)
Comedy (279)
Adventure (259)
```

Theory:

When an operation is giving you a sequence of sequences like:

```scala
Array("hello", "world").map(word => word.split(""))
res91: Array[Array[String]] = Array(Array(h, e, l, l, o), Array(w, o, r, l, d))
```

You may want to flatten this to only have a single list like:
```scala
Array("hello", "world").map(_.split("")).flatten
res93: Array[String] = Array(h, e, l, l, o, w, o, r, l, d)
```

You can achieve the same result (i.e. `map` + `flatten`) using `flatMap`:
```scala
Array("hello", "world").flatMap(_.split(""))
res95: Array[String] = Array(h, e, l, l, o, w, o, r, l, d)
```

We are going to apply this same technique with the `genres` member.

Steps:

* Use `flatMap()` to get the list with all the genres
* Make sure to remove trailling whitespaces
* Count the genres
* Sort them by decreasing order
* Show the top N genres
<!-- #endregion -->

```scala
// TODO student
```

<!-- #region -->
### Ex4 - Print the average number of votes per year, order by descreasing number of votes

Goal:

* Print the average votes per year
* This output is sorted by descreasing votes

Output example:

```plain
...
year: 2008 average votes: 275505.3846153846
year: 2009 average votes: 255780.64705882352
year: 2010 average votes: 252782.31666666668
...
```

Theory:

We are going to use `reduceByKey()` which has the following signature `reduceByKey(func: (V, V) => V): RDD[(K, V)]`. 

`reduceByKey()` works on a RDD like `RDD[(K,V)]` (i.e. sort of "list of key/values pairs"). 

`reduceByKey()` takes a function that, from two elements, returns one i.e. the `func: (V, V) => V` in the signature.
The difference with `reduce()` is that `reduceByKey()` uses two elements sharing the same key.

For example (pseudo code):

```plain
 year, count
(2010, 2)
(2011, 3)
(2011, 4)
(2010, 8)
// use reduceByKey((count1, count2) => count1+count2)
> (2010, 10)
> (2011, 7)
```

Note: here `count` is just an Int but it can be anything e.g. `Movie`

Steps:

* To compute the average we need the **total sum** of votes per year and the **count** of all the movies per year
* Use `map()` to create an RDD made of `(year, (votes, 1))`. Like a word count we use the `1` to be able to count the number of movies per year
* Use `reduceByKey()` to sum the votes and to count the number of movies per year. The output should look like: `(year, (totalVotes, moviePerYearCount))`
* Find a way to compute the average using the result from the last operation
* Sort by number of votes decreasing
<!-- #endregion -->

```scala
// TODO student
```

## Part 2 - Create a basic Inverted Index

The goal of this part is to show you how to create an inverted index that indexes words from all the movies' description.

<!-- #region -->
Goal:

Using `rddMovies` create an inverted that use the movies' description:

```plain
Movie(1,Guardians of the Galaxy,List(Action, Adventure, Sci-Fi),A group of intergalactic [...] of the universe.,James Gunn,List(Chris Pratt, Vin Diesel, Bradley Cooper, Zoe Saldana),2014,8.1,757074.0)
Movie(2,Prometheus,List(Adventure, Mystery, Sci-Fi),Following clues to the origin[...] not alone.,Ridley Scott,List(Noomi Rapace, Logan Marshall-Green, Michael Fassbender, Charlize Theron),2012,7.0,485820.0)
Movie(3,Split,List(Horror, Thriller),Three girls are kidnapped [...] a frightful new 24th.,M. Night Shyamalan,List(James McAvoy, Anya Taylor-Joy, Haley Lu Richardson, Jessica Sula),2016,7.3,157606.0)
...
```

and extract them to produce an inverted index like:

```plain
"reunion" -> (640, 697)
"runner" -> (338)
"vietnam" -> (797, 947, 983)
...
```

Steps

* Tokenize description i.e. produce an RDD like (movId, words)
* Normalize words e.g. toLowercase, trimming,..
* Remove stopwords (ignored here)
* Apply stemming (ignored here)
* Group by document id
<!-- #endregion -->

```scala
    /**
    * Goal: create an inverted index that allows searching a word
    * in the movies description.
    * Features:
    * - case insensitive
    *
    */
    // TODO student
    // In this first function we are going to tokenize and order the descriptions of the movies, then return these data. We are not going to apply any search right now.
    def createInvertedIndex(movies: RDD[Movie]): RDD[(String, Iterable[Int])] = {
        // Define helper functions directly inside this function. In scala you can declare inner functions
        // and use them only inside the function they were declared. Useful to encapsulate/restrict 
        // their use outside this function.
        
        // Split the given string into an array of words (without any formatting), then return it.
        def tokenizeDescription(description: String): Seq[String] = {
            // TODO student
        }
        
        // Remove the blank spaces (trim) in the given word, transform it in lowercase, then return it.
        def normalizeWord(word: String): String = {
            // TODO student
        }
        
        // For the sake of simplicity let's ignore the implementation (in a real case we would return true if w is a stopword, otherwise false).
        // TODO student nothing here but still call this function in your invertedIndex creation process.
        def isStopWord(w: String): Boolean = {
          false
        }
        
        // For the sake of simplicity let's ignore the implementation (in a real case we would apply stemming to w and return the result, e.g. w=automation -> w=automat).
        // TODO student nothing here but still call this function in your invertedIndex creation process.
        def applyStemming(w: String): String = {
          w
        }
      
       // TODO student
       // Here we are going to work on the movies RDD, by tokenizing and normalizing the description of every movie, then by building a key-value object that contains the tokens as keys, and the IDs of the movies as values
       // (see the example on 4).
       // The goal here is to do everything by chaining the possible transformations and actions of Spark.
       // Possible steps:
       //   1) What we first want to do here is applying the 4 previous methods on any movie's description. Be aware of the fact that we also want to keep the IDs of the movies.
       //   2) For each tokenized word, create a tuple as (word, id), where id is the current movie id
       //        [
       //          ("toto", 120), ("mange", 120), ("des", 120), ("pommes", 120),
       //          ("toto", 121), ("lance", 121), ("des", 121), ("photocopies", 121)
       //        ]
       //      Hint: you can use a `map` function inside another `map` function.
       //   3) We finally need to find a way to remove duplicated keys and thus only having one entry per key, with all the linked IDs as values. For example:
       //        [
       //          ("toto", [120, 121]),
       //          ("mange", [120]),
       //          ...
       //        ]
       val invertedIndex = ...

       // Return the new-built inverted index.
       invertedIndex
  }
```

Now we would like to use our inverted index to display the top N most used words in the descriptions of movies.

```scala
// TODO student
// Here we are going to operate the analytic and display its result on a given inverted index (that will be obtained from the previous function).
def topN(invertedIndex: RDD[(String, Iterable[Int])], N: Int): Unit = {
  // TODO student
  // We are going to work on the given invertedIndex array to do our analytic:
  //   1) Find a way to get the number of movie in which a word appears.
  //   2) Keep only the top N words and their occurence.
  val topMovies = ...
  
  // Print the words and the number of descriptions in which they appear.
  println("Top '" + N + "' most used words")
  topMovies.foreach(println)
}
```

```scala
// Code used to test your implementation.
// Create the inverted index of the movies.
val invertedIndex = createInvertedIndex(rddMovies)

// Show how the inverted index looks like.
invertedIndex.take(3).foreach(x => println(x._1 + ": " + x._2.mkString(", ")))

// Show the top 10 most used words.
topN(invertedIndex, 10)
```

## Part 3 - Dataframe and SparkSQL

For all of the following exercices, write your queries in two different ways: 

* using the sql literal 
* using DataFrame API (select, where, etc.)


### Exercice 1 - Use the moviesDF DataFrame

* Use the dataframe `moviesDF` already created when loading the data 
* Print the schema of moviesDF
* Show the first 10 lines of the moviesDF as a table

```scala
// TODO students
```

### Exercice 2 - Get the movies (id, title, votes, director) whose title contains "City" 

Apply two different ways: 

* use the sql literal 
* use DataFrame API (select, where, etc.)


```scala
// TODO students
```

### Exercice 3 - Get the number of movies which have a number of votes between 500 and 2000 (inclusive range)

```scala
// TODO students
```

### Exercice 4 - Get the minimum, maximum and average rating of films per director. Sort the results by minimum rating.  

```scala
// TODO students
```

<!-- #region -->
### Exercice 5 - Find the title of the movie(s) having the minimum rating for each year. Show the title, year and the rating information in the result, order by increasing rating.

**Example output**

```plain
+--------------------+----+------+
|               title|year|rating|
+--------------------+----+------+
|      Disaster Movie|2008|   1.9|
|Don't Fuck in the...|2016|   2.7|
|Dragonball Evolution|2009|   2.7|
...
```
<!-- #endregion -->

```scala
// TODO students
```

<!-- #region -->
### Exercice 6 - Find the title of movies having the same director. 

**Example output**

```plain
+------------------+--------------------+--------------------+
|         director1|              title1|              title2|
+------------------+--------------------+--------------------+
|        James Gunn|Guardians of the ...|               Super|
|        James Gunn|Guardians of the ...|             Slither|
|      Ridley Scott|          Prometheus|        Body of Lies|
|      Ridley Scott|          Prometheus|         A Good Year|
...
```

Note that when using the dataframe API:
* You can use `===` to test equality of columns and `!==` to test non-equality of them
* You can use the `as` keyword to make alias of column names and table names (check the reference documentation)
<!-- #endregion -->

```scala
// TODO students
```

```scala

```
