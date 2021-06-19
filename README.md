# big_data_infrastructure

Labs for course "Infrastructure of Big Data" in ITMO University (spring 2020)

**Lab#1**
You need to mount pvc-shared-<your username> into your pod with Jupyter. There you can find "bigdata20" folder. The dataset contains data from the itmo community vk.com (https://vk.com/club94). There are group posts, likes for posts, followers, followers posts from 2019, and his/her likes.

To solve the tasks below you should use Jupyter and PySpark in combination. The result should be in the form of a jupyter notebook separated into sections by markdown cells [1] containing titles describing the tasks.

For each task, there must be a solution that processes data using PySpark dataframes, pyspark built-in functions, and your functions. 

Note: the dataset reading and common preprocessing required by multiple tasks may be allocated to a separate section.

Tasks: 
  1. Find the top 20 posts in the group: (a) by likes; (b) by comments; (c) by reposts
  2. Find the top 20 users by (a) likes and (b) reposts they have made (to trace reposts use "copy_history" field).
  3. get reposts of the original posts of the itmo group (posts.json) from user posts (the result should be similar to (group_post_id, Array (user_post_ids)))
  4. find emoticons in posts and their comments (negative, positive, neutral).
  5. Probable “fans”. Find for each user the top 10 other users whose posts this user likes. 
  6. Probable friends. If two users like each other posts they may be friends. Find pairs of users where both users are top likers of each other
  
    Note: you can use external libraries or predefined emoticon lists
    Note: build an UDF from your code for emoticon checking to use it with pyspark

 **Lab#2**
  
Develop application based on Apache Kafka + Apache Spark Streaming, which analyzes posts of vk users (https://cloud.mail.ru/public/29hd/4wctpKRwH), in the following way:

  ⁃ Posts must go to Kafka by a separate producer.
  - Divide the stream of posts into male and female
  - Divide streams by age - up to 18, 18-27, 27 -40, 40-60, 60 and more.
  - Divide streams into words, filters stop words.
  - Counts words with a window of 1 hour / day / week in each stream (Which category more active writes?).
  - Saves the result to Kafka and prints it to the console
  - Processed results must be read by a separate consumer
  - Use python, java or scala, DStreams or Structured Streaming.

PS Use the post time as the event time, simulating the temporal data stream. The minimum post time is the starting point of work, you can modify the post time thereby speeding up / slowing down the stream. 
PPS. Or you can completely randomize this process (it does not matter the time in the post, just read all posts and send data periodically to the Kafka)

**Lab#3**
  
You have to complete following steps:
  1. Design table schemas for all datasets into /nfs/shared/clickhouse_data/
  2. Insert these datasets in ClickHouse
  3. Create 2-3 materialized views (MVs)

Requirements:
  1. Partitioning and order key have to be reasonable.
  2. All tables have to be distributed.
  3. Data has to keep locality as much as possible.
  4. Data has to be inserted through a buffer. You are able to use built-in techniques or Kafka (using Kafka you’ll receive extra points).
MVs also have to be distributed.
