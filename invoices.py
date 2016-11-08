#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import division
from __future__ import print_function

from pyspark.sql import SparkSession
# $example on:schema_merging$
from pyspark.sql import Row
# $example off:schema_merging$



import plotly.graph_objs as go
import plotly.plotly as py
from plotly.tools import FigureFactory as FF
import numpy as np
import plotly.tools as tls
tls.set_credentials_file(username='chihebbha', api_key='ufoo177icv')



"""
A simple example demonstrating Spark SQL data sources.
Run with:
  ./bin/spark-submit examples/src/main/python/sql/datasource.py
"""


# def basic_datasource_example(spark):
#     # $example on:generic_load_save_functions$
#     df = spark.read.load("examples/src/main/resources/users.parquet")
#     df.select("name", "favorite_color").write.save("namesAndFavColors.parquet")
#     # $example off:generic_load_save_functions$
#
#     # $example on:manual_load_options$
#     df = spark.read.load("examples/src/main/resources/people.json", format="json")
#     df.select("name", "age").write.save("namesAndAges.parquet", format="parquet")
#     # $example off:manual_load_options$
#
#     # $example on:direct_sql$
#     df = spark.sql("SELECT * FROM parquet.`examples/src/main/resources/users.parquet`")
#     # $example off:direct_sql$
#
#
# def parquet_example(spark):
#     # $example on:basic_parquet_example$
#     peopleDF = spark.read.json("examples/src/main/resources/people.json")
#
#     # DataFrames can be saved as Parquet files, maintaining the schema information.
#     peopleDF.write.parquet("people.parquet")
#
#     # Read in the Parquet file created above.
#     # Parquet files are self-describing so the schema is preserved.
#     # The result of loading a parquet file is also a DataFrame.
#     parquetFile = spark.read.parquet("people.parquet")
#
#     # Parquet files can also be used to create a temporary view and then used in SQL statements.
#     parquetFile.createOrReplaceTempView("parquetFile")
#     teenagers = spark.sql("SELECT name FROM parquetFile WHERE age >= 13 AND age <= 19")
#     teenagers.show()
#     # +------+
#     # |  name|
#     # +------+
#     # |Justin|
#     # +------+
#     # $example off:basic_parquet_example$
#
#
# def parquet_schema_merging_example(spark):
#     # $example on:schema_merging$
#     # spark is from the previous example.
#     # Create a simple DataFrame, stored into a partition directory
#     sc = spark.sparkContext
#
#     squaresDF = spark.createDataFrame(sc.parallelize(range(1, 6))
#                                       .map(lambda i: Row(single=i, double=i ** 2)))
#     squaresDF.write.parquet("data/test_table/key=1")
#
#     # Create another DataFrame in a new partition directory,
#     # adding a new column and dropping an existing column
#     cubesDF = spark.createDataFrame(sc.parallelize(range(6, 11))
#                                     .map(lambda i: Row(single=i, triple=i ** 3)))
#     cubesDF.write.parquet("data/test_table/key=2")
#
#     # Read the partitioned table
#     mergedDF = spark.read.option("mergeSchema", "true").parquet("data/test_table")
#     mergedDF.printSchema()
#
#     # The final schema consists of all 3 columns in the Parquet files together
#     # with the partitioning column appeared in the partition directory paths.
#     # root
#     #  |-- double: long (nullable = true)
#     #  |-- single: long (nullable = true)
#     #  |-- triple: long (nullable = true)
#     #  |-- key: integer (nullable = true)
#     # $example off:schema_merging$
#
#
def json_dataset_example(spark):
    # $example on:json_dataset$
    # spark is from the previous example.
    sc = spark.sparkContext

    # A JSON dataset is pointed to by path.
    # The path can be either a single text file or a directory storing text files
    path = "population.json"
    invoiceDF = spark.read.json(path)

    # The inferred schema can be visualized using the printSchema() method

   # invoiceDF.printSchema()

    # root
    #  |-- age: long (nullable = true)
    #  |-- name: string (nullable = true)

    # Creates a temporary view using the DataFrame
    invoiceDF.createOrReplaceTempView("pop")

    # SQL statements can be run by using the sql methods provided by spark
    invoiceDF = spark.sql("SELECT population.date,population.value,population.homme,population.femme from pop")

    datesDS = invoiceDF.rdd.map(lambda row:  row.date)
    for dates in datesDS.collect():
        dates=dates
    clientsDS = invoiceDF.rdd.map(lambda row:  row.value)
    somme=0
    difference=0;
    for client in clientsDS.collect():
        for i in range(len(client)):
                if i != len(client)-1:
                 difference=difference+(client[i]-client[i+1])

                somme = somme+client[i]

                sommes = somme/len(client)
    difference=difference/len(client)



    #__________________________________________________________________________________________________________________
    clientsDSH = invoiceDF.rdd.map(lambda row: row.homme)
    sommeH = 0
    differenceH = 0;
    for clientH in clientsDSH.collect():
        for i in range(len(clientH)):
            if i != len(clientH) - 1:
                differenceH = differenceH + (clientH[i] - clientH[i + 1])

            sommeH = sommeH + clientH[i]

            sommesH = sommeH / len(clientH)
    differenceH = differenceH / len(clientH)

    #__________________________________________________________________________________________________________________

    clientsDSF = invoiceDF.rdd.map(lambda row: row.femme)
    sommeF = 0
    differenceF = 0;
    for clientF in clientsDSF.collect():
        for i in range(len(clientF)):
            if i != len(clientF) - 1:
                differenceF = differenceF + (clientF[i] - clientF[i + 1])

            sommeF = sommeF + clientF[i]

            sommesF = sommeF / len(clientF)
    differenceF = differenceF / len(clientF)

    # __________________________________________________________________________________________________________________



                # A JSON dataset is pointed to by path.
        # The path can be either a single text file or a directory storing text files
    path1 = "test.json"
    invoiceDF1 = spark.read.json(path1)

             # The inferred schema can be visualized using the printSchema() method

                  # invoiceDF.printSchema()

             # root
                  #  |-- age: long (nullable = true)
        #  |-- name: string (nullable = true)

        # Creates a temporary view using the DataFrame
    invoiceDF1.createOrReplaceTempView("pop1")

        # SQL statements can be run by using the sql methods provided by spark
    invoiceDF1 = spark.sql("SELECT population.annee,population.valeur1,population.valeur2 from pop1")
       # invoiceDF1.show()

    clientsDS1 = invoiceDF1.rdd.map(lambda row: row.valeur1)
    somme1=0
    for client1 in clientsDS1.collect():

        for i in range(len(client1)):
         somme1=somme1+client1[i]
    moy1=somme1/len(client1)
    resultat=(moy1+client1[len(client1)-1])/2


    clientsDS2 = invoiceDF1.rdd.map(lambda row: row.valeur2)
    somme2 = 0
    for client2 in clientsDS2.collect():

        for j in range(len(client2)):
            somme2 = somme2 + client2[j]
    moy2 = somme2 / len(client2)
    resultat2 = (moy2 + client2[len(client2) - 1]) / 2

    total=difference + sommes + client[0] * 500
    totalH=differenceH + sommesH + clientH[0] * 500
    totalF=differenceF + sommesF + clientF[0] * 500
    print("______________________________________________________________________________________________________________________________________")
    print("le nombre de population total entre 10 et 14 ans en 2020 est : ", total)
    print("le nombre de garcons entre 10 et 14 ans en 2020 est : ", totalH,"  =======>     %.3f"% round((totalH/total)*100,3)," %")
    print("le nombre de filles entre 10 et 14 ans en 2020 est : ", totalF,"  =======>     %.3f"% round((totalF/total)*100,3)," %")
    print("le nombre des candidat en 2020 est : ", resultat * 1000)
    print("le nombre des candidat admis en 2020 est : ", resultat2 * 1000)
    print("______________________________________________________________________________________________________________________________________")

    data = [
        go.Scatter(  # all "scatter" attributes: https://plot.ly/python/reference/#scatter
            x=dates,  # more about "x":  /python/reference/#scatter-x
            y=client,  # more about "y":  /python/reference/#scatter-y
            marker=dict(  # marker is an dict, marker keys: /python/reference/#scatter-marker
                color="rgb(16, 32, 77)"  # more about marker's "color": /python/reference/#scatter-marker-color
            )
        )
    ]

    layout = go.Layout(  # all "layout" attributes: /python/reference/#layout
        title="population entre 10 et 14 ans en tunisie",  # more about "layout's" "title": /python/reference/#layout-title
        xaxis=dict(  # all "layout's" "xaxis" attributes: /python/reference/#layout-xaxis
            title="time"  # more about "layout's" "xaxis's" "title": /python/reference/#layout-xaxis-title
        ),
        annotations=[
            dict(  # all "annotation" attributes: /python/reference/#layout-annotations
                text="simple annotation",  # /python/reference/#layout-annotations-text
                x=0,  # /python/reference/#layout-annotations-x
                xref="paper",  # /python/reference/#layout-annotations-xref
                y=0,  # /python/reference/#layout-annotations-y
                yref="paper"  # /python/reference/#layout-annotations-yref
            )
        ]
    )

    figure = go.Figure(data=data, layout=layout)

    py.plot(figure, filename='graph')







    # x1 = client
    # x2 = clientH
    # x3 = clientF
    #
    # hist_data = [x1, x2, x3]
    #
    # group_labels = ['Population', 'Population Homme', 'Population Femme']
    # colors = ['#333F44', '#37AA9C', '#94F3E4']
    #
    # # Create distplot with curve_type set to 'normal'
    # fig = FF.create_distplot(hist_data, group_labels, show_hist=False, colors=colors)
    #
    # # Add title
    # fig['layout'].update(title='Popoulation en Tunisie')
    #
    # # Plot!
    # py.iplot(fig, filename='Popolation en Tunisie', validate=False)
    # invoiceDFa = spark.sql("SELECT population.value from pop")
        # invoiceDFa.show()
        #
        # clientsDSa = invoiceDF.rdd.map(lambda row: "value:%s" % row.date)
        #
        # for client in clientsDSa.collect():
        #     print(client)




    #


    # +------+
    # |  name|
    # +------+
    # |Justin|
    # +------+

    # Alternatively, a DataFrame can be created for a JSON dataset represented by
    # an RDD[String] storing one JSON object per string
    # jsonStrings = ['{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}']
    # otherPeopleRDD = sc.parallelize(jsonStrings)
    # otherPeople = spark.read.json(otherPeopleRDD)
    # otherPeople.show()
    # +---------------+----+
    # |        address|name|
    # +---------------+----+
    # |[Columbus,Ohio]| Yin|
    # +---------------+----+
    # $example off:json_dataset$


# def jdbc_dataset_example(spark):
#     # $example on:jdbc_dataset$
#     # Note: JDBC loading and saving can be achieved via either the load/save or jdbc methods
#     # Loading data from a JDBC source
#     jdbcDF = spark.read \
#         .format("jdbc") \
#         .option("url", "jdbc:postgresql:dbserver") \
#         .option("dbtable", "schema.tablename") \
#         .option("user", "username") \
#         .option("password", "password") \
#         .load()
#
#     jdbcDF2 = spark.read \
#         .jdbc("jdbc:postgresql:dbserver", "schema.tablename",
#               properties={"user": "username", "password": "password"})
#
#     # Saving data to a JDBC source
#     jdbcDF.write \
#         .format("jdbc") \
#         .option("url", "jdbc:postgresql:dbserver") \
#         .option("dbtable", "schema.tablename") \
#         .option("user", "username") \
#         .option("password", "password") \
#         .save()
#
#     jdbcDF2.write \
#         .jdbc("jdbc:postgresql:dbserver", "schema.tablename",
#               properties={"user": "username", "password": "password"})
#     # $example off:jdbc_dataset$
#

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL data source example") \
        .getOrCreate()

    # basic_datasource_example(spark)
    # parquet_example(spark)
    # parquet_schema_merging_example(spark)
    json_dataset_example(spark)
    # jdbc_dataset_example(spark)

    spark.stop()