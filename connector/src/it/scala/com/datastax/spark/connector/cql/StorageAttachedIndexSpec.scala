package com.datastax.spark.connector.cql

import com.datastax.spark.connector.SparkCassandraITWordSpecBase
import com.datastax.spark.connector.cluster.DefaultCluster
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions._

class StorageAttachedIndexSpec extends SparkCassandraITWordSpecBase with DefaultCluster {
  override lazy val conn = CassandraConnector(defaultConf)

  override def beforeClass {
    conn.withSessionDo { session =>
      createKeyspace(session)

      session.execute(
        s"""CREATE TABLE IF NOT EXISTS $ks.types_test (
           |  pk int,
           |  text_col text,
           |  numeric_col date,
           |
           |  list_col list<text>,
           |  set_col set<text>,
           |
           |  map_col_1 map<int,int>,
           |  map_col_2 map<int,int>,
           |  map_col_3 map<int,int>,
           |
           |  PRIMARY KEY (pk));""".stripMargin)

      session.execute(
        s"""CREATE CUSTOM INDEX numeric_sai_idx ON $ks.types_test (numeric_col)
           |USING 'StorageAttachedIndex';""".stripMargin)
      session.execute(
        s"""CREATE CUSTOM INDEX text_sai_idx ON $ks.types_test (text_col)
           |USING 'StorageAttachedIndex'
           |WITH OPTIONS = {'case_sensitive': false, 'normalize': true };""".stripMargin)

      session.execute(
        s"""CREATE CUSTOM INDEX list_sai_idx ON $ks.types_test (list_col)
           |USING 'StorageAttachedIndex';""".stripMargin)
      session.execute(
        s"""CREATE CUSTOM INDEX set_sai_idx ON $ks.types_test (set_col)
           |USING 'StorageAttachedIndex';""".stripMargin)

      session.execute(
        s"""CREATE CUSTOM INDEX keys_map_sai_idx ON $ks.types_test (keys(map_col_1))
           |USING 'StorageAttachedIndex';""".stripMargin)
      session.execute(
        s"""CREATE CUSTOM INDEX values_map_sai_idx ON $ks.types_test (values(map_col_2))
           |USING 'StorageAttachedIndex';""".stripMargin)
      session.execute(
        s"""CREATE CUSTOM INDEX entries_map_sai_idx ON $ks.types_test (entries(map_col_3))
           |USING 'StorageAttachedIndex';""".stripMargin)


      session.execute(
        s"""CREATE TABLE IF NOT EXISTS $ks.pk_test (
           |  pk_1 int,
           |  pk_2 int,
           |  pk_3 int,
           |  ck_1 int,
           |  ck_2 int,
           |
           |  PRIMARY KEY ((pk_1, pk_2, pk_3), ck_1, ck_2));""".stripMargin)

      session.execute(
        s"""CREATE CUSTOM INDEX pk_1_sai_idx ON $ks.pk_test (pk_1)
           |USING 'StorageAttachedIndex';""".stripMargin)

      session.execute(
        s"""CREATE CUSTOM INDEX pk_3_sai_idx ON $ks.pk_test (pk_3)
           |USING 'StorageAttachedIndex';""".stripMargin)

      session.execute(
        s"""CREATE CUSTOM INDEX ck_2_sai_idx ON $ks.pk_test (ck_2)
           |USING 'StorageAttachedIndex';""".stripMargin)


      session.execute(
        s"""CREATE TABLE IF NOT EXISTS $ks.static_test (
           |  pk int,
           |  ck int,
           |  static_col_1 int STATIC,
           |  static_col_2 int STATIC,
           |
           |  PRIMARY KEY (pk, ck));""".stripMargin)

      session.execute(
        s"""CREATE CUSTOM INDEX static_sai_idx ON $ks.static_test (static_col_1)
           |USING 'StorageAttachedIndex';""".stripMargin)
    }
  }

  private def df(table: String): DataFrame = spark.read.cassandraFormat(table, ks).load()

  private def assertPushDown(dataFrame: DataFrame): Unit = {
    val plan = dataFrame.queryExecution.sparkPlan.toString()
    withClue("The given plan should not contain Filter element, some of the predicates were not pushed down") {
      plan should not include "Filter "
    }
    dataFrame.collect()
  }

  private def assertNoPushDown(dataFrame: DataFrame): Unit = {
    val plan = dataFrame.queryExecution.sparkPlan.toString()
    withClue("The given plan should contain Filter element, some of the predicates should not be pushed down") {
      plan should include("Filter ")
    }
    dataFrame.collect()
  }

  "Index on numeric column" should {
    "allow for equality, lt and gt predicate push down" in {
      assertPushDown(df("types_test").filter(col("numeric_col") >= "2006-05-15"))
      assertPushDown(df("types_test").filter(col("numeric_col") > "2006-05-15"))
      assertPushDown(df("types_test").filter(col("numeric_col") < "2006-05-15"))
      assertPushDown(df("types_test").filter(col("numeric_col") <= "2006-05-15"))
      assertPushDown(df("types_test").filter(col("numeric_col") === "2006-05-15"))
    }
  }

  "Index on text column" should {
    "allow for equality predicate push down" in {
      assertPushDown(df("types_test").filter(col("text_col") === "Eppinger"))
    }

    "allow for contains predicate push down" in {
      assertPushDown(df("types_test").filter(col("text_col") contains "ing"))
    }
  }

  "Index on list column" should {
    "allow for contains predicate push down" in {
      assertPushDown(df("types_test").filter(array_contains(col("list_col"), "AA")))
    }

    "not allow for equality predicate push down" in {
      // TODO: we may consider pushing down contains (one or more) predicate and instruct spark to do equality afterwards, same thing applies for sets
      assertPushDown(df("types_test").filter(col("list_col") === Array("AA", "BB")))
    }
  }

  "Index on set column" should {
    "allow for contains predicate push down" in {
      assertPushDown(df("types_test").filter(array_contains(col("set_col"), "AA")))
    }

    "not allow for equality predicate push down" in {
      assertNoPushDown(df("types_test").filter(col("set_col") === Array("BB, AA")))
    }
  }

  "Index on map keys" should {
    "allow for contains predicate push down on keys" in {
      // SELECT * from test_storage_attached_index_spec.types_test WHERE map_col_1 CONTAINS KEY 2;
      assertPushDown(df("types_test").filter(array_contains(map_keys(col("map_col_1")), 2)))
    }

    "not allow for contains predicate push down on values" in {
      assertNoPushDown(df("types_test").filter(array_contains(map_values(col("map_col_1")), 2)))
    }

    "not allow for equality predicate push down" in {
      assertNoPushDown(df("types_test").filter(col("map_col_1").getItem(5) === 4))
    }
  }

  "Index on map values" should {
    "allow for contains predicate push down on values" in {
      // SELECT * from test_storage_attached_index_spec.types_test WHERE map_col_2 CONTAINS 2;
      assertPushDown(df("types_test").filter(array_contains(map_values(col("map_col_2")), 2)))
    }

    "not allow for contains predicate push down on keys" in {
      assertNoPushDown(df("types_test").filter(array_contains(map_keys(col("map_col_2")), 2)))
    }

    "not allow for equality predicate push down" in {
      assertNoPushDown(df("types_test").filter(col("map_col_2").getItem(5) === 4))
    }
  }

  "Index on map entries" should {
    "allow for equality predicate push down" in {
      // SELECT * from test_storage_attached_index_spec.types_test WHERE map_col_3[5] = 4;
      assertPushDown(df("types_test").filter(col("map_col_3").getItem(5) === 4))
    }

    "not allow for predicate push down different than equality" in {
      assertNoPushDown(df("types_test").filter(col("map_col_3").getItem(5) > 3))
      assertNoPushDown(df("types_test").filter(col("map_col_3").getItem(5) >= 3))
    }

    "not allow for contains predicate push down on keys" in {
      assertNoPushDown(df("types_test").filter(array_contains(map_keys(col("map_col_3")), 2)))
    }

    "not allow for contains predicate push down on values" in {
      assertNoPushDown(df("types_test").filter(array_contains(map_values(col("map_col_3")), 2)))
    }
  }

  "Index on partition key columns" should {
    "allow for predicate push down for indexed parts of the partition key" in {
      assertPushDown(df("pk_test").filter(col("pk_1") === 1))
      assertPushDown(df("pk_test").filter(col("pk_3") === 1))
    }

    "not allow for predicate push down for non-indexed parts of the partition key" in {
      assertNoPushDown(df("pk_test").filter(col("pk_2") === 1))
    }
  }

  "Index on clustering key columns" should {
    "allow for predicate push down for indexed parts of the clustering key" in {
      assertPushDown(df("pk_test").filter(col("ck_2") === 1))
    }

    "not allow for predicate push down for non-indexed parts of the clustering key" in {
      assertNoPushDown(df("pk_test").filter(col("ck_1") === 1))
    }
  }

  "Index on static columns" should {
    "allow for predicate push down" in {
      assertPushDown(df("static_test").filter(col("static_col_1") === 1))
    }

    "not cause predicate push down for non-indexed static columns" in {
      assertNoPushDown(df("static_test").filter(col("static_col_2") === 1))
    }
  }
}
