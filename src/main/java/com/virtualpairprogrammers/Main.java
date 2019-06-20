package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

public class Main {

	public static void main(String[] args) {

		System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkSession spark = SparkSession.builder().appName("GymCompetitors")
				.config("spark.sql.warehouse.dir", "file:///c:/tmp/").master("local[*]").getOrCreate();

		Dataset<Row> csvData = spark.read().option("header", true).csv("src/main/resources/GymCompetition.csv");

		csvData.printSchema();

		VectorAssembler vectorAssembler = new VectorAssembler();
		vectorAssembler.setInputCols(new String[] { "Age", "Height", "Weight" });
		vectorAssembler.setOutputCol("features");
		Dataset<Row> csvDataWithFeatures = vectorAssembler.transform(csvData);

		Dataset<Row> modelInputData = csvDataWithFeatures.select("NoOfReps", "features").withColumnRenamed("NoOfReps",
				"label");
		modelInputData.show();

	}
}