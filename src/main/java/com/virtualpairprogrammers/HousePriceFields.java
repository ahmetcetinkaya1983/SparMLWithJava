package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HousePriceFields {
	

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkSession spark = SparkSession.builder()
				.appName("House Price Analysis")
				.config("spark.sql.warehouse.dir", "file:///c:/tmp/")
				.master("local[*]").getOrCreate();

		Dataset<Row> csvData = spark.read()
				.option("header", true)
				.option("inferSchema", true)
				.csv("src/main/resources/kc_house_data.csv");
	
		//csvData.describe().show();	
		
		//remove unuseful columns
		csvData = csvData.drop("id", "date", "waterfront", "view", "condition", "grade", "yr_renovated", "zipcode", "lat", "long");
		
		//correlation close to 1 or -1 is high and close to 0 is less
		for(String col: csvData.columns()) {
			
			System.out.println("The correlation between price and " +col+" is : "+csvData.stat().corr("price", col));
		}
		
		//drop low correlated columns/features
		csvData = csvData.drop("sqft_lot", "sqft_lot", "yr_built", "sft_living15");
		
		//calculate correlation for each column with another
		
		for(String col1 : csvData.columns()) {
			for(String col2 : csvData.columns()) {
				System.out.println("The correlation between "+col1+ " and " +col2+" is : "+csvData.stat().corr(col1, col2));
			}
		}
		
//		Data Preparation Steps
//		1- Acquisition
//		2- Data Cleaning
//		3- Feature Selection
//		4-Data formatting
		
		
	}
	
	

}
