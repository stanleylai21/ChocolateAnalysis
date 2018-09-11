/**
 * Author: Milaud Nik-Ahd, Stanley Lai
 * CS185C - Final Project
 */

package spring2018.project;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;



// command to run
/*
/opt/mapr/spark/spark-2.1.0/bin/spark-submit --master local[*] --class spring2018.project.SparkChocolate CS185-jar-with-dependencies.jar /user/user01/PROJECT/flavors_of_cacao.csv /user/user01/PROJECT/sparkout
*/
public class SparkChocolate {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		
		if (args.length != 2) {
			System.err.println("usage: SparkAA <input-file> <output-dir>");
			System.exit(1);
		}
		String inputFile = args[0];
		String outputDir = args[1];
		
		SparkSession spark = SparkSession
				  .builder()
				  .appName("Java Spark SQL Flavors of Cacao")
				  .config("spark.some.config.option", "some-value")
				  .getOrCreate();
		
		SQLContext sqlContext = new SQLContext(spark);
		
		// Column Names:
		// Company,"SpecificBeanOrigin",REF,"ReviewDate","CocoaPercent","Location",Rating,"BeanType","BroadBeanOrigin"

		Dataset<Row> df = sqlContext.read().format("com.databricks.spark.csv")
				.option("sep", ",")
				//.schema(customSchema)
				.option("inferSchema", "true")
				.option("header", "true")
				.load(inputFile);
		
		df.registerTempTable("Chocolates");
		
		
		//df.show();
		
		// show schema
		df.printSchema();
		
		/*
		 *  What is the most popular chocolate bar in <some country>?
			What is the most popular bean type?
			Which company makes the most popular chocolate bar?
			Which cacao percentage are most popular in certain areas of the world?
			Where do the most popular beans arise from in the world?
		 */
		
		// ----------------------------- Keep these queries -------------------------------------------------------
		
		// Best Chocolate Bar company in each country
		Dataset<Row> q1 = sqlContext.sql("SELECT first(Company), Max(Rating), Location FROM Chocolates Group By Location Order By Location Asc");
		q1.show((int) q1.count(), false);
		//q1.write().format("csv").save(outputDir + "_q1");
		
		// Best Bean Origin Ratings (change to BroadBeanOrigin?
		Dataset<Row> q2 = sqlContext.sql("SELECT SpecificBeanOrigin, Rating FROM Chocolates Order By Rating Desc");
		q2.show(30, false);
		//q2.write().format("csv").save(outputDir + "_q2");
		
		// Top rated chocolate bars in the United States
		Dataset<Row> q3 = sqlContext.sql("SELECT Company, Rating FROM Chocolates WHERE Location='United States' Order By Rating Desc");
		q3.show((int) q3.count(), false);
		//q3.write().format("csv").save(outputDir + "_q3");
		
		//**ToDo: FIX Q4**
		// Which cocoa percentage are most popular in certain areas of the world?
		//Dataset<Row> q4 = sqlContext.sql("SELECT first(SpecificBeanOrigin), first(CocoaPercent), Rating FROM Chocolates Group By Location Order By Rating Desc");
		//q4.show(40, false);
		//q4.write().format("csv").save(outputDir + "_q4");
		
		// Where do the most popular beans arise from in the world?
		// Add distinct tag to SpecificBeanOrigin?
		Dataset<Row> q5 = sqlContext.sql("SELECT SpecificBeanOrigin, Rating FROM Chocolates Order By Rating Desc");
		q5.show(40, false);
		//q5.write().format("csv").save(outputDir + "_q5");
		
		
		// ------------------------------------------ New Queries -----------------------------------------------------
		
		
		//What is the Chocolate Bar each Company produces? Done
		Dataset<Row> q6 = sqlContext.sql("SELECT first(Company), first(SpecificBeanOrigin), Max(Rating) FROM Chocolates Group By Company");
		q6.show((int) q6.count(), false);
		
		//Which Company makes the best bar Maximum
		Dataset<Row> q7 = sqlContext.sql("SELECT first(Company), Max(Rating) FROM Chocolates Group By Company Order By Max(Rating) Desc");
		q7.show(10, false); //Done
		
		//Average rating of all the chocolate bars from a company
		Dataset<Row> q7Avg = sqlContext.sql("SELECT first(Company), Avg(Rating) FROM Chocolates Group By Company Order By Avg(Rating) Desc");
		q7Avg.show(10, false); //Done
		
		// Count all the chocolate bars made in every country (Visualization)
		Dataset<Row> q8 = sqlContext.sql("SELECT BroadBeanOrigin, count(BroadBeanOrigin) FROM Chocolates Group By BroadBeanOrigin Order By Count(BroadBeanOrigin) Desc");
		q8.show(27, false); // Get rid of null value
		q8.coalesce(1).write().format("csv").save(outputDir + "_q8");
		
		//Where do the best beans arise from?
		//Rating, Bean type, Broad Bean Origin
		// Can't use bean type, lots of null values in beantype
		Dataset<Row> q9 = sqlContext.sql("SELECT first(BroadBeanOrigin), Max(Rating) FROM Chocolates Group By BroadBeanOrigin Order By Max(Rating) Desc");
		q9.show(27, false); //Done
		
		//Where are the best chocolate bars manufactured? (Max) (Visualization)
		Dataset<Row> q10 = sqlContext.sql("SELECT first(Location), Max(Rating) FROM Chocolates Group By Location Order By Max(Rating) Desc");
		q10.show(20, false); //Done
		
		//Where are the best chocolate bars manufactured? (Avg) (Visualization)
		Dataset<Row> q10Avg = sqlContext.sql("SELECT first(Location), Avg(Rating) FROM Chocolates Group By Location Order By Avg(Rating) Desc");
		q10Avg.show(20, false); //Done 
		
		//What is the most popular bean type in each country(where company is located)?
		//Bean Type, Company/bbo?
		// Lots of nulls, skip this one
		///Dataset<Row> q11 = sqlContext.sql("SELECT first(BeanType), first(Location), Max(Rating) FROM Chocolates Group By Location Order By Location Asc");
		//q11.show((int) q11.count()); //Check

		//What is best dark chocolate bar? (70-100% cocoa)
		Dataset<Row> q12 = sqlContext.sql("SELECT BroadBeanOrigin, CocoaPercent, Rating FROM Chocolates WHERE CocoaPercent Between 0.70 AND 1 Order By Rating Desc");
		q12.show(10, false);

		//Best low/medium cocoa bar? (20-69% Cocoa)
		Dataset<Row> q13 = sqlContext.sql("SELECT BroadBeanOrigin, CocoaPercent, Rating FROM Chocolates WHERE CocoaPercent Between 0.20 AND 0.69 Order By Rating Desc");
		q13.show(10, false);	
		
		//Best Avg Ratings for companies that source their beans from far away countries and the location of the company
		Dataset<Row> q14 = sqlContext.sql("SELECT first(Company), first(SpecificBeanOrigin), Avg(Rating) FROM Chocolates Group By Company Order By Avg(Rating) Desc");
		q14.show(20, false);
		
		//Worse Avg Ratings for companies that source their beans from far away countries and the location of the company
		Dataset<Row> q15 = sqlContext.sql("SELECT first(Company), first(SpecificBeanOrigin), Avg(Rating) FROM Chocolates Group By Company Order By Avg(Rating) Asc");
		q15.show(20, false);
		
		spark.stop();
		
	}


	
}
