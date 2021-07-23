/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package wazaf;

/**
 *
 * @author shehab
 */


import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;



public class DataSet {
    private static final String COMMA_DELIMITER = ",";
    private Dataset<Row> jobsDF;
    private Dataset<Row> jobsDF_cleaned;
    
    public void intializeSpark_ml(){
        SparkSession sparkSession= SparkSession.builder()
                .appName("wazaf_jobs")
                .master("local[4]")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();
        DataFrameReader dataFrameReader= sparkSession.read();
        dataFrameReader.option("header", true);
        this.jobsDF = dataFrameReader.csv("src/main/resources/Wuzzuf_Jobs.csv");
    }
    
    public void showData(int n){
        this.jobsDF.show(n);
        this.jobsDF.printSchema();

    }
    
    public void clean(){
        // convert df DataSet<Row> -> RDD<vector 
        this.jobsDF_cleaned = this.jobsDF.na().drop();
        this.jobsDF_cleaned = this.jobsDF_cleaned.distinct();
        System.out.println("distinct count :"+ this.jobsDF_cleaned.count());
        System.out.println("Original data count : "+ this.jobsDF.count());
        this.jobsDF_cleaned.describe().show();
        
    }
    
    
    
    
}
