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




import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import org.knowm.xchart.BitmapEncoder;
import org.knowm.xchart.PieChart;
import org.knowm.xchart.PieChartBuilder;
import org.knowm.xchart.style.Styler;

import java.io.IOException;
import java.util.List;

import static org.apache.spark.sql.functions.col;


public class DataSet {
    private static final String COMMA_DELIMITER = ",";
    private Dataset<Row> jobsDF;
    
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


    public void clean(){
        // convert df DataSet<Row> -> RDD<vector
        this.jobsDF = this.jobsDF.na().drop();
        this.jobsDF = this.jobsDF.distinct();
        System.out.println("distinct count :"+ this.jobsDF.count());
        System.out.println("Original data count : "+ this.jobsDF.count());
        this.jobsDF.describe().show();

    }


    public void showData(int n){
        this.jobsDF.show(n);
        this.jobsDF.printSchema();

    }

    // and display some from it.
    public List<Row> Head(int n)
    {
        List<Row> head = jobsDF.limit(n).collectAsList();
        return head;
    }


    // 2. Display structure and summary of the data.
    public String getStructure() throws JsonProcessingException
    {
        StructType structure = jobsDF.schema();
        return structure.prettyJson();
    }

    public  String getSummary()
    {
        Dataset<Row> str = jobsDF.summary();
        List<Row> SummaryList = str.limit(10).collectAsList();
        return SummaryList;
    }

//4. Count the jobs for each company and display that in order
//            (What are the most demanding companies for jobs?
    //5. Show step 4 in a pie chart


    public String getCompanyPieChart() throws IOException
    {
        Dataset<Row> groupedByCompany = jobsDF.groupBy("Company")
                .count()
                .orderBy(col("count").desc())
                .limit(10);

        List<String> companies = groupedByCompany.select("Company").as(Encoders.STRING()).collectAsList();
        List<String> counts = groupedByCompany.select("count").as(Encoders.STRING()).collectAsList();

        PieChart chart = new PieChartBuilder().width(1400).height(700).title("Companies Pie-Chart").build();
        chart.getStyler().setLegendPosition(Styler.LegendPosition.OutsideS);
        chart.getStyler().setLegendLayout(Styler.LegendLayout.Horizontal);

        for (int i = 0; i < companies.size() ; i++)
            chart.addSeries(companies.get(i), Integer.parseInt(counts.get(i)));

        BitmapEncoder.saveBitmap(chart, "src\\main\\resources\\company_pie_chart.png", BitmapEncoder.BitmapFormat.PNG);
        return "src\\main\\resources\\company_pie_chart.png";

    }

//    6. Find out What are it the most popular job titles?
//            7. Show step 6 in bar chart

    public String jobsForCompany() throws IOException
    {
        Dataset<Row> groupedByCompany = jobsDF.groupBy("Title")
                .count()
                .orderBy(col("count").desc())
                .limit(10);
        List<String> titles = groupedByCompany.select("Title").as(Encoders.STRING()).collectAsList();
        List<String> counts = groupedByCompany.select("count").as(Encoders.STRING()).collectAsList();

        PieChart chart = new PieChartBuilder().width(1400).height(700).title("Titles Pie-Chart").build();
        chart.getStyler().setLegendPosition(Styler.LegendPosition.OutsideE);
        chart.getStyler().setLegendLayout(Styler.LegendLayout.Vertical);

        for (int i = 0; i < titles.size() ; i++)
            chart.addSeries(titles.get(i), Integer.parseInt(counts.get(i)));

        BitmapEncoder.saveBitmap(chart, "src\\main\\resources\\title_pie_chart.png", BitmapEncoder.BitmapFormat.PNG);
        return "src\\main\\resources\\title_pie_chart.png";
    }


//8. Find out the most popular areas?
//            9. Show step 8 in bar chart



//    10. Print skills one by one and how many each repeated and  order the output to find out the most important skills
//    required?



//    11. Factorize the YearsExp feature and convert it to numbers
//    in new col. (Bounce )



//    12. Apply K-means for job title and companies (Bounce )



}
