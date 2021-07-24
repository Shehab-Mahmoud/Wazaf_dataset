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
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.knowm.xchart.*;
import org.knowm.xchart.style.Styler;


import java.util.*;
import java.io.IOException;

import static org.apache.spark.sql.functions.col;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;


public class DataSet {

    SparkSession sparkSession= SparkSession.builder()
            .appName("wazaf_jobs")
            .master("local[*]")
            .config("spark.some.config.option", "some-value")
            .getOrCreate();
    String path = "src\\main\\resources\\Wuzzuf_Jobs.csv";
    private Dataset<Row> jobsDF = sparkSession.read().option("header", true).csv(path);

    public void clean(){
        this.jobsDF = this.jobsDF.na().drop();
        this.jobsDF = this.jobsDF.dropDuplicates().filter((FilterFunction<Row>) row -> !row.get(5).equals("null Yrs of Exp"));

    }


//    public void showData(int n){
//        this.jobsDF.show(n);
//        this.jobsDF.printSchema();
//
//    }

    // and display some from it.
    public List<Row> head(int n)
    {
        List<Row> headValues = jobsDF.limit(n).collectAsList();
        return headValues;
    }


    // 2. Display structure and summary of the data.
    public String getStructure() throws JsonProcessingException
    {
        StructType structure = jobsDF.schema();
        return structure.prettyJson();
    }

    public  List<Row> getSummary()
    {
        Dataset<Row> str = jobsDF.summary();
        List<Row> SummaryList = str.collectAsList();
        return SummaryList;
    }

//4. Count the jobs for each company and display that in order
//            (What are the most demanding companies for jobs?
    //5. Show step 4 in a pie chart




    public String plotCompanyPieChart(int n) throws IOException
    {
        Dataset<Row> groupedByCompany = jobsDF.groupBy("Company")
                .count()
                .orderBy(col("count").desc())
                .limit(n);

        List<String> companies = groupedByCompany.select("Company").as(Encoders.STRING()).collectAsList();
        List<String> counts = groupedByCompany.select("count").as(Encoders.STRING()).collectAsList();

        PieChart chart = new PieChartBuilder().width(1400).height(700).title("Companies Pie-Chart").build();
        chart.getStyler().setLegendPosition(Styler.LegendPosition.OutsideS);
        chart.getStyler().setLegendLayout(Styler.LegendLayout.Horizontal);

        for (int i = 0; i < companies.size() ; i++)
            chart.addSeries(companies.get(i), Integer.parseInt(counts.get(i)));

        BitmapEncoder.saveBitmap(chart, "src/main/resources/company_pie_chart.png", BitmapEncoder.BitmapFormat.PNG);
        return "src/main/resources/company_pie_chart.png";

    }

    public List<Row> getMostDemandingCompanies(int n)
    {
        Dataset<Row> groupedByCompany = jobsDF.groupBy("Company")
                .count()
                .orderBy(col("count").desc())
                .limit(n);
        List<Row> mostDemandingCompanies = groupedByCompany.collectAsList();

        return mostDemandingCompanies;
    }

//    6. Find out What are it the most popular job titles?
//            7. Show step 6 in bar chart

    public String jobsForCompany(int n) throws IOException
    {
        Dataset<Row> groupedByCompany = jobsDF.groupBy("Title")
                .count()
                .orderBy(col("count").desc())
                .limit(n);
        List<String> titles = groupedByCompany.select("Title").as(Encoders.STRING()).collectAsList();
        List<String> counts = groupedByCompany.select("count").as(Encoders.STRING()).collectAsList();

        PieChart chart = new PieChartBuilder().width(1400).height(700).title("Titles Pie-Chart").build();
        chart.getStyler().setLegendPosition(Styler.LegendPosition.OutsideE);
        chart.getStyler().setLegendLayout(Styler.LegendLayout.Vertical);

        for (int i = 0; i < titles.size() ; i++)
            chart.addSeries(titles.get(i), Integer.parseInt(counts.get(i)));

        BitmapEncoder.saveBitmap(chart, "src/main/resources/title_pie_chart.png", BitmapEncoder.BitmapFormat.PNG);
        return "src/main/resources/title_pie_chart.png";
    }


//              8. Find out the most popular areas?
//            9. Show step 8 in bar chart

    public String plotAreaBarChart(int n) throws IOException
    {
        Dataset<Row> groupByLocations = jobsDF.groupBy("Location")
                .count()
                .orderBy(col("count").desc())
                .limit(n);

        List<String> Areas = groupByLocations.select("Location").as(Encoders.STRING()).collectAsList();
        List<String> counted = groupByLocations.select("count").as(Encoders.STRING()).collectAsList();
        List<Float> counting = new ArrayList<>();

        for(String s : counted)
            counting.add(Float.valueOf(s));

        CategoryChart charts = new CategoryChartBuilder().width (1400).height (700).title ("Locations Bar-chart").xAxisTitle("Locations").yAxisTitle("frequency").build();
        charts.getStyler().setLegendPosition(Styler.LegendPosition.OutsideS);
        charts.getStyler().setLegendLayout(Styler.LegendLayout.Horizontal);
        charts.getStyler().setHasAnnotations(true);
        charts.getStyler().setStacked(true);
        charts.addSeries("Locations", Areas, counting);

        BitmapEncoder.saveBitmap(charts, "src/main/resources/Areas_Bar_chart.png", BitmapEncoder.BitmapFormat.PNG);
        return "src/main/resources/Areas_Bar_chart.png";
    }


//    10. Print skills one by one and how many each repeated and  order the output to find out the most important skills
//    required?


        public List<Map.Entry> getMostDemandedSkills()
    {
        JavaRDD<String> skillByRow = jobsDF.select("Skills").as(Encoders.STRING()).javaRDD();
        JavaRDD<String>  skills = skillByRow.flatMap(skill ->
            Arrays.asList(skill
                    .toLowerCase()
                    .trim()
                    .split(",")).iterator());


        List<Map.Entry> skillsCounts = skills
                .countByValue()
                .entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue())
                .collect(Collectors.toList());
        Collections.reverse(skillsCounts);

        return skillsCounts;
    }


//    11. Factorize the YearsExp feature and convert it to numbers
//    in new col. (Bounce )

    public List<Row> getFactorizedYearsOfExp(int n)
    {
        Dataset<Row> factorizedYears = new StringIndexer()
                .setInputCol("YearsExp")
                .setOutputCol("FactorizedYears")
                .fit(jobsDF)
                .transform(jobsDF);

        String[] cols = {"YearsExp", "FactorizedYears"};
        List<Row> yearsOfExp = factorizedYears.select("YearsExp", "FactorizedYears")
                .limit(n)
                .collectAsList();

        return yearsOfExp;
    }


//    12. Apply K-means for job title and companies (Bounce )


    public String kMeansAlgorithm()
    {
        Dataset<Row> dataset = jobsDF.as("data");
        String[] cols = {"Title", "Company"};
        String[] factorizedCols = {"TitleFactorized", "CompanyFactorized"};

        for(int i = 0; i < cols.length; i++)
        {
            StringIndexer indexer = new StringIndexer();
            indexer.setInputCol(cols[i]).setOutputCol(factorizedCols[i]);
            dataset = indexer.fit(dataset).transform(dataset);
        }

        for(int i = 0; i < cols.length; i++)
            dataset = dataset.withColumn(factorizedCols[i], dataset.col(factorizedCols[i]).cast("double"));


        VectorAssembler vectorAssembler = new VectorAssembler();
        vectorAssembler.setInputCols(factorizedCols).setOutputCol("features");
        Dataset<Row> trainData = vectorAssembler.transform(dataset);

        KMeans kmeans = new KMeans().setK(3).setSeed(1L);
        kmeans.setFeaturesCol("features");
        KMeansModel model = kmeans.fit(trainData);

        return "<center>" +
                "Model Distance Measure: " + model.getDistanceMeasure()
                + "<br>" +
                "Number of Features: " + model.numFeatures()
                + "<br>" +
                "Number of iterations: " + model.getMaxIter()
                + "<br>" +
                "Model Centers:" + Arrays.toString(model.clusterCenters())
                + "</center>";
    }


}
