import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class MappySparkJobExam {
    public static void main(String args[]) {
        SparkConf conf = new SparkConf().setAppName("mappy-spark-job-log-stat").setMaster("local");
        
        /* Spark Context*/
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        JavaRDD<String> linesRDD = sc.textFile("data/tornik-map-20171006.10000.tsv");
        
        System.out.println("NB line " + linesRDD.count());
        
     
      
        JavaRDD<String[]> part2FiltredLinesRDD  = linesRDD.map(line -> line.split("	")) // We can use \t instead of tab white char
        .filter(tab -> tab.length > 1)
        .map(tab -> tab[1])
        .map(element -> element.split("/"))
        .filter(tabElement -> tabElement.length > 7);
        
        JavaPairRDD<String, Integer>  modeViewRDD2  = part2FiltredLinesRDD.mapToPair( tabTuple ->  new Tuple2<String , Integer>(tabTuple[4], Integer.valueOf(1)))
                .reduceByKey((occurs1,occurs2) -> (occurs1 + occurs2));
        
        
        
        
        JavaPairRDD<String, String> zoomsDistinctRDD21 = part2FiltredLinesRDD.mapToPair( tabTuple ->  new Tuple2<String , String>(tabTuple[4], tabTuple[6]))
			//.filter(f);
			//.mapToPair(t -> new Tuple2<String, String>(t._1, t._2.toString()))
			.reduceByKey((zoom1, zoom2) -> (zoom1 +  "," + zoom2));
					
					
        
        modeViewRDD2.join(zoomsDistinctRDD21).saveAsTextFile("/data/output/tornik-map-result.txt");
        
        
        
       
        sc.close();
    }
}