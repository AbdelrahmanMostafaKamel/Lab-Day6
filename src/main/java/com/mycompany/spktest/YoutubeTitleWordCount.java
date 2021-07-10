package com.mycompany.spktest;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


public class YoutubeTitleWordCount {
    public static void main(String[] args) throws IOException {
       DAOInterface DAO=new DAOInterface();
       JavaSparkContext sparkContext=DAO.setConfiguration();
       JavaRDD<String> videos=DAO.readFile(sparkContext,"src/main/resources/data/USvideos.csv");
       JavaRDD<String> titles=DAO.getTitles(videos);
       JavaRDD<String> words=DAO.getWords(titles);
       Map<String, Long> wordCounts=DAO.getWordCounts(words);
       List<Map.Entry> sorted=DAO.getWordCountsSorted(wordCounts);
       //DAO.displayInfo(sorted);
       JavaRDD<String> tags=DAO.getTags(videos);
       //JavaRDD<String> tagsSeparted=DAO.getWords(tags);
       JavaRDD<String>tagsSeparted=DAO.getTagsWords(tags);
       Map<String,Long> tagsCounts=DAO.getWordCounts(tagsSeparted);
       List<Map.Entry> tagsSorted=DAO.getWordCountsSorted(tagsCounts);
       DAO.displayInfo(tagsSorted);
    }
}
//private static final String COMMA_DELIMITER = ",";
 //Logger.getLogger ("org").setLevel (Level.ERROR);
        // CREATE SPARK CONTEXT
       // SparkConf conf = new SparkConf ().setAppName ("wordCounts").setMaster ("local[3]");
        //JavaSparkContext sparkContext = new JavaSparkContext (conf);
        // LOAD DATASETS
        //JavaRDD<String> videos = sparkContext.textFile ("src/main/resources/data/USvideos.csv");
        
   
        
        // TRANSFORMATIONS
        //JavaRDD<String> titles = videos
               // .map (YoutubeTitleWordCount::extractTitle)
                //.filter (StringUtils::isNotBlank);
       // JavaRDD<String>
       // JavaRDD<String> words = titles.flatMap (title -> Arrays.asList (title
              //  .toLowerCase ()
              //  .trim ()
              //  .replaceAll ("\\p{Punct}", " ")
             //  .split (" ")).iterator ());
       // System.out.println(words.toString ());
        // COUNTING
      //  Map<String, Long> wordCounts = words.countByValue ();
      //  List<Map.Entry> sorted = wordCounts.entrySet ().stream ()
             //   .sorted (Map.Entry.comparingByValue ()).collect (Collectors.toList ());
        // DISPLAY
        //for (Map.Entry<String, Long> entry : sorted) {
            //System.out.println (entry.getKey () + " : " + entry.getValue ());
       // }
    //}
   // public static String extractTitle(String videoLine) {
       // try {
          //  return videoLine.split (COMMA_DELIMITER)[2];
        //} catch (ArrayIndexOutOfBoundsException e) {
          //  return "";
       // }