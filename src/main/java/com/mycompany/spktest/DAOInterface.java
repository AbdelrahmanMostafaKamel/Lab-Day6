/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.spktest;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 *
 * @author HP
 */
public class DAOInterface {
    private static final String COMMA_DELIMITER = ",";
    public JavaSparkContext  setConfiguration(){
        Logger.getLogger ("org").setLevel (Level.ERROR);
        // CREATE SPARK CONTEXT
        SparkConf conf = new SparkConf ().setAppName ("wordCounts").setMaster ("local[3]");
        JavaSparkContext sparkContext = new JavaSparkContext (conf);
        return sparkContext;
    }
    public JavaRDD<String> readFile(JavaSparkContext sparkContext,String path){
        JavaRDD<String> videos = sparkContext.textFile (path);
        return videos;
    }
    public JavaRDD<String> getTitles(JavaRDD<String> videos){
        // TRANSFORMATIONS
        JavaRDD<String> titles = videos
                .map (DAOInterface::extractTitle)
                .filter (StringUtils::isNotBlank);
        return titles;
    } 
    public JavaRDD<String> getWords(JavaRDD<String> titles){
         JavaRDD<String> words = titles.flatMap (title -> Arrays.asList (title
                .toLowerCase ()
                .trim ()
                .replaceAll ("\\p{Punct}", " ")
               .split (" ")).iterator ());
         System.out.println(words.toString ());
         return words;
    }  
    public Map<String, Long> getWordCounts(JavaRDD<String> words){
        // COUNTING
        Map<String, Long> wordCounts = words.countByValue ();
        return wordCounts;
    }
   public List<Map.Entry> getWordCountsSorted(Map<String, Long> wordCounts){
       List<Map.Entry> sorted = wordCounts.entrySet ().stream ()
                .sorted (Map.Entry.comparingByValue ()).collect (Collectors.toList ());
       return sorted;
   }
   public void displayInfo(List<Map.Entry> sorted){
       // DISPLAY
        for (Map.Entry<String, Long> entry : sorted) {
            System.out.println (entry.getKey () + " : " + entry.getValue ());
        }
   }         
    public static String extractTitle(String videoLine) {
        try {
            return videoLine.split (COMMA_DELIMITER)[2];
        } catch (ArrayIndexOutOfBoundsException e) {
            return "";
        }
    }
    public JavaRDD<String> getTags(JavaRDD<String> videos){
        // TRANSFORMATIONS
        JavaRDD<String> tags = videos
                .map (DAOInterface::extractTags)
                .filter (StringUtils::isNotBlank);
        return tags;
    }  
    public static String extractTags(String videoLine){
       try {
            return videoLine.split (COMMA_DELIMITER)[6];
        } catch (ArrayIndexOutOfBoundsException e) {
            return "";
       }   
}
    public JavaRDD<String> getTagsWords(JavaRDD<String> tags){
         JavaRDD<String> tagWords= tags.flatMap (tag -> Arrays.asList (tag
                .toLowerCase ()
                .trim ()
               .split ("\\|")).iterator ());
         System.out.println(tagWords.toString ());
         return tagWords;
   } 
}
    