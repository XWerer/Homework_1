package it.unipd.dei.bdc1718;

import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.*;
import java.io.*;

//prova github comment

public class G08HM1 {

  //Commento prova github
    //kjjsbibibsois7
    //bkjebheobninboniboenobenobn


  //Method to find the mean of an ArrayList of Double elements
  public static double mean(ArrayList<Double> x){
    double sum=0;
    for(int i=0; i<x.size(); i++){
      sum = sum + x.get(i);
    }
    return sum/x.size();
  }

  //Implementation of the Serializable and Comparator interfaces
  public static class DoubleComparator implements Serializable, Comparator <Double> {

    //Implementation of the compare method
    public int compare(Double  a, Double b) {
      if (a < b) return -1;
      if (a > b) return 1;
      return 0;
    }

  }

  public static void main(String[] args) throws FileNotFoundException {
    if (args.length == 0){
      throw new IllegalArgumentException("Expecting the file name on the command line");
    }

    // Read a list of numbers from the program options
    ArrayList<Double> lNumbers = new ArrayList<>();
    Scanner s =  new Scanner(new File(args[0]));
    while (s.hasNext()){
      lNumbers.add(Double.parseDouble(s.next()));
    }
    s.close();

    // Setup Spark
    SparkConf conf = new SparkConf(true).setAppName("Preliminaries");
    JavaSparkContext sc = new JavaSparkContext(conf);

    // Creates and calculates lDiffavgs
    ArrayList<Double> lDiffavgs = new ArrayList<>();
    double mean = mean(lNumbers);
    for(int i = 0; i < lNumbers.size(); i++){
      lDiffavgs.add(Math.abs(mean - lNumbers.get(i)));
    }

    // Create a parallel collection
    JavaRDD<Double> dDiffavgs = sc.parallelize(lDiffavgs);

    // Computes the min with the reduce phase
    double min = dDiffavgs.reduce((x, y) -> {
      if(x >= y) return y;
      return x;
            });
    System.out.println("The min using the reduce phase is " + min);

    // Computes the min with the min method from JavaRDD class and passes to it the comparator previously done
    min = dDiffavgs.min(new DoubleComparator());
    System.out.println("The min using the min method from JavaRDD class is " + min);

    // Computes the max with the max method from JavaRDD class and passes to it the comparator previously done
    double max = dDiffavgs.max(new DoubleComparator());
    System.out.println("The max using the max method from JavaRDD class is " + max);

    // Counts all elements in dDiffavgs
    long count = dDiffavgs.count();
    System.out.println("The total number of elements of our dataset is " + count);

    // Counts all distinct elements in dDiffavgs
    count = dDiffavgs.distinct().count();
    System.out.println("The total number of distinct elements of our dataset is " + count);
  }
}
