import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

/**
 * @author Olivia(Jingyi) Wu
 * Andrew id: jingyiw2
 * This class uses spark to read The Tempest and perform various calculations, including line count, word count, letter and symbol count.
 * Basically we split content in different ways.
 */

public class TempestAnalytics {
    private final static String fileName = "TheTempest.txt";
    //create a SparkConf
    SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("JD Word Counter");
    JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
    JavaRDD<String> inputFile = null;
    public TempestAnalytics(String fileName) {
        inputFile = sparkContext.textFile(fileName);
    }
    // task 0: count method of the JavaRDD class, display the number of lines in "The Tempest"
    public long lineCount() {
        return inputFile.count();
    }
    // task 1: display the number of words in The Tempest
    public long wordCount() {
        // split the content with words and ignore empty string
        JavaRDD<String> wordsInFile = inputFile.flatMap(content -> Arrays.asList(content.split("[^a-zA-Z]+"))).filter(content -> ( !content.isEmpty()));
        return wordsInFile.count();
    }
    // task2: display the number of distinct words in The Tempest
    public long wordDistinctCount() {
        JavaRDD<String> wordsInFile = inputFile.flatMap(content -> Arrays.asList(content.split("[^a-zA-Z]+"))).filter(content -> ( !content.isEmpty()));
        return wordsInFile.distinct().count();
    }
    // task 3: find the number of symbols in The Tempest
    public long symbolCount() {
        // split the content with symbols
        JavaRDD<String> symbolsInFile = inputFile.flatMap(content -> Arrays.asList(content.split("")));
        return symbolsInFile.count();
    }

    // Task 4: Find the number of distinct symbols in The Tempest
    public long symbolDistinctCount() {
        JavaRDD<String> symbolsInFile = inputFile.flatMap(content -> Arrays.asList(content.split("")));
        return symbolsInFile.distinct().count();
    }

    // Task 5: Find the number of distinct letters in The Tempest
    public long letterCount() {
        // split with symbols and find symbol that matches letter pattern
        JavaRDD<String> letterInFile = inputFile.flatMap(content -> Arrays.asList(content.split(""))).filter( content -> ( content.matches("[a-zA-Z]+")));
        return letterInFile.distinct().count();
    }

    // Task 6: Find lines with specific word
    public List<String> getTargetLines(String word) {
        // simply split by default -- lines
        JavaRDD<String> lines = inputFile.filter(content -> content.contains(word));
        return lines.collect();
    }


    public static void main(String[] args) {
        TempestAnalytics ta = new TempestAnalytics(fileName);
        // Task 0: count method of the JavaRDD class, display the number of lines in "The Tempest"
        long lineCount = ta.lineCount();
        System.out.println("Task 0: The number of lines in TheTempest.txt is: " + lineCount);
        // Task 1: display the number of words in The Tempest
        long wordCount = ta.wordCount();
        System.out.println("Task 1: The number of words in TheTempest.txt is: " + wordCount);
        // Task 2: display the number of distinct words in The Tempest
        long wordDistinctCount = ta.wordDistinctCount();
        System.out.println("Task 2: The number of distinct words in TheTempest.txt is: " + wordDistinctCount);
        // Task 3: find the number of symbols in The Tempest
        long symbolCount = ta.symbolCount();
        System.out.println("Task 3: The number of symbols in TheTempest.txt is: " + symbolCount);
        // Task 4: Find the number of distinct symbols in The Tempest
        long symbolDistinctCount = ta.symbolDistinctCount();
        System.out.println("Task 4: The number of distinct symbols in TheTempest.txt is: " + symbolDistinctCount);
        // Task 5: Find the number of distinct letters in The Tempest
        long letterCount = ta.letterCount();
        System.out.println("Task 5: The number of distinct letters in TheTempest.txt is: " + letterCount);
        // Task 6: show all lines of The Tempest that contain specific word
        Scanner sc = new Scanner(System.in);
        System.out.println("Task 6: Input search word: ");
        String word = sc.nextLine();
        for (String line: ta.getTargetLines(word)){
            System.out.println(line);
        }
    }






}
