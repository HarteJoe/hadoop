/*
** @Author: Josef Harte		@Student Number: 12251747		@Email: josef.harte@ucdconnect.ie
** @Date last modified: 26 / 03 / 2013
** @Purpose: This class maps a timestamp String (the key) of form "year / month" to a host String (the value).
** @Outline: The class extends Mapper and overrides its map method. Map receives a line from the log file as the value, the key being the byte offset
** of the line in the file. This key is ignored. Map also takes a Context argument to which the output (key, value) pair is written. A regular expression is
** used to find the timestamp in the line. The required parts are then removed from the timestamp String and reordered for convenience. Another regular 
** expression finds the host String.
*/
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MonthMapper extends Mapper < LongWritable, Text, Text, Text > {
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        // Extract the timestamp from the line
        String line = value.toString();
        Pattern pat = Pattern.compile("\\[.+\\]");
        Matcher mat = pat.matcher( line );
        boolean found = mat.find();
        String timeStamp = "";
        if ( found == false ) {
            System.err.println("Timestamp not found in log file entry!");
            System.exit(1);
        } else {
            timeStamp = mat.group();
        } 
        
        // Extract the month from the timestamp and reorder
        String year = timeStamp.substring( 8, 12 );
        String month = timeStamp.substring( 4, 7 );
        String monthStamp = year + " / " + month + " ---- ";
        
        // Extract the host from the input line
        pat = Pattern.compile("(^.[^ ]+ )");
        mat = pat.matcher( line );
        found = mat.find();
        String temp = "";
        if ( found == false ) {
            System.err.println("Host not found in log file entry!");
            System.exit(1);
        } else {
            temp = mat.group(1);
        }
        String host = temp.trim();
        
        context.write( new Text(monthStamp), new Text(host) );
    }
    
}
