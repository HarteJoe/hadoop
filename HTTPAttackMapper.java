/*
** @Author: Josef Harte		@Student Number: 12251747		@Email: josef.harte@ucdconnect.ie
** @Date last modified: 26 / 03 / 2013
** @Purpose: This class is used to find occurences of particular HTTP attack types in the log file. The output key is the attack type and
** the output value is an empty String simply used to count the number of occurences of that attack type.
** @Outline: The class extends Mapper and overrides its map method. Map receives a line from the log file as the value, the key being the byte offset
** of the line in the file. This key is ignored. Map also takes a Context argument to which the output (key, value) pair is written. A regular expression is 
** used to find occurences of a directory traversal (../) in the URL String of the HTTP request. If this pattern is found, the URL is searched for occurences
** of "/etc/passwd" or "/proc/self/environ". These are two common files on a UNIX system that attackers try to gain access to with a directory traversal. 
** If a directory traversal is not found, the URL is still searched for occurences of the above files as direct access may still be attempted. As outlined above,
** the output value is a dummy empty String, used by the reducer simply to count the number of attacks of a given type.
**
*/
import java.io.IOException;
import java.util.regex.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HTTPAttackMapper extends Mapper < LongWritable, Text, Text, Text > {
    
    @Override
    public void map( LongWritable key, Text value, Context context ) throws IOException, InterruptedException {
        
        String line = value.toString();
        Pattern pat = Pattern.compile("(\\.\\./)+");
        Matcher mat = pat.matcher( line );
        boolean found = mat.find();
        String type1 = "etc/passwd";
        String type2 = "proc/self/environ";
        String attack;
        if ( found == true ) {
            if ( line.contains(type1) ) {
                attack = "attackType1";
            } else if ( line.contains(type2) ) {
                attack = "attackType2";
            } else { attack = "attackType3"; }
        } else {
            if ( line.contains(type1) ) {
                attack = "attackType4";
            } else if ( line.contains(type2) ) {
                attack = "attackType5";
            } else { attack = "attackType6"; }
        }  
        
        String dummy = ""; // Dummy variable used just to count number of attacks
        context.write( new Text(attack), new Text(dummy) );
    }
    
}
