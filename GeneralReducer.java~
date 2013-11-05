/*
** @Author: Josef Harte		@Student Number: 12251747		@Email: josef.harte@ucdconnect.ie
** @Date last modified: 26 / 03 / 2013
** @Purpose: This the the reducer class for all the mapper classes finding unique hosts. The output key is the timestamp that came
** from the mapper class and the output value is the unique number of hosts for that particular timestamp.
** Output (key, value) is actually of class Text, which is comparable to String but optimized for network serialization.
*/
import java.io.IOException;
import java.util.TreeSet;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GeneralReducer extends Reducer< Text, Text, Text, Text > {
    
    @Override
    public void reduce( Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        
        TreeSet<String> hostSet = new TreeSet<>();
        for ( Text val : values ) {
            String host = val.toString();
            hostSet.add( host ); /* Will filter out duplicate Strings (hosts) */
        }
        int uniqueNum = hostSet.size();
        String uniqueNumStr = String.valueOf( uniqueNum );
        
        context.write( key, new Text( uniqueNumStr ) );                
  
    }   
}
