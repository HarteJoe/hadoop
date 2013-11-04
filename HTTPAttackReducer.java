import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class HTTPAttackReducer extends Reducer < Text, Text, Text, Text > {
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        
        ArrayList<String> attackList = new ArrayList<>();
        for ( Text value : values) {
            String attackStr = value.toString();
            attackList.add( attackStr );
        }
        long size = attackList.size();
        String attackNum = String.valueOf(size);
        
        String attackType = key.toString();
        String output ="";
        switch ( attackType ) {
            case "attackType1":
                output = "Directory traversal. Attempt to access \"/etc/passwd\":   ";
                break;
            case "attackType2":
                output = "Directory traversal. Attempt to access \"/proc/self/environ\":   ";
                break;
            case "attackType3":
                output = "Directory traversal (other):   ";
                break;
            case "attackType4":
                output = "No directory traversal. Attempt to directly access \"/etc/passwd\":   ";
                break;
            case "attackType5":
                output = "No directory traversal. Attempt to directly access \"/proc/self/environ\":   ";
                break;
            case "attackType6":
                output = "No directory traversal or attempt to directly access \"/etc/passwd\" or \"/proc/self/environ\":    ";
        }
        
        context.write( new Text(output), new Text(attackNum) );
    } 
}
