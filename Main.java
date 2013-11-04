/*
** @Author: Josef Harte		@Student Number: 12251747		@Email: josef.harte@ucdconnect.ie
** @Date last modified: 26 / 03 / 2013
** @Purpose: This class sets up all of the MapReduce jobs and displays results in Firefox.
*/
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.File;

public class Main {
        
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
      
        /* Create the job for number of unique hosts per hour */
        Job jobHour = new Job();
        jobHour.setJarByClass( Main.class ); // Tells nodes where to find Mapper & Reducer
        jobHour.setJobName("Unique number of hosts hourly");
        jobHour.setMapperClass( HourMapper.class );
        jobHour.setReducerClass( GeneralReducer.class );
        jobHour.setMapOutputKeyClass( Text.class ); 
        jobHour.setMapOutputValueClass( Text.class );
        jobHour.setOutputKeyClass( Text.class );
        jobHour.setOutputValueClass( Text.class );
        FileInputFormat.addInputPath( jobHour, new Path(args[0]) );
        String outputPath = args[1] + "/unique_hosts_hourly";
        FileOutputFormat.setOutputPath( jobHour, new Path( outputPath ) );
        
        jobHour.waitForCompletion(true); // Blocks while waiting for 1st job to finish
        
        /* Create the job for unique number of hosts per day */
        Job jobDay = new Job();
        jobDay.setJarByClass( Main.class );
        jobDay.setJarByClass( Main.class );
        jobDay.setJobName("Unique number of hosts daily");
        jobDay.setMapperClass( DayMapper.class );
        jobDay.setReducerClass( GeneralReducer.class );
        jobDay.setMapOutputKeyClass( Text.class );
        jobDay.setMapOutputValueClass( Text.class );
        jobDay.setOutputKeyClass( Text.class );
        jobDay.setOutputValueClass( Text.class );
        FileInputFormat.addInputPath( jobDay, new Path(args[0]) );
        outputPath = args[1] + "/unique_hosts_daily";
        FileOutputFormat.setOutputPath( jobDay, new Path( outputPath ) );
        
        jobDay.waitForCompletion(true); // Blocks ...
               
        /* Create the job for number of unique hosts per week */
        Job jobWeek = new Job();
        jobWeek.setJarByClass( Main.class );
        jobWeek.setJobName("Unique number of hosts weekly");
        jobWeek.setMapperClass( WeekMapper.class );
        jobWeek.setReducerClass( GeneralReducer.class );
        jobWeek.setMapOutputKeyClass( Text.class );
        jobWeek.setMapOutputValueClass( Text.class );
        jobWeek.setOutputKeyClass( Text.class );
        jobWeek.setOutputValueClass( Text.class );
        FileInputFormat.addInputPath( jobWeek, new Path(args[0]) );
        outputPath = args[1] + "/unique_hosts_weekly";
        FileOutputFormat.setOutputPath( jobWeek, new Path( outputPath ) );
         
        jobWeek.waitForCompletion(true); // Blocks again ...  
        
        /* Create the job for number of unique hosts per month */
        Job jobMonth = new Job();
        jobMonth.setJarByClass( Main.class );
        jobMonth.setJobName("Unique number of hosts monthly");
        jobMonth.setMapperClass( MonthMapper.class );
        jobMonth.setReducerClass( GeneralReducer.class );
        jobMonth.setMapOutputKeyClass( Text.class );
        jobMonth.setMapOutputValueClass( Text.class );
        jobMonth.setOutputKeyClass( Text.class );
        jobMonth.setOutputValueClass ( Text.class );
        FileInputFormat.addInputPath( jobMonth, new Path(args[0]) );
        outputPath = args[1] + "/unique_hosts_monthly";
        FileOutputFormat.setOutputPath( jobMonth, new Path(outputPath) );
        
        jobMonth.waitForCompletion(true); // Blocks ...
        
        /* Create the job for number of attacks */
        Job jobAttack = new Job();
        jobAttack.setJarByClass( Main.class );
        jobAttack.setJobName("Number of HTTP attacks");
        jobAttack.setMapperClass( HTTPAttackMapper.class );
        jobAttack.setReducerClass( HTTPAttackReducer.class );
        jobAttack.setMapOutputKeyClass( Text.class );
        jobAttack.setMapOutputValueClass ( Text.class );
        jobAttack.setOutputKeyClass( Text.class );
        jobAttack.setOutputValueClass( Text.class );
        FileInputFormat.addInputPath( jobAttack, new Path(args[0]) );
        outputPath = args[1] + "/HTTP_attacks";
        FileOutputFormat.setOutputPath( jobAttack, new Path(outputPath) );
        
        jobAttack.waitForCompletion(true);
       
        /* Write HTML files */
        outputPath = args[1];
        HTMLWriter myWriter = new HTMLWriter();
        String strHourly = "unique_hosts_hourly";
        myWriter.writeHTML( outputPath + "/" + strHourly, strHourly );
        String strDaily = "unique_hosts_daily";
        myWriter.writeHTML( outputPath + "/" + strDaily,  strDaily );
        String strWeekly = "unique_hosts_weekly";
        myWriter.writeHTML( outputPath + "/" + strWeekly, strWeekly );
        String strMonthly = "unique_hosts_monthly";
        myWriter.writeHTML( outputPath + "/" + strMonthly, strMonthly );
        String strAttacks = "HTTP_attacks";
        myWriter.writeHTML( outputPath + "/" + strAttacks, strAttacks );
        
        /* Display HTML files using a web browser */
        Runtime rt = Runtime.getRuntime();
        String[] command = 
        		{ "firefox",
        		"./" + strHourly + "/" + strHourly + ".html",
        		"./" + strDaily + "/" + strDaily + ".html",
        		"./" + strWeekly + "/" + strWeekly + ".html",
        		"./" + strMonthly + "/" + strMonthly + ".html",
        		"./" + strAttacks + "/" + strAttacks + ".html" }; // Pass html files to Firefox
        	System.out.println("Displaying results in Firefox ...");
        Process proc = rt.exec( command, null, new File( outputPath ) ); // Run process in same directory as "outputPath"
        proc.waitFor();
        
    }  
}
