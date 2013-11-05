/*
** @Author: Josef Harte		@Student Number: 12251747		@Email: josef.harte@ucdconnect.ie
** @Date last modified: 31 / 03 / 2013
** @Purpose: This class contains a single method writeHTML that writes the output Hadoop data to a HTML file for displaying.
** @Outline: The lone method writeHTML takes two Strings as arguments. These arguments are the directory where the data file to be processed 
** is located (and output HTML file), and the name to be given to the output HTML file. The data file name follows the standard Hadoop name "part-r-00000".
** HTML5 is used and a CSS file called "style.css", residing in the same directory as the Hadoop output directory, is specified for styling purposes.
** Each line of the input data file is read in and written as a list item (<li>) in an unordered list(<ul>).
*/
import java.io.*;

public class HTMLWriter {	
	
	public void writeHTML( String dataFileDir, String outputName ) { /* Directory of the data file and output file name */
		try {
				File output = new File( dataFileDir, outputName + ".html" );
				BufferedWriter writer = new BufferedWriter( new FileWriter(output) );
				
				/* Write standard HTML5 elements like <head>, <body> etc. */
				writer.write("<!DOCTYPE html>\n<html>\n<head>\n<title>Log File Analysis Results: " + outputName + "</title>\n");
				writer.write("<link href=\"../../style.css\" rel=\"stylesheet\" type=\"text/css\"/>\n</head>\n<body>\n");
				writer.write("<h1>" + outputName.replace( '_', ' ') + "</h1>"); /* Job name to appear as title on page */
				File input = new File( dataFileDir, "part-r-00000" ); /* Hadoop data file name */
				if ( input.exists() == false ) {
					System.out.println( "ERROR: data file in \"" + dataFileDir + "\" does not exist\nNo HTML file produced!");
					return;
				}
				
				/* Read in data and write the unordered list */
				BufferedReader reader = new BufferedReader( new FileReader(input) );
				writer.write("<ul>\n");
				String line;
				while ( ( line = reader.readLine() ) != null ) {
					writer.write( "<li>" + line + "</li>\n");
				}
				writer.write("</ul>\n");
				reader.close();
		
			    writer.write("</body>\n</html>");
			    writer.close(); /* Flushes and closes stream */
			    System.out.println("HTML results file for " + outputName + " written" );
		
		} catch (IOException e) {
			System.out.println("*****An error has occured while writing the data HTML file*****\n");
			e.printStackTrace();
		}
	}
}
		
		
