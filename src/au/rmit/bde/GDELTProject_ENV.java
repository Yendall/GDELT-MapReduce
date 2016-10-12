package au.rmit.bde;
/**
 * Author: 		Max Yendall (s3436993)
 * Version: 	1.0
 * File Name:	GDELTProject_ENV.java	
 */
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.javatuples.*;

import au.rmit.bde.GDELTProject_ENV.GDELTParserMapper.GDELT_HEADER;

public class GDELTProject_ENV extends Configured implements Tool {

	/**
	 * GDELT Mapper which reads each TXT file from the GDELT Event Database and
	 * parses specific event specifications related to the denoted query
	 * 
	 */
	static public class GDELTParserMapper extends Mapper<LongWritable, Text, Text, Text> {

		// Define Header Enum for GDELT Records
		enum GDELT_HEADER {
			GLOBALEVENTID, SQLDATE, MonthYear, Year, FractionDate, Actor1Code, Actor1Name, Actor1CountryCode, Actor1KnownGroupCode, Actor1EthnicCode, Actor1Religion1Code, Actor1Religion2Code, Actor1Type1Code, Actor1Type2Code, Actor1Type3Code, Actor2Code, Actor2Name, Actor2CountryCode, Actor2KnownGroupCode, Actor2EthnicCode, Actor2Religion1Code, Actor2Religion2Code, Actor2Type1Code, Actor2Type2Code, Actor2Type3Code, IsRootEvent, EventCode, EventBaseCode, EventRootCode, QuadClass, GoldsteinScale, NumMentions, NumSources, NumArticles, AvgTone, Actor1Geo_Type, Actor1Geo_FullName, Actor1Geo_CountryCode, Actor1Geo_ADM1Code, Actor1Geo_Lat, Actor1Geo_Long, Actor1Geo_FeatureID, Actor2Geo_Type, Actor2Geo_FullName, Actor2Geo_CountryCode, Actor2Geo_ADM1Code, Actor2Geo_Lat, Actor2Geo_Long, Actor2Geo_FeatureID, ActionGeo_Type, ActionGeo_FullName, ActionGeo_CountryCode, ActionGeo_ADM1Code, ActionGeo_Lat, ActionGeo_Long, ActionGeo_FeatureID, DATEADDED, SOURCEURL
		}

		// Define all header maps and output variables
		private Text tokenValue = new Text();
		private Text toneNumValue = new Text();
		private DoubleWritable toneValue = new DoubleWritable(1.0);

		private HashMap<String, String> quad_class = new HashMap<String, String>();
		private HashMap<String, String> event_codes = new HashMap<String, String>();
		private HashMap<String, String> country_codes = new HashMap<String, String>();
		private HashMap<String, String> event_country = new HashMap<String, String>();

		/**
		 * Setup method which reads specified TXT files into header HashMap
		 * objects for reference when parsing numeric attributes from the data
		 * set
		 * 
		 * @param map
		 *            : Header HashMap
		 * @param filename
		 *            : TXT file name in root directory
		 */
		public void setupMaps(HashMap<String, String> map, String filename) {
			InputStream is = getClass().getResourceAsStream(filename);
			InputStreamReader isr = new InputStreamReader(is);
			try (BufferedReader br = new BufferedReader(isr);) {
				String current_line;

				while ((current_line = br.readLine()) != null) {
					String[] current = current_line.toString().split("\\t");
					map.put(current[0], current[1]);
				}

			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		/**
		 * Hadoop Setup method to be run on each Mapper to ensure all headers
		 * are ready for processing the data set
		 * 
		 * @param Context
		 *            contextual reference
		 * @throws IOException
		 * @throws InterruptedException
		 * 
		 */
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			// Set up Country Codes
			setupMaps(country_codes, "country.txt");
			// Set up Quad Classes
			setupMaps(quad_class, "quad_class.txt");
			// Set up Event Codes
			setupMaps(event_codes, "eventcodes.txt");
			// Set up Event Country Codes
			setupMaps(event_country, "event_countries.txt");
		}

		/**
		 * Hadoop Map method where all parallel processing occurs. Filtered
		 * using header comparisons
		 * 
		 * @param context
		 *            Contextual reference
		 * @param text
		 *            Line parsed by the current Mapper from the data
		 * @param offset
		 *            Split for global file offset
		 * @throws IOException
		 * @throws InterruptedException
		 * 
		 */
		@Override
		protected void map(LongWritable offset, Text text, Context context) throws IOException, InterruptedException {

			String type = "", country = "", code = "";
			String[] result = text.toString().split("\\t");
			double latitude = 0, longitude = 0, tone = 0, goldstein_scale = 0, avg_tone = 0, num_mentions = 0;
			boolean LAT_FLAG = false, LONG_FLAG = false, QUAD_FLAG = false, ENV_FLAG = false, GOLD_FLAG = false,
					AVG_FLAG = false;
			
			for (int i = 0; i < result.length; i++) {
				
				// Set current GDELT header value
				GDELT_HEADER curr = GDELT_HEADER.values()[i];

				// Begin checking against the Taxonomy
				if (!result[i].isEmpty() && result[i] != null) {
					switch (curr) {
					// Check taxonomy reference for country name via cameo code
					case ActionGeo_CountryCode:
						code = result[i];
						country = event_country.get(result[i]);
						break;
					// Check dyadic victim is related to the environment
					case Actor2Type1Code:
						type = result[i];
						if (type.equals("ENV")) {
							ENV_FLAG = true;
						}
						break;
					// Check latitude value for location
					case ActionGeo_Lat:
						latitude = Double.parseDouble(result[i]);
						LAT_FLAG = true;
						break;
					// Check longitude value for location
					case ActionGeo_Long:
						longitude = Double.parseDouble(result[i]);
						LONG_FLAG = true;
						break;
					// Check quad class and ensure it is matieral / verbal conflict
					case QuadClass:
						int quad_class = Integer.parseInt(result[i]);
						if (quad_class == 3 || quad_class == 4) {
							QUAD_FLAG = true;
						}
						break;
					// Check goldstein scale for story
					case GoldsteinScale:
						goldstein_scale = Double.parseDouble(result[i]);
						GOLD_FLAG = true;
						break;
					// Check average tone for news story
					case AvgTone:
						avg_tone = Double.parseDouble(result[i]);
						AVG_FLAG = true;
						break;
					// Check number of mentions for news story
					case NumMentions:
						num_mentions = Integer.parseInt(result[i]);
						break;

					default:
						break;
					}
				}
			}

			if (LAT_FLAG && LONG_FLAG && ENV_FLAG && GOLD_FLAG && AVG_FLAG && QUAD_FLAG) {

				// Create tone, mentions and Goldstein value
				String tone_num = String.valueOf(avg_tone) + "," + String.valueOf(num_mentions) + ","
						+ String.valueOf(goldstein_scale);
				// Create a Triplet as the key
				Triplet<String, Double, Double> key = Triplet.with(country, longitude, latitude);
				// Set all output values
				tokenValue.set(key.toString());
				toneNumValue.set(tone_num);
				toneValue.set(avg_tone);
				// Write to reducer
				context.write(tokenValue, toneNumValue);
			}
		}
	}

	/**
	 * Hadoop Reducer class where all reduction and omitting occurs. The average
	 * tone is calculated by summation and division
	 * 
	 */
	static public class GDELTParserReducer extends Reducer<Text, Text, Text, Text> {
		private DoubleWritable total = new DoubleWritable();
		private Text outputToken = new Text();

		/**
		 * Hadoop Reduce method where all reduction and omitting occurs. Average
		 * tone is calculated using the mathematical average of all average
		 * tones
		 * 
		 * @param context
		 *            Contextual reference
		 * @param token
		 *            The key to omit (Triplet of country, longitude and
		 *            latitude)
		 * @param tones
		 *            Iterable list of tones for this particular location
		 * @throws IOException
		 * @throws InterruptedException
		 * 
		 */
		@Override
		protected void reduce(Text token, Iterable<Text> tones, Context context)
				throws IOException, InterruptedException {

			// Remove brackets from Pair data structure for CSV formatting and
			// Python post-processing
			String token_trim = token.toString();
			token_trim = token_trim.replace("[", "");
			token_trim = token_trim.replace("]", "");
			token_trim = token_trim.replace(" ", "");

			// Variables for calculating the average tone
			int avg_tone = 0, total_tone = 0;
			int avg_goldstein = 0, total_goldstein = 0;
			int num_mentions = 0;
			int num_tones = 0;
			String[] tone_trim;

			// Write out average tone for the data entry
			for (Text tone : tones) {
				tone_trim = tone.toString().split(",");
				total_tone += Double.parseDouble(tone_trim[0]);
				num_mentions += Double.parseDouble(tone_trim[1]);
				total_goldstein += Double.parseDouble(tone_trim[2]);
				num_tones++;
			}

			// Calculate the average tone for this area
			avg_tone = total_tone / num_tones;
			avg_goldstein = total_goldstein / num_tones;

			// Set values and omit
			outputToken.set(String.valueOf(avg_tone) + "," + String.valueOf(num_mentions) + ","
					+ String.valueOf(avg_goldstein));
			token.set(token_trim);
			context.write(token, outputToken);

		}
	}

	public int run(String[] args) throws Exception {
		Configuration configuration = getConf();

		// Initialising Map Reduce Job
		Job job = new Job(configuration, "GDLET Parser");

		// Set Map Reduce main jobconf class
		job.setJarByClass(GDELTProject_ENV.class);

		// Set Mapper class
		job.setMapperClass(GDELTParserMapper.class);

		// set Reducer class
		job.setReducerClass(GDELTParserReducer.class);

		// set Input Format
		job.setInputFormatClass(TextInputFormat.class);

		// set Output Format
		job.setOutputFormatClass(TextOutputFormat.class);

		// set Output key class
		job.setOutputKeyClass(Text.class);

		// set Output value class
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : -1;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new GDELTProject_ENV(), args));
	}
}
