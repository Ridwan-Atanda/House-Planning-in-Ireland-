package com.data;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * The mapper WeatherMapper. It holds implementation to map the weather data.
 */
public class WeatherMapper extends MapReduceBase implements Mapper<Object, Text, Text, Text> {

	/**
	 * Maps the input
	 */
	public void map(Object key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

		// NOTE: The following keys are there in weather data- Date, county, maxtp,
		// mintp, rain
		String tag = "Weather";
		try {
			String[] columns = value.toString().split(",");
			if (columns.length == 5) {

				Double maxTemp = Double.parseDouble(columns[2]);
				Double minTemp = Double.parseDouble(columns[3]);
				Double average = (maxTemp + minTemp) / 2;
				output.collect(new Text(columns[1] + " " + columns[0]),
						new Text(tag + " " + average.toString() + " " + columns[4]));
			}
		} catch (Exception exception) {
			System.out.println("Error mapping the line: " + value.toString());
		}
	}
}
