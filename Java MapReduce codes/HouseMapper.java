package com.data;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * The mapper HouseMapper. It holds implementation to map the house data.
 */
public class HouseMapper extends MapReduceBase implements Mapper<Object, Text, Text, Text> {

	/**
	 * Maps the input
	 */
	public void map(Object key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

		// NOTE: The following keys are there in house data- GrantDate,
		// PlanningAuthority, Decision, Time of Processing
		String tag = "House";
		try {
			String[] columns = value.toString().split(",");
			output.collect(new Text(columns[1] + " " + columns[0]), new Text(tag + " " + columns[2]));
		} catch (Exception exception) {
			System.out.println("Error mapping the line: " + value.toString());
		}
	}
}
