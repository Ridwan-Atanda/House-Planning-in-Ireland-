package com.data;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * The DataJoiner. It holds implementation to join data from weather and house
 * mappers.
 */
public class DataJoiner extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

	/**
	 * Reduces the mapped input
	 */
	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

		try {
			int decisionsInFavor = 0;
			int decisionNotInFavor = 0;
			double totalAvgTemp = 0.0;
			int avgTempOccurrence = 0;
			double totalAvgRain = 0.0;
			double avgRainOccurrence = 0;
			boolean houseDataPresent = false;
			boolean weatherDataPresent = false;

			while (values.hasNext()) {
				Text value = values.next();
				String valueInfo = value.toString();
				String[] splittedValues = valueInfo.split(" ");

				if (StringUtils.equals(splittedValues[0], "House")) { // The data belongs to house data set
					houseDataPresent = true;

					if (StringUtils.equals(splittedValues[1], "approved")) { // The decision is in favor
						decisionsInFavor++;
					} else { // The decision is not in favor
						decisionNotInFavor++;
					}
				} else {
					weatherDataPresent = true;
					try {
						totalAvgTemp += Double.parseDouble(splittedValues[1]);
						avgTempOccurrence++;
					} catch (NumberFormatException numberFormatException) {
						// do nothing
					}

					try {
						totalAvgRain += Double.parseDouble(splittedValues[2]);
						avgRainOccurrence++;
					} catch (NumberFormatException numberFormatException) {
						// do nothing
					}
				}
			}

			// calculating the final averages
			double avgTemp = avgTempOccurrence > 0 ? (totalAvgTemp / avgTempOccurrence) : 0;
			double avgRain = avgRainOccurrence > 0 ? (totalAvgRain / avgRainOccurrence) : 0;

			if (houseDataPresent && weatherDataPresent) {
				output.collect(new Text(key.toString()),
						new Text(avgTemp + " " + avgRain + " " + decisionsInFavor + " " + decisionNotInFavor));
			}
		} catch (Exception exception) {
			System.out.println("Reduce process failed!!");
		}
	}
}
