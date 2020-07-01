package com.data;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * The TaskExecutor. It holds implementation to execute the map-reduce task.
 */
public class TaskExecutor extends Configured implements Tool {

	public int run(String[] args) throws Exception {

		if (args.length != 3) { // Checking for command line arguments
			System.err.printf("Usage: %s <input> <output>\n", getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		// Configuring the job
		JobConf conf = new JobConf(getConf(), TaskExecutor.class);
		conf.setJobName("Housing-Stats");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setReducerClass(DataJoiner.class);
		conf.setOutputFormat(TextOutputFormat.class);

		// Setting the input path
		MultipleInputs.addInputPath(conf, new Path(args[0]), TextInputFormat.class, WeatherMapper.class);
		MultipleInputs.addInputPath(conf, new Path(args[1]), TextInputFormat.class, HouseMapper.class);

		// Setting the output path
		FileOutputFormat.setOutputPath(conf, new Path(args[2]));

		// Running the job
		JobClient.runJob(conf);
		return 0;
	}

	/**
	 * The execution starts from here
	 * 
	 * @param args The command line arguments
	 * @throws Exception
	 */
	public static void main(String[] args) {

		try {
			int exitCode = ToolRunner.run(new TaskExecutor(), args);
			System.exit(exitCode);
		} catch (Exception exception) {
			System.out.println("Error executing the map reduce task.");
		}
	}
}
