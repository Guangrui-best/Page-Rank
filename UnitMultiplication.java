import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UnitMultiplication {

    public static class TransitionMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, javax.xml.soap.Text value, Context context) throws IOException, InterruptedException {
        	//value: relations.txt fromId\tto1,to2,to3... 1\t2,7,8,29
        	//split tos -> calculate prob from fromId to toId
        	//outputKey = formId
        	//outputValue = toId=prob
        	
        	String[] fromTo = value.toString().trim().split("\t");
        	if (fromTo.length < 2) {
        		return;
        	}
        	
        	String[] tos = fromTo[1].split(",");
        	for (String to: tos) {
        		double prob = (double)1/tos.length;
        		String outputValue = to + "=" + prob;
        		String outputKey = fromTo[0];
        		context.write(new Text(outputKey), new Text(outputValue));
        		//1 \t 7=1/4
        	}
        	
            //input format: fromPage\t toPage1,toPage2,toPage3
            //target: build transition matrix unit -> fromPage\t toPage=probability
        }
    }

    public static class PRMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	//value=id \t pr
        	//split value
        	//outputKey = id
        	//outputValue = pr
        	String[] idPr = value.toString().trim().split("\t");
        	String outputKey = idPr[0];
        	String outputValue = idPr[1];
        	context.write(new Text(outputKey), new Text(outputValue));
        	
            String[] pr = value.toString().trim().split("\t");
            context.write(new Text(pr[0]), new Text(pr[1]));
        }
    }

    public static class MultiplicationReducer extends Reducer<Text, Text, Text, Text> {


        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            //input key = fromPage value=<toPage=probability..., pageRank>
            //target: get the unit multiplication
        	double pr = 0;
        	List<String> transitionCell = new ArrayList<String>();
        	for (Text value: values) {
        		if (value.toString().contains("=")) {
        			transitionCell.add(value);
        		} else {
        			pr = Double.parseDouble(value.toString());
        		}
        	}
        	
        	for (String cell: transitionCell) {
        		//toId=prob
        		String toId = cell.split(regex: "=")[0];
        		double prob = Double.parseDouble(cell.split("=")[1]);
        		double subPr = prob*pr;
        		context.write(new Text(toId), new Text(String.valueOf(subPr)));
        	}
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(UnitMultiplication.class);

        //how chain two mapper classes?

        job.setReducerClass(MultiplicationReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TransitionMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PRMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
    }

}
