
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by dongxu on 4/9/17.
 */
public class GridGenerator {

  public static class DerivedArrayWritable extends ArrayWritable {
    public DerivedArrayWritable() {
      super(Text.class);
    }

    public DerivedArrayWritable(String[] strings) {
      super(Text.class);
      Text[] texts = new Text[strings.length];
      for (int i = 0; i < strings.length; i++) {
        texts[i] = new Text(strings[i]);
      }
      set(texts);
    }
  }

  private static Long timeParser(String timeString, Integer localDifference) throws ParseException{
    String timeFormat = "EE MMM dd HH:mm:ss +0000 yyyy";
    SimpleDateFormat formatter = new SimpleDateFormat(timeFormat);
    long timeStamp = formatter.parse(timeString).getTime();
    localDifference *= 1000*60;
    return timeStamp + localDifference;
  }

  public static class GridGeneratorMapper
      extends Mapper<Object, Text, Text, DerivedArrayWritable> {

    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
      String[] fields = FileIOHelper.TabLineParse(value.toString());
      String venueKey;
      DerivedArrayWritable values;

      if (fields.length == 7){
        // is venue
        venueKey = fields[0];
        values  = new DerivedArrayWritable(fields);
      } else {
        // is check-in
        venueKey = fields[1];
        try {
          String[] newFields = {fields[0], timeParser(fields[2], Integer.parseInt(fields[3])).toString()};
          values = new DerivedArrayWritable(newFields);
        } catch (ParseException e){
          e.printStackTrace();
          return;
        }

      }
      context.write(new Text(venueKey), values);
    }
  }

//    private static class GridGeneratorPartitioner extends Partitioner<Text,DerivedArrayWritable> {
//
//        @Override
//        public int getPartition(Text key, DerivedArrayWritable value, int numReduceTasks) {
//            if (numReduceTasks == 0)
//                return 0;
//            return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
//        }
//    }

  public static class GridGeneratorReducer
      extends Reducer<Text,DerivedArrayWritable,NullWritable,Text> {

    public void reduce(Text key, Iterable<DerivedArrayWritable> values,
                       Context context
    ) throws IOException, InterruptedException {

      String[] venue = null;
      List<String[]> checkins = new ArrayList<>();
      Iterator<DerivedArrayWritable> iterator = values.iterator();
      while (iterator.hasNext()) {
        DerivedArrayWritable currentValue = iterator.next();
        String[] rowValues = currentValue.toStrings();
        if (rowValues.length == 7) {
          venue = rowValues;
        } else {
          checkins.add(rowValues);
        }
      }

      assert (venue != null);

      // Convert to JSON format
      JSONObject obj = new JSONObject();
      try {
        obj.put("ID", venue[0]);
        obj.put("Lat", venue[1]);
        obj.put("Lng", venue[2]);
        obj.put("Cat", venue[4]);
        obj.put("Sub_Cat", venue[3]);
        obj.put("Country", venue[5]);
        obj.put("City", venue[6]);
      } catch (JSONException e) {
        e.printStackTrace();
      }


      JSONArray allCheckIns = new JSONArray();
      for (String[] checkin : checkins){
        JSONArray checkInItem = new JSONArray();
        checkInItem.put(checkin[0]);
        checkInItem.put(checkin[1]);
        allCheckIns.put(checkInItem);
      }

      try {
        obj.put("Check-Ins", allCheckIns);
      } catch (JSONException e) {
        e.printStackTrace();
      }

      context.write(NullWritable.get(), new Text(obj.toString()));

    }
  }


  public static void main(String[] args) throws Exception {
    Configuration conf1 = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();

    if (otherArgs.length < 3) {
      System.err.println("Usage: AddCityBroadRange <Venue file (with category)> " +
          "<Check-in file> <Output file>");
      System.exit(2);
    }

    Job job1 = Job.getInstance(conf1, "GridGenerator");

    job1.setJarByClass(GridGenerator.class);
    //job1.setPartitionerClass(GridGeneratorPartitioner.class);
    job1.setMapperClass(GridGeneratorMapper.class);
    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(DerivedArrayWritable.class);

    job1.setReducerClass(GridGeneratorReducer.class);
    job1.setOutputKeyClass(NullWritable.class);
    job1.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
    FileInputFormat.addInputPath(job1, new Path(otherArgs[1]));
    FileOutputFormat.setOutputPath(job1, new Path(otherArgs[2]));

    if (!job1.waitForCompletion(true))
      System.exit(1);

    System.exit(0);

  }

}