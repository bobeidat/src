
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class FileReaderSpout implements IRichSpout {
  private SpoutOutputCollector _collector;
  private TopologyContext context;
  private String fileName;
  BufferedReader reader;

  @Override
  public void open(Map conf, TopologyContext context,
                   SpoutOutputCollector collector) {

     /*
    ----------------------TODO-----------------------
    Task: initialize the file reader


    ------------------------------------------------- */
	  
      try {
          reader = new BufferedReader(new FileReader(fileName));
      } catch (FileNotFoundException e) {
          e.printStackTrace();
      }
      

    this.context = context;
    this._collector = collector;
  }
  public FileReaderSpout(String fileName) {
		this.fileName = fileName;
	}
  @Override
  public void nextTuple() {

     /*
    ----------------------TODO-----------------------
    Task:
    1. read the next line and emit a tuple for it
    2. don't forget to sleep when the file is entirely read to prevent a busy-loop

    ------------------------------------------------- */
	  String nextLine = null;
      try {
          nextLine = reader.readLine();
      } catch (IOException e) {
          e.printStackTrace();
      }
      if(nextLine != null){
          _collector.emit(new Values(nextLine));
      }
      else {
    	  //wait for 60 seconds and then kill the topology
    	    Thread.sleep(60 * 1000);
      }
  }


  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

    declarer.declare(new Fields("word"));

  }

  @Override
  public void close() {
   /*
    ----------------------TODO-----------------------
    Task: close the file


    ------------------------------------------------- */
	  if (reader == null) return;
		try {
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
			
		}
  }


  @Override
  public void activate() {
  }

  @Override
  public void deactivate() {
  }

  @Override
  public void ack(Object msgId) {
  }

  @Override
  public void fail(Object msgId) {
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}
