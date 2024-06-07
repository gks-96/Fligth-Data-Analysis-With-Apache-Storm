package bolts;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import utility.AirportInformation;

import java.io.FileReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
	Interfaces are defined for spouts and bolts :
	i) IRichSpout: a spout interface
	ii) IRichBolt : a bolt interface

	Classes implementing the above interfaces
	i) BaseRichSpout: a spout super class
	ii) BaseBasicBolt : a bolt super class
 */
public class AirlineSorter extends BaseBasicBolt {

	Integer id;
	String name;

//	private long  timeTakenInAirlineSorter = 0L;
	// airport code, list of all flights with code sign near that airport
	Map<String,Map<String, Integer>> counters;


	/**
	 * At the end of the spout (when the cluster is shutdown
	 * We will show the word counters
	 */
	@Override
	public void cleanup() {

		for ( Map.Entry<String, Map<String, Integer>> temp : counters.entrySet())
		{
			System.out.println( temp.getKey());
			int sum = 0;
			if ( temp.getValue().size() != 0 )  // meaning one element in the square, then print it
			{
				for (Map.Entry<String, Integer> innerMap : temp.getValue().entrySet()) {
					System.out.println(innerMap.getKey() + ":" + innerMap.getValue());
					sum += innerMap.getValue();
				}
				System.out.println("total # flights = " + sum); //  the map sum
			}
		}
	}

	/**
	 * On create
	 * called when the bolt task is initialized
	 * called once for each bolt task
	 * used for initializing purpose
	 */
	@Override
	public void prepare(Map stormConf, TopologyContext context) {

		System.out.println(counters);
		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();

	}



	// this does not do anything - does not transfer data forward
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {}

	/*
		called continously by the bolt task for each tuple for the task
		processing of the tuple is done in this method

		@param: input is the tuple to be processed
		@param: collecotr is used for emitting the tuples- since this is the last bolt, we are not using the collector in here
	 */
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {

		Double longitude = Double.parseDouble(input.getStringByField("longitude"));
		Double latitude = Double.parseDouble(input.getStringByField("longitude"));
		String flightCode = input.getStringByField("flightCallSign");

		int longitudeRounded = Integer.parseInt(String.valueOf(Math.floor(longitude)));
		int latitudeRounded = Integer.parseInt(String.valueOf(Math.floor(latitude)));

		String key = "In Area: " + latitudeRounded + "," + longitudeRounded;


		if ( counters.containsKey(key))
		{
			Map<String,Integer> temp = counters.get(key);
			temp.put(flightCode,temp.getOrDefault(flightCode,0)+1);
		}
		else{
			Map<String,Integer> temp = new HashMap<>();
			temp.put(flightCode,temp.getOrDefault(flightCode,0)+1);
			counters.put(key,temp);
		}
	}


}
