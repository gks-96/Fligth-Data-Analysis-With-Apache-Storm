package bolts;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import utility.AirportInformation;
import utility.FlightInformation;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
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
public class HubIdentifier extends BaseBasicBolt {

	private final Integer latitudeChangePerDegree = 70;
	private final Integer longitudeChangePerDegree = 45;

	public long timeTakenInHubIdentifier = 0;
	private FileReader fileReader;
	List<AirportInformation> airportInformation;
	long start = 0L;
	long end = 0L;
	/**
	 * At the end of the spout (when the cluster is shutdown
	 * We will show the word counters
	 */
	@Override
	public void cleanup() {

	}

	/**
	 * On create
	 * called when the bolt task is initialized
	 * called once for each bolt task
	 * used for initializing purpose
	 */
	@Override
	public void prepare(Map stormConf, TopologyContext context) {

		try {
			this.fileReader = new FileReader(stormConf.get("AirportsData").toString());
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Error reading file [" + stormConf.get("AirportsData") + "]");
		}
		airportInformation = creatingAirportInformation();

	}
	/*
     called continously by the bolt task for each tuple for the task
     processing of the tuple is done in this method
     @param: input is the tuple to be processed
     @param: collecotr is u   sed for emitting the tuples- since this is the last bolt, we are not using the collector in here
     */
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {

		String longitude = input.getStringByField("longitude");
		String latitude = input.getStringByField("latitude");

		String callSign =  input.getStringByField("callSign");
		callSign = callSign.trim();


		boolean callSignCheckPass = !callSign.equals("")  &&  !callSign.equals("null");
		if( callSignCheckPass)
		{
			callSign = callSign.substring(0,Math.min(3,callSign.length()));
		}
//		System.out.println("verticalRate is---" + verticalRate + "  velocityString   "+ velocityString);

		/**
		 * check if this value can be transmitted
		 * if it can be
		 * if null ,then why is it being called ?
		 */
//		if ( lon)
		boolean isLongitudeNull =  (longitude.equals("null")) ;
		boolean isLatitudeNull =  (latitude.equals("null") ) ;
//		System.out.println("isLongitudeNull is " + isLongitudeNull + "   isLatitudeNull is " + isLatitudeNull);

		if ( !isLongitudeNull  &&  !isLatitudeNull  &&callSignCheckPass) {
			collector.emit( new Values( latitude,longitude, callSign));
			}
	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		declarer.declare(new Fields(
									"latitude",
									"longitude",
				 					"flightCallSign"
									)
						);
	}

	private List<AirportInformation> creatingAirportInformation() {
		String str;
		//Open the reader
		BufferedReader br = new BufferedReader(fileReader);
		//Read all lines
		List<AirportInformation> ai = new ArrayList<>();
		try {

			while ((str = br.readLine()) != null) {
				String[] airportDetails = str.split(",");
				AirportInformation airport = new AirportInformation(
						airportDetails[0],
						airportDetails[1],
						Double.parseDouble(airportDetails[2]), Double.parseDouble(airportDetails[3]));
				ai.add(airport);
				br.readLine();
			}
		} catch (Exception e) {
		}
		return ai;
	}

}