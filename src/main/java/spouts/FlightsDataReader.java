package spouts;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import utility.FlightInformation;

//	i) BaseRichSpout: a spout super class
public class FlightsDataReader extends BaseRichSpout {

	private SpoutOutputCollector collector; // for emitting the output tuples
	private FileReader fileReader;
	private boolean completed = false;

	private List<FlightInformation> fi;

	public void ack(Object msgId) {
		System.out.println("OK:" + msgId);
	}

	public void close() {
	}

	public void fail(Object msgId) {
		System.out.println("FAIL:" + msgId);
	}

	/**
	 * The only thing that the methods will do It is emit each
	 * file line
	 * called continously by the spout task
	 * /collector emit has 3 overloaded methods
	 */
	public void nextTuple() {

		if (completed) {
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				//Do nothing
			}
			return;
		}
		try {
				fi = creatingFlightList();
				for (int i = 0; i < fi.size(); i++) {


//				System.out.println("fdr -- isLongitudeNull is " + (fi.get(i).getLongitude() )  + "   isLatitudeNull is " + fi.get(i).getLatitude()  );
				FlightInformation individualFlightDetails = fi.get(i);
//				System.out.println("velocity is " + fi.get(i).getVelocity());
				this.collector.emit(new Values(
												individualFlightDetails.getTransponderAddress(),
												individualFlightDetails.getCallSign(),
												individualFlightDetails.getOriginCountry(),
												individualFlightDetails.getStartTimestamp(),
												individualFlightDetails.getLastTimestamp(),
												individualFlightDetails.getLongitude(),
												individualFlightDetails.getLatitude(),
												individualFlightDetails.getAltitude(),
												individualFlightDetails.getIsSurface(),
												individualFlightDetails.getVelocity(),
												individualFlightDetails.getDegree(),
												individualFlightDetails.getVerticalRate(),
												individualFlightDetails.getSensors(),
												individualFlightDetails.getAltitudeGeometric(),
												individualFlightDetails.getTransponderCode(),
												individualFlightDetails.getIsSpecialPurpose(),
												individualFlightDetails.getOrigin()
												)
							);
			}
		}catch(Exception e){
				throw new RuntimeException("Error reading tuple",e);
		}finally{
				completed = true;
		}
	}

	/**
	 * We will create the file and get the collector object
	 * called when the spout task is initialized
	 *
	 */
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		try {
			this.fileReader = new FileReader(conf.get("FlightsFile").toString());
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Error reading file ["+conf.get("FlightsFile")+"]");
		}

		this.collector = collector; // collector initialized
	}

	/**
	 * Declare the output field "word"
	 * called once for each spout/bolt task
	 * used to specify the schema for the fields
	 *
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {


		declarer.declare(new Fields("transponderAddress",
									"callSign",
									"originCountry",
									"firstTimestamp",
									"lastTimestamp",
									"longitude",
									"latitude",
									"altitudeBarometric",
									"SurfaceOrAir",
									"velocity",
									"degreeNorth",
									"verticalRate",
									"sensors",
									"altitudeGeometric",
									"transponderCode",
									"specialPurpose",
									"origin"
									)
						);

	}
	private List<FlightInformation> creatingFlightList() {
		JSONParser parser = new JSONParser();
		List<FlightInformation> fi = new ArrayList<>();
		try {
			Object obj = parser.parse(fileReader);
			// A JSON object. Key value pairs are unordered. JSONObject supports java.util.Map interface.
			JSONObject jsonObject = (JSONObject) obj;
			// A JSON array. JSONObject supports java.util.List interface.
			JSONArray flightList = (JSONArray) jsonObject.get("states");

//			System.out.println(flightList);
			for (int i = 0; i < flightList.size(); i++) {
				JSONArray innerArray = (JSONArray) flightList.get(i);

				FlightInformation flightDetails = new FlightInformation(
						String.valueOf(innerArray.get(0))  , // transponder
						String.valueOf(innerArray.get(1)), // call sign
						String.valueOf(innerArray.get(2)), // origin country
						String.valueOf(innerArray.get(3)), // start t.s
						String.valueOf(innerArray.get(4)), // last t.s
						String.valueOf(innerArray.get(5)), // longi
						String.valueOf(innerArray.get(6)), // lati
						String.valueOf(innerArray.get(7)),// altitude
						String.valueOf(innerArray.get(8)), // isSurface
						String.valueOf(innerArray.get(9)),  // velocity
						String.valueOf(innerArray.get(10)), //degree
						String.valueOf(innerArray.get(11)),  //vertical rate
						String.valueOf(innerArray.get(12)),  // sensors
						String.valueOf(innerArray.get(13)),  // altitude geo
						String.valueOf(innerArray.get(14)),   // transponder code
						String.valueOf(innerArray.get(15)),  // isSpeicalpurpose
						String.valueOf(innerArray.get(16)) //origin
				);
				fi.add(flightDetails);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return fi;
	}


}
