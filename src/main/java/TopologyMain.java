

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import bolts.AirlineSorter;
import bolts.HubIdentifier;
import spouts.FlightsDataReader;
import utility.AirportInformation;
import utility.FlightInformation;


public class TopologyMain {

	public static long start = 0L;

	public static long end = 0L;

//	public static long getStart() {
//		return start;
//	}

	public static void main(String[] args) throws InterruptedException, InvalidTopologyException, AlreadyAliveException {

		//A topology is a graph of spouts and bolts that are connected with stream groupings.
        //Topology definition
		/**
		 *  TopologyBuilder is used for building the topology
		 *  	creates spouts and bolts
		 */

		TopologyBuilder builder = new TopologyBuilder(); // use this class to construct topologies in Java
		builder.setSpout("flight-data-reader",new FlightsDataReader()); // setting the spout -done
		builder.setBolt("hub-identifier", new HubIdentifier(),1).shuffleGrouping("flight-data-reader"); // defining how to distribute it
		builder.setBolt("airline-sorter", new AirlineSorter(),1).shuffleGrouping("hub-identifier");

//		System.out.println("before setting the configurations");
        //Configuration
		Config conf = new Config();
		conf.put("FlightsFile", args[0]);
		conf.put("AirportsData", args[1]);
		conf.setDebug(false);
		conf.put(Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION, true);

		//registering bean with topology
		conf.registerSerialization(FlightInformation.class);
		conf.registerSerialization(AirportInformation.class);
//		StormSubmitter.submitTopology("MyTopologyName", conf, builder.createTopology());

		//Topology run
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
//		StormTopology st = new StormTopology();
		LocalCluster cluster = new LocalCluster();
		start = System.currentTimeMillis();
		System.out.println("start time is ----"+ start);
		cluster.submitTopology("Getting-Started-Toplogie", conf, builder.createTopology());
		Thread.sleep(10000);
//		end = System.currentTimeMillis();
//		System.out.println("----------total time taken ---------" + (end-start));
		cluster.shutdown();
//		System.out.println("after setting the configurations");
	}
}
