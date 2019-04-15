package com.wordpress.chapter10.simplewastemanagement;

import org.linqs.psl.application.inference.InferenceApplication;
import org.linqs.psl.application.inference.MPEInference;
import org.linqs.psl.config.Config;
import org.linqs.psl.database.Database;
import org.linqs.psl.database.DataStore;
import org.linqs.psl.database.Partition;
import org.linqs.psl.database.loading.Inserter;
import org.linqs.psl.database.rdbms.driver.H2DatabaseDriver;
import org.linqs.psl.database.rdbms.driver.H2DatabaseDriver.Type;
import org.linqs.psl.database.rdbms.driver.PostgreSQLDriver;
import org.linqs.psl.database.rdbms.RDBMSDataStore;
import org.linqs.psl.evaluation.statistics.DiscreteEvaluator;
import org.linqs.psl.evaluation.statistics.Evaluator;
import org.linqs.psl.groovy.PSLModel;
import org.linqs.psl.model.atom.GroundAtom;
import org.linqs.psl.model.term.Constant;
import org.linqs.psl.model.term.ConstantType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;


public class Run {
	
	private static final String PARTITION_OBSERVATIONS = "observations";
	private static final String PARTITION_TARGETS = "targets";
	private static final String PARTITION_TRUTH = "truth";

	private static final String BASEDB_PATH = Config.getString("dbpath", System.getProperty("user.dir"));
	private static final String DATA_PATH = Paths.get(BASEDB_PATH, "data").toString();
	private static final String OUTPUT_PATH = "inferred-predicates";

	private static Logger log = LoggerFactory.getLogger(Run.class)

	private DataStore dataStore;
	private PSLModel model;
	
	
	public Run() {
		String suffix = System.getProperty("user.name") + "@" + getHostname();
		String dbPath = Paths.get(BASEDB_PATH, this.getClass().getName() + "_" + suffix).toString();
		dataStore = new RDBMSDataStore(new H2DatabaseDriver(Type.Disk, dbPath, true));
		// dataStore = new RDBMSDataStore(new PostgreSQLDriver("psl", true));
		
		log.info("Data Path: " + DATA_PATH);
		log.info("DB Path  : " + dbPath);

		model = new PSLModel(this, dataStore);
	}
	
	
	/**
	 * Defines the logical predicates used in this model
	 */
	private void definePredicates() {
		// evidence
		model.add predicate: "Bin", types: [ConstantType.UniqueStringID, ConstantType.UniqueStringID];
		model.add predicate: "IsFull", types: [ConstantType.UniqueStringID];
		
		
		model.add predicate: "LocIsFull", types: [ConstantType.UniqueStringID];
		
		// function
		// model.add function: "LocIsFull", implementation: new LocationCapacity(dataStore.getPartition(PARTITION_OBSERVATIONS));
		
		// query
		model.add predicate: "SendTruck", types: [ConstantType.UniqueStringID];
		
		
	}
	
	
	/**
	 * Defines the rules for this model.
	 */
	private void defineRules() {
		log.info("Defining model rules");

		
		model.add(
			rule: "10.0: Bin(Bid, Loc) & IsFull(Bid) & LocIsFull(Loc) -> SendTruck(Loc) ^2"
		);
		
		
		model.add(
			rule: "1.0: Bin(Bid, Loc) & IsFull(Bid) -> SendTruck(Loc) ^2"
		);

		model.add(
			rule: "5.0: Bin(Bid1, Loc) & Bin(Bid2, Loc) & (Bid1 != Bid2) & IsFull(Bid1) & IsFull(Bid2) -> SendTruck(Loc) ^2"
		);

		model.add(
			rule: "1.0: !SendTruck(Loc) ^2"
		);

		log.info("model: {}", model);
	}
	
	/**
	 * Load data from text files into the DataStore.
	 * Three partitions are defined and populated: observations, targets, and truth.
	 * Observations contains evidence that we treat as background knowledge and use to condition our inferences.
	 * Targets contains the inference targets - the unknown variables we wish to infer.
	 * Truth contains the true values of the inference variables and will be used to evaluate the model's performance.
	 */
	private void loadData(Partition obsPartition, Partition targetsPartition, Partition truthPartition) {
		log.info("Loading data into database");

		Inserter inserter = dataStore.getInserter(Bin, obsPartition);
		inserter.loadDelimitedData(Paths.get(DATA_PATH, "bin_obs.txt").toString());

		inserter = dataStore.getInserter(IsFull, obsPartition);
		inserter.loadDelimitedDataTruth(Paths.get(DATA_PATH, "isfull_obs.txt").toString());

		
		inserter = dataStore.getInserter(LocIsFull, obsPartition);
		inserter.loadDelimitedDataTruth(Paths.get(DATA_PATH, "locisfull_obs.txt").toString());
		
		//inserter = dataStore.getInserter(Knows, obsPartition);
		//inserter.loadDelimitedData(Paths.get(DATA_PATH, "knows_obs.txt").toString());

		inserter = dataStore.getInserter(SendTruck, targetsPartition);
		inserter.loadDelimitedData(Paths.get(DATA_PATH, "send_truck_targets.txt").toString());

		//inserter = dataStore.getInserter(Knows, truthPartition);
		//inserter.loadDelimitedDataTruth(Paths.get(DATA_PATH, "knows_truth.txt").toString());
	}
	
	
	/**
	 * Run inference to infer the unknown Knows relationships between people.
	 */
	private void runInference(Partition obsPartition, Partition targetsPartition) {
		log.info("Starting inference");

		Database inferDB = dataStore.getDatabase(targetsPartition, [Bin, IsFull] as Set, obsPartition);

		InferenceApplication inference = new MPEInference(model, inferDB);
		inference.inference();

		inference.close();
		inferDB.close();

		log.info("Inference complete");
	}
	
	/**
	 * Writes the output of the model into a file
	 */
	private void writeOutput(Partition targetsPartition) {
		Database resultsDB = dataStore.getDatabase(targetsPartition);

		(new File(OUTPUT_PATH)).mkdirs();
		FileWriter writer = new FileWriter(Paths.get(OUTPUT_PATH, "SendTruck.txt").toString());

		for (GroundAtom atom : resultsDB.getAllGroundAtoms(SendTruck)) {
			for (Constant argument : atom.getArguments()) {
				writer.write(argument.toString() + "\t");
			}
			writer.write("" + atom.getValue() + "\n");
		}

		writer.close();
		resultsDB.close();
	}
	
	
	public void run() {
		Partition obsPartition = dataStore.getPartition(PARTITION_OBSERVATIONS);
		Partition targetsPartition = dataStore.getPartition(PARTITION_TARGETS);
		Partition truthPartition = dataStore.getPartition(PARTITION_TRUTH);

		definePredicates();
		defineRules();
		loadData(obsPartition, targetsPartition, truthPartition);
		runInference(obsPartition, targetsPartition);
		writeOutput(targetsPartition);

		dataStore.close();
	}
	
	/**
	 * Run this model from the command line
	 * @param args - the command line arguments
	 */
	public static void main(String[] args) {
		
		Run run = new Run();
		run.run();
	}
	
	
	private static String getHostname() {
		String hostname = "unknown";

		try {
			hostname = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException ex) {
			log.warn("Hostname can not be resolved, using '" + hostname + "'.");
		}

		return hostname;
	}
}