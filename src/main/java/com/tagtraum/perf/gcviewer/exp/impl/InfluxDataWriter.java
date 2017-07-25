package com.tagtraum.perf.gcviewer.exp.impl;

import com.tagtraum.perf.gcviewer.exp.AbstractDataWriter;
import com.tagtraum.perf.gcviewer.model.AbstractGCEvent;
import com.tagtraum.perf.gcviewer.model.GCEvent;
import com.tagtraum.perf.gcviewer.model.GCModel;
import com.tagtraum.perf.gcviewer.util.FormattedValue;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Created with IntelliJ IDEA.
 * author: Ivan_Kalachyev
 */
public class InfluxDataWriter extends AbstractDataWriter {

	private static final Logger LOG = Logger.getLogger(InfluxDataWriter.class.getName());

	//Influx connection constants
	public static final String INFLUX_HOST = "influx.host";
	public static final String INFLUX_PORT = "influx.port";
	public static final String INFLUX_USER = "influx.user";
	public static final String INFLUX_PWD = "influx.pwd";

	//
	public static final String GC_TIME_SERIES = "gcTimeSeries";
	public static final String GC_SUMMARY = "gcSummary";

	// Env constants
	public static final String BUILD_TAG = "build";
	public static final String TEST_TYPE = "test";
	public static final String APP_SERVER = "app";
	public static final String SERVER = "server"; // STG PRF DEV

	public static final String TEST_DEFAULT = "WEB";

	InfluxDB influxDB;

	/** Influx tag that we should add and use */
	Map<String, String> tags = new HashMap<>();
	private ISummaryExportFormatter formatter;

	public InfluxDataWriter(OutputStream outputStream) {

		super(outputStream,
				new HashMap<String, Object>() { {
						put(ISummaryExportFormatter.NAME, new InfluxSummaryExportFormatter());
					}
				});
		formatter = new InfluxSummaryExportFormatter();
		// import using influx java api
		initInfluxConnection();

		//init env specific props
		initEnvProperties();
	}

	private void initEnvProperties() {
		String app    = System.getProperty(APP_SERVER);
		String test   = System.getProperty(TEST_TYPE);
		String build  = System.getProperty(BUILD_TAG);
		String server = System.getProperty(SERVER);

		getTags().put(TEST_TYPE, test != null ? test : TEST_DEFAULT);
		getTags().put(BUILD_TAG, build  != null ? build: BUILD_TAG );
		getTags().put(APP_SERVER, app != null ? app : APP_SERVER );
		getTags().put(SERVER, server != null ? server : SERVER );

	}

	@Override
	public void write(GCModel model) throws IOException {
		//export summary for memory pauses etc
		exportGCSummary(model);

		//export all the events that we have parsed
		exportGCEvents(model);
	}

	                                                
	private void exportGCEvents(GCModel model) {
		BatchPoints batchPoints = BatchPoints
				.database(GC_TIME_SERIES)
				.consistency(InfluxDB.ConsistencyLevel.ALL)
				.build();
		long start = System.currentTimeMillis();
		Iterator eventsIter = model.getEvents();
		while (eventsIter.hasNext()){
			AbstractGCEvent abstractGCEvent = (AbstractGCEvent) eventsIter.next();

			int total = 0;
			int youngPre = 0;
			int youngPost= 0;
			int youngTotal = 0;
			int tenuredPre = 0;
			int tenuredPost= 0;
			int tenuredTotal = 0;

			if (abstractGCEvent instanceof GCEvent) {
				GCEvent event = (GCEvent) abstractGCEvent;
				youngTotal = event.getYoung().getTotal();
				youngPre = event.getYoung().getPreUsed();
				youngPost = event.getYoung().getPostUsed();

				if (event.getTenured() != null){
					tenuredTotal = event.getTenured().getTotal();
					tenuredPre = event.getTenured().getPreUsed();
					tenuredPost = event.getTenured().getPostUsed();
				}
			}
			long millis = System.currentTimeMillis();
			if (abstractGCEvent.getDatestamp() != null) {
				millis =  abstractGCEvent.getDatestamp().toInstant().toEpochMilli();
			}
			// filter "application stopped" events
			Point point1 = Point.measurement("gc")
					.tag(getTags())
					.time(millis, TimeUnit.MILLISECONDS)
					.tag("name", abstractGCEvent.getExtendedType().getName())
					.tag("generation", abstractGCEvent.getExtendedType().getGeneration().name())
					.tag("collection", abstractGCEvent.getExtendedType().getConcurrency().name())
					.field("pause", abstractGCEvent.getPause())
					.field("total", total)
					.field("yTotal", youngTotal)
					.field("yPre", youngPre)
					.field("yPost", youngPost)
					.field("tPre", tenuredPre)
					.field("tPost", tenuredPost)
					.field("tTotal", tenuredTotal)
					.build();

			System.out.println(point1);
			batchPoints.point(point1);
		}

		influxDB.write(batchPoints);
	}



	private void exportGCSummary(GCModel model) {
		BatchPoints batchPoints = BatchPoints
				.database(GC_SUMMARY)
				.consistency(InfluxDB.ConsistencyLevel.ALL)
				.build();
		//batchPoints.point(exportPauseSummary(model));
		batchPoints.point(exportOverallSummary(model));
		influxDB.write(batchPoints);
	}

	private Point exportOverallSummary(GCModel model) {
		Map<String, Object> fieldsToAdd = new HashMap<>();
		fieldsToAdd.put("gcEvents", model.size());
		fieldsToAdd.put("gcEvents", model.size());

		//pause in seconds
		fieldsToAdd.put("totalGCPause", model.getPause().getSum());
		fieldsToAdd.put("pauseMin", model.getPause().getMin());
		fieldsToAdd.put("pauseMax", model.getPause().getMax());
		fieldsToAdd.put("pauseAvg", model.getPause().average());

		//// pause information about all stw events for detailed output
		//fieldsToAdd.put("gcPauseMin", model.getGcEventPauses().size());

		//Full GC events
		fieldsToAdd.put("fullGCNum", model.getFullGCPause().getN());
		fieldsToAdd.put("fullGCSum", model.getFullGCPause().getSum());

		//Memory KB
		fieldsToAdd.put("freedMemory",model.getFreedMemory());
		fieldsToAdd.put("freedMemoryByGC",model.getFreedMemoryByGC().getSum());
		fieldsToAdd.put("freedMemoryFullGC",model.getFreedMemoryByFullGC().getSum());
		fieldsToAdd.put("footprint", model.getFootprint());

		// %
		fieldsToAdd.put("throughput", model.getThroughput());

		Point point1 = Point.measurement("gcSum")
				.tag(getTags())
				.tag("summary", "overall")
				.fields(fieldsToAdd)
				.build();

		System.out.println(point1);
		return point1;

	}


	private boolean isSignificant(final double average, final double standardDeviation) {
		// at least 68.3% of all points are within 0.75 to 1.25 times the average value
		// Note: this may or may not be a good measure, but it at least helps to mark some bad data as such
		return average-standardDeviation > 0.75 * average;
	}

	private double isSignificant(GCModel model) {
		int n = model.getPause().getN();
		double avg = model.getPause().average();
		double deviation = model.getPause().standardDeviation();
		double edge = avg + deviation;

		Iterator<GCEvent> iter = model.getGCEvents();
		double badEvents = 0;
		while (iter.hasNext()){
			GCEvent gce = iter.next();
			double pause = gce.getPause();
			if (pause > edge) {
				badEvents++;
			}
		}

		return badEvents;
	}

	private void initInfluxConnection() {
		String host = System.getProperty(INFLUX_HOST);
		String port = System.getProperty(INFLUX_PORT);
		String user = System.getProperty(INFLUX_USER);
		String pwd  = System.getProperty(INFLUX_PWD);

		influxDB = InfluxDBFactory.connect("http://"  + host+ ":" + port , user, pwd);
		String dbName = GC_TIME_SERIES;

		List dbs = influxDB.describeDatabases();
		if (!dbs.contains(dbName)){
			influxDB.createDatabase(dbName);
		}
		if (!dbs.contains(GC_SUMMARY)){
			influxDB.createDatabase(dbName);
		}
	}

	public Map<String, String> getTags() {
		return tags;
	}
}
