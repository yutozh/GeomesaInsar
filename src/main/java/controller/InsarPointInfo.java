package controller;

import app.ServerSearch;
import org.geotools.data.Query;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.geotools.util.factory.Hints;
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import scala.Array;
import scala.util.parsing.input.StreamReader;

public class InsarPointInfo implements TutorialData {
    private static final Logger logger = LoggerFactory.getLogger(InsarPointInfo.class);

    private SimpleFeatureType sft = null;
    private List<SimpleFeature> features = null;
    private List<Query> queries = null;
    private Filter subsetFilter = null;

    String path_day = "D:\\Experiment\\point_db\\beijing\\day.txt";
    String path_rate = "D:\\Experiment\\point_db\\beijing\\def_rate.txt";
    String path_ts = "D:\\Experiment\\point_db\\beijing\\def_ts.txt";
    String path_lonlat = "D:\\Experiment\\point_db\\beijing\\lonlat.txt";

    BufferedReader brRate;
    BufferedReader brTs;
    BufferedReader brLonlat;

    String[] daysList;

    private int allEntryID = 1;
    private int allLineID = 1;

    public InsarPointInfo() throws FileNotFoundException {
//        brRate = new BufferedReader(new FileReader(path_rate));
//        brTs = new BufferedReader(new FileReader(path_ts));
//        brLonlat = new BufferedReader(new FileReader(path_lonlat));

        try {
            // Read days
            InputStream is = ServerSearch.class.getClassLoader().getResourceAsStream("day.txt");

//            InputStream is = new FileInputStream(path_day);
            int iAvail = is.available();
            byte[] bytes = new byte[iAvail];
            is.read(bytes);
            String days = new String(bytes);
            daysList = days.split("\r\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String getTypeName() {
        return "insar-point-info";
    }

    public SimpleFeatureType getSimpleFeatureType() {
        if (sft == null) {
            // list the attributes that constitute the feature type
            // this is a reduced set of the attributes from GDELT 2.0

            // create the simple-feature type - use the GeoMesa 'SimpleFeatureTypes' class for best compatibility
            // may also use geotools DataUtilities or SimpleFeatureTypeBuilder, but some features may not work
            String attributes = "pointID:String:index=true," +
                    "day:Integer:index=true," +
                    "ts:Double:index=true," +
                    "*coord:Point:srid=4326," +
                    "rate:Double:index=true";
            sft = SimpleFeatureTypes.createType(getTypeName(), attributes);

            // use the user-data (hints) to specify which date field to use for primary indexing
            // if not specified, the first date attribute (if any) will be used
            // could also use ':default=true' in the attribute specification string
//            sft.getUserData().put(SimpleFeatureTypes.DEFAULT_DATE_KEY, "dtg");
        }
        return sft;
    }

    public List<SimpleFeature> getTestData(){
        if (features == null) {
            List<SimpleFeature> features = new ArrayList<>();
            String path_day = "D:\\Experiment\\point_db\\beijing\\day.txt";
            String path_rate = "D:\\Experiment\\point_db\\beijing\\def_rate.txt";
            String path_ts = "D:\\Experiment\\point_db\\beijing\\def_ts.txt";
            String path_lonlat = "D:\\Experiment\\point_db\\beijing\\lonlat.txt";

            try {
                // Read days
                InputStream is = new FileInputStream(path_day);
                int iAvail = is.available();
                byte[] bytes = new byte[iAvail];
                is.read(bytes);
                String days = new String(bytes);
                String[] daysList = days.split("\r\n");

                BufferedReader brRate = new BufferedReader(new FileReader(path_rate));
                BufferedReader brTs = new BufferedReader(new FileReader(path_ts));
                BufferedReader brLonlat = new BufferedReader(new FileReader(path_lonlat));
                String strLine;
                int entryID = 1;
                int lineID = 1;
                while (null != (strLine = brLonlat.readLine())) {
                    String lon = strLine.split(",")[0];
                    String lat = strLine.split(",")[1];

                    String rate = brRate.readLine();

                    String tsLine = brTs.readLine();
                    int tsCount = 0;
                    for(String ts : tsLine.split(",")){
                        SimpleFeature sf = geneSimpleFeature(String.valueOf(entryID), daysList[tsCount], ts, lon, lat, rate);
                        features.add(sf);
                        entryID++;
                        tsCount++;
                    }
                    lineID ++;
                    if(lineID % 10000 == 0){
                        System.out.println(lineID);
                    }
                }
                this.features = Collections.unmodifiableList(features);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return features;
    }

    public List<SimpleFeature> getNextData(int lines){
        List<SimpleFeature> features = new ArrayList<>();
        try {
            String strLine;
            int viewdLines = 0;
            while (viewdLines < lines){
                if(null != (strLine = brLonlat.readLine())){
                    String lon = strLine.split(",")[0];
                    String lat = strLine.split(",")[1];

                    String rate = brRate.readLine();
                    String tsLine = brTs.readLine();
                    int tsCount = 0;
                    for(String ts : tsLine.split(",")){
                        SimpleFeature sf = geneSimpleFeature(String.valueOf(allLineID), daysList[tsCount], ts, lon, lat, rate);
                        features.add(sf);
                        allEntryID++;
                        tsCount++;
                    }
                    allLineID ++;
                    if(allLineID % 10000 == 0){
                        System.out.println(allLineID);
                    }
                }
                viewdLines ++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return features;
    }

    public SimpleFeature geneSimpleFeature(String pointID, String day, String ts, String lon, String lat, String rate){
        SimpleFeatureBuilder builder = new SimpleFeatureBuilder(getSimpleFeatureType());
        builder.set("pointID", String.valueOf(pointID));
        builder.set("day", Integer.valueOf(day));
        builder.set("ts", Double.valueOf(ts));
        builder.set("coord", "POINT (" + lon + " " + lat + ")");
        builder.set("rate", Double.valueOf(rate));

        // be sure to tell GeoTools explicitly that we want to use the ID we provided
//        builder.featureUserData(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE);

        // build the feature - this also resets the feature builder for the next entry
        // use the GLOBALEVENTID as the feature ID

        return builder.buildFeature(null);
    }

    @Override
    public List<Query> getTestQueries() {
        if (queries == null) {
            try {
                List<Query> queries = new ArrayList<>();

                // most of the data is from 2018-01-01
                // note: DURING is endpoint exclusive
                String during = "startTime DURING 2014-01-31T07:00:00.000Z/2018-01-02T07:00:00.000Z";
                // bounding box over most of the united states
                String bbox = "bbox(startPoint,114.26,30.57 , 114.28, 30.59)";

                // basic spatio-temporal query
                queries.add(new Query(getTypeName(), ECQL.toFilter(bbox + " AND " + during)));
                // basic spatio-temporal query with projection down to a few attributes
                queries.add(new Query(getTypeName(), ECQL.toFilter(bbox + " AND " + during),
                        new String[]{ "startTime", "endTime", "startPoint", "endPoint" }));
                // attribute query on a secondary index - note we specified index=true for EventCode
                queries.add(new Query(getTypeName(), ECQL.toFilter("carID = 'MMC8000GPSANDASYN051113-22239-00000000'")));
                // attribute query on a secondary index with a projection
                queries.add(new Query(getTypeName(), ECQL.toFilter("carID = 'MMC8000GPSANDASYN051113-22239-00000000' AND " + during),
                        new String[]{ "startTime", "endTime", "startPoint", "endPoint" }));

                this.queries = Collections.unmodifiableList(queries);
            } catch (CQLException e) {
                throw new RuntimeException("Error creating filter:", e);
            }
        }
        return queries;
    }

    @Override
    public Filter getSubsetFilter() {
        if (subsetFilter == null) {
            // Get a FilterFactory2 to build up our query
            FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();

            // most of the data is from 2018-01-01
            ZonedDateTime dateTime = ZonedDateTime.of(2018, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
            Date start = Date.from(dateTime.minusDays(1).toInstant());
            Date end = Date.from(dateTime.plusDays(1).toInstant());

            // note: BETWEEN is inclusive, while DURING is exclusive
            Filter dateFilter = ff.between(ff.property("dtg"), ff.literal(start), ff.literal(end));

            // bounding box over small portion of the eastern United States
            Filter spatialFilter = ff.bbox("geom",-83,33,-80,35,"EPSG:4326");

            // Now we can combine our filters using a boolean AND operator
            subsetFilter = ff.and(dateFilter, spatialFilter);

            // note the equivalent using ECQL would be:
            // ECQL.toFilter("bbox(geom,-83,33,-80,35) AND dtg between '2017-12-31T00:00:00.000Z' and '2018-01-02T00:00:00.000Z'");
        }
        return subsetFilter;
    }


    public static void main(String[] args) throws FileNotFoundException {
        new InsarPointInfo().getTestData();
    }

    public static float geoLongToFloat(String s) {
        return Integer.parseInt(s)/1000000.0f;
    }
}
