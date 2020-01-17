package controller;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.geomesa.example.quickstart.CommandLineDataStore;
import org.geotools.data.*;
import org.geotools.filter.FilterFactoryImpl;
import org.geotools.filter.SortByImpl;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore;
import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreFactory;
import org.locationtech.geomesa.accumulo.data.AccumuloQueryPlan;
import org.locationtech.geomesa.index.conf.QueryHints;
import org.geotools.data.Transaction;
import org.locationtech.geomesa.index.index.z2.Z2Index$;
import org.locationtech.geomesa.index.index.attribute.AttributeIndex;
import org.locationtech.geomesa.index.utils.ExplainString;

import org.locationtech.geomesa.index.planning.QueryPlanner.CostEvaluation$;
import org.locationtech.geomesa.index.utils.ExplainPrintln;
import org.locationtech.jts.geom.Point;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.filter.sort.SortBy;
import org.opengis.filter.sort.SortOrder;
import scala.Option;
import scala.collection.Seq;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
import java.util.*;

public class GeomesaSearch extends GeomesaBase{
    public static String typeName;
    public static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
    public static SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

    private final Map<String, String> params;
    private final TutorialData data;

    public GeomesaSearch(String[] args, DataAccessFactory.Param[] parameters, TutorialData data) throws ParseException {
        // parse the data store parameters from the command line
        Options options = createOptions(parameters);
        CommandLine command = CommandLineDataStore.parseArgs(getClass(), options, args);
        params = CommandLineDataStore.getDataStoreParams(command, options);
        this.data = data;
        typeName = data.getTypeName();
    }
    public List<SimpleFeature> searchEntries(String startDay, String endDay,
                                               String startTs, String endTs,
                                               String startRate, String endRate,
                                               String pointID, String areaString,
                                               String maxView,
                                               String sortBy, Boolean ascend) throws IOException {
        String queryCQL = getECQL(startDay, endDay, startTs, endTs, startRate, endRate, pointID, areaString);
        System.out.println(queryCQL);

        ByteArrayOutputStream result = null;
        List<SimpleFeature> queryResult = null;
        DataStore datastore;
        try {
            datastore = createDataStore(params);
            SimpleFeatureType sft = data.getSimpleFeatureType();
            sft.getUserData().put("geomesa.partition.scan.parallel", true);
//            sft.getDescriptor("day").getUserData().put("cardinality", "high");
//            sft.getDescriptor("rate").getUserData().put("cardinality", "low");
            createSchema(datastore, sft);
            System.out.println(sft.getUserData());

            Query query;
            if (queryCQL.length() > 0){
                query = new Query(typeName, ECQL.toFilter(queryCQL));
            }else {
                query = new Query(typeName);
            }

            // set max return features
            if(maxView != null && !"".equals(maxView)){
                query.setMaxFeatures(Integer.parseInt(maxView));
            }
            // sort by
            if (!sortBy.equals("")){
                FilterFactoryImpl ff = new FilterFactoryImpl();
                query.setSortBy(new SortBy[]{new SortByImpl(
                        ff.property(sortBy),
                        ascend ? SortOrder.ASCENDING : SortOrder.DESCENDING
                )});
            }

            // query index
//            query.getHints().put(QueryHints.QUERY_INDEX(), "attr:8:rate:coord");
            // query planning type
//            query.getHints().put(QueryHints.COST_EVALUATION(), CostEvaluation$.MODULE$.Stats());
            query.getHints().put(QueryHints.COST_EVALUATION(), CostEvaluation$.MODULE$.Index());

            // start query
            queryResult = queryFeatures(datastore, query);
            // Limit max number result
//            queryResult = queryResult.subList(0, Math.min(queryResult.size(), Integer.parseInt(maxView)));

        } catch (CQLException e) {
            throw new RuntimeException("Error creating filter:", e);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return queryResult;
    }

    public String getECQL(String startDay, String endDay,
                          String startTs, String endTs,
                          String startRate, String endRate,
                          String pointID, String areaString) {
        String queryCQL = "";
        if (startDay!=null && !startDay.equals("")) {
            queryCQL = queryCQL + "day >= " + startDay + " AND ";
        }
        if (endDay!=null && !endDay.equals("")) {
            queryCQL = queryCQL + "day <= " + endDay + " AND ";
        }

        if (startTs!=null && !startTs.equals("")) {
            queryCQL = queryCQL + "ts >= " + startTs + " AND ";
        }
        if (endTs!=null && !endTs.equals("")) {
            queryCQL = queryCQL + "ts <= " + endTs + " AND ";
        }

        if (startRate!=null && !startRate.equals("")) {
            queryCQL = queryCQL + "rate >= " + startRate + " AND ";
        }
        if (endRate!=null && !endRate.equals("")) {
            queryCQL = queryCQL + "rate <= " + endRate + " AND ";
        }

        if (pointID!=null && !pointID.equals("")) {
            queryCQL = queryCQL + "pointID = '" + pointID + "' AND ";
        }
        if (areaString!=null && !areaString.equals("")) {
            queryCQL = queryCQL + "CONTAINS(Polygon((" + polygonFormat(areaString) + ")), coord) AND ";
        }
        if (queryCQL.length() >= 4){
            queryCQL = queryCQL.substring(0, queryCQL.length() - 4); // remove the last AND
        }
        return queryCQL;
    }

    public String polygonFormat(String s) {
        String res = "";
        StringBuilder resBuilder = new StringBuilder();
        if (!s.equals("")) {
            String[] list1 = s.split(";");
            for (String pointStr : list1) {
                double x = Double.parseDouble(pointStr.split(",")[0]);
                double y = Double.parseDouble(pointStr.split(",")[1]);
                resBuilder.append(y);
                resBuilder.append(" ");
                resBuilder.append(x);
                resBuilder.append(",");
            }
            res = resBuilder.toString();
            res = res.substring(0, res.length() - 1);
        }
        return res;
    }

    public List<SimpleFeature> queryFeatures(DataStore datastore, Query query) throws IOException {
        List<SimpleFeature> queryFeatureList = new ArrayList<>();

        System.out.println("Running query " + ECQL.toCQL(query.getFilter()));
        if (query.getPropertyNames() != null) {
            System.out.println("Returning attributes " + Arrays.asList(query.getPropertyNames()));
        }
        if (query.getSortBy() != null) {
            SortBy sort = query.getSortBy()[0];
            System.out.println("Sorting by " + sort.getPropertyName() + " " + sort.getSortOrder());
        }

        // submit the query, and get back an iterator over matching features
        // use try-with-resources to ensure the reader is closed
        try (FeatureReader<SimpleFeatureType, SimpleFeature> reader =
                     datastore.getFeatureReader(query, Transaction.AUTO_COMMIT)) {
            int n = 0;
            while (reader.hasNext()) {
                SimpleFeature feature = reader.next();
                queryFeatureList.add(feature);
                n++;
//                System.out.println(feature);
            }
            System.out.println();
            System.out.println("Returned " + n + " total features");
            System.out.println();
        }

        return queryFeatureList;
    }

    public ByteArrayOutputStream featuresToByteArray(List<SimpleFeature> queryResult) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ByteArrayOutputStream resBaos = new ByteArrayOutputStream();

        resBaos.write(getBytes(queryResult.size()));

        for (SimpleFeature feature : queryResult) {
            String pointID = feature.getProperty("pointID").getValue().toString();
            String day  = feature.getProperty("day").getValue().toString();
            String ts = feature.getProperty("ts").getValue().toString();
            String rate = feature.getProperty("rate").getValue().toString();
            Point o = (Point) feature.getProperty("coord").getValue();
            double coord_x = o.getX();
            double coord_y = o.getY();

            baos.write(pointID.getBytes());
            baos.write(getBytes(Float.floatToIntBits(Float.parseFloat(day))));
            baos.write(getBytes(Float.floatToIntBits(Float.parseFloat(ts))));
            baos.write(getBytes(Float.floatToIntBits(Float.parseFloat(rate))));
            baos.write(getBytes(Float.floatToIntBits((float) coord_x)));
            baos.write(getBytes(Float.floatToIntBits((float) coord_y)));
        }

        resBaos.write(baos.toByteArray());

        return resBaos;
    }
    public static byte[] getBytes(int data) {
        byte[] bytes = new byte[4];
        bytes[0] = (byte) (data & 0xff);
        bytes[1] = (byte) ((data & 0xff00) >> 8);
        bytes[2] = (byte) ((data & 0xff0000) >> 16);
        bytes[3] = (byte) ((data & 0xff000000) >> 24);
        return bytes;
    }

    public String getStatistic(){
        DataStore datastore = null;
        String totalNum = "0";
        String pointNum = "0";
        String minmaxDate =  "{\"min\": 0, \"max\": 0}";
        try {
            datastore = createDataStore(params);
            SimpleFeatureType sft = data.getSimpleFeatureType();
            createSchema(datastore, sft);

            // get totalNum
            Query query = new Query(typeName);
            query.getHints().put(QueryHints.STATS_STRING(), "Count()");
            Type t1  = new TypeToken<Map<String,String>>(){}.getType();
            Map<String,String> list1 = new Gson().fromJson(startQuery(datastore, query), t1);
            totalNum = list1.get("count");
            System.out.println(totalNum);

            // get point Num
//            query = new Query(typeName);
//            query.getHints().put(QueryHints.STATS_STRING(), "GroupBy(\"pointID\",Count())");
//            Type t2  = new TypeToken<List<Map<String,Map<String,String>>>>(){}.getType();
//            List<Map<String,Map<String,String>>> list2 = new Gson().fromJson(startQuery(datastore, query), t2);
//            pointNum = String.valueOf(list2.size());
//            System.out.println(pointNum);

            // get min_maxStartTime
            query = new Query(typeName);
            query.getHints().put(QueryHints.STATS_STRING(), "MinMax(\"day\")");
            Type t3  = new TypeToken<Map<String,String>>(){}.getType();
            Map<String,String> list3 = new Gson().fromJson(startQuery(datastore, query), t3);
            String min = String.valueOf(list3.get("min"));
            String max = String.valueOf(list3.get("max"));
            minmaxDate = "{\"min\":" + min +", \"max\":" + max +"}";
            System.out.println(minmaxDate);

            return "{\"totalNum\":" + totalNum + ", \"pointNum\":" + pointNum + ", \"dayInfo\":" + minmaxDate + "}";
        }
        catch (Exception e) {
            e.printStackTrace();
            return "";
        }
    }
    public String startQuery(DataStore datastore, Query query){
        Seq<AccumuloQueryPlan> qps = ((AccumuloDataStore) datastore).getQueryPlan(query, null, null);
        System.out.println(qps);

        try (FeatureReader<SimpleFeatureType, SimpleFeature> reader =
                     datastore.getFeatureReader(query, Transaction.AUTO_COMMIT)) {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            String countJson = reader.next().getAttribute(0).toString();

            return countJson;
        }catch (Exception e) {
            e.printStackTrace();
            return "";
        }
    }

    public static void main(String[] args) throws IOException {
        try {
            args = new String[]{
                    "--accumulo.instance.id","hdp-accumulo-instance",
                    "--accumulo.zookeepers","a0.hdp:2181,a1.hdp:2181,a2.hdp:2181",
                    "--accumulo.user","geomesa",
                    "--accumulo.password", "geomesa",
                    "--accumulo.catalog","geomesa.geodata_insar"
            };

            long time1=System.currentTimeMillis();
            new GeomesaSearch(args, new AccumuloDataStoreFactory().getParametersInfo(), new InsarPointInfo())
                    .searchEntries("736703", "737703",
//                            "3.2", "3.8",
                            null, null,
                            null,null,
//                            "3.2", "3.8",
                            null,
                            "39.70,116.8998;39.7002,116.8998;39.7002,116.90;39.70,116.90;39.70,116.8998",
//                            "",
                            "",
                            "",true);
//            System.out.println( new GeomesaSearch(args, new AccumuloDataStoreFactory().getParametersInfo(), new InsarPointInfo())
//                    .getStatistic());
            long time2=System.currentTimeMillis();
            System.out.println(time2-time1);
        } catch (ParseException e) {
            System.exit(1);
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(2);
        }
        System.exit(0);
    }
}
