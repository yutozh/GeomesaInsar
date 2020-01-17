package controller;


import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import org.geomesa.example.quickstart.CommandLineDataStore;
import org.geotools.data.*;
import org.geotools.data.simple.SimpleFeatureStore;

import org.geotools.util.factory.Hints;
import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreFactory;

import org.locationtech.geomesa.index.geotools.GeoMesaDataStore;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class GeomesaInsert {
    public static String typeName;
    private final Map<String, String> params;
    private final TutorialData data;

    public GeomesaInsert(String[] args, DataAccessFactory.Param[] parameters, TutorialData data) throws ParseException {
        // parse the data store parameters from the command line
        Options options = createOptions(parameters);
        CommandLine command = CommandLineDataStore.parseArgs(getClass(), options, args);
        params = CommandLineDataStore.getDataStoreParams(command, options);
        this.data = data;
        typeName = data.getTypeName();
    }
    public void run(){
        DataStore datastore = null;
        try {
            datastore = createDataStore(params);

            System.out.println("Cleaning up test data");
            // delete before insert
            if (datastore instanceof GeoMesaDataStore) {
                ((GeoMesaDataStore) datastore).delete();
                datastore = createDataStore(params);
            } else {
                ((SimpleFeatureStore) datastore.getFeatureSource(typeName)).removeFeatures(Filter.INCLUDE);
                datastore.removeSchema(typeName);
            }
            System.out.println("Old table cleaned");

            SimpleFeatureType sft = data.getSimpleFeatureType();
            createSchema(datastore, sft);
            // Read from file in loop
            List<SimpleFeature> features = getFeatures(data);
            while (0 != features.size()){
                writeFeatures(datastore, sft, features);
                features = getFeatures(data);
            }

        }catch (Exception e){
            e.printStackTrace();
        }
    }
    public Options createOptions(DataAccessFactory.Param[] parameters) {
        // parse the data store parameters from the command line
        Options options = CommandLineDataStore.createOptions(parameters);

        options.addOption(Option.builder().longOpt("cleanup").desc("Delete tables after running").build());

        return options;
    }

    public DataStore createDataStore(Map<String, String> params) throws IOException {
        System.out.println("Creating data store");

        // use geotools service loading to get a datastore instance
        DataStore datastore = DataStoreFinder.getDataStore(params);
        if (datastore == null) {
            throw new RuntimeException("Could not create data store with provided parameters");
        }
        System.out.println();
        return datastore;
    }

    public void createSchema(DataStore datastore, SimpleFeatureType sft) throws IOException {
        System.out.println("Creating schema: " + DataUtilities.encodeType(sft));
        // we only need to do the once - however, calling it repeatedly is a no-op
        datastore.createSchema(sft);
        System.out.println();
    }

    public List<SimpleFeature> getFeatures(TutorialData data) {
        List<SimpleFeature> features = data.getNextData(10000);
        return features;
    }

    public void writeFeatures(DataStore datastore, SimpleFeatureType sft, List<SimpleFeature> features) throws IOException {
        if (features.size() > 0) {
            // use try-with-resources to ensure the writer is closed
            try (FeatureWriter<SimpleFeatureType, SimpleFeature> writer =
                         datastore.getFeatureWriterAppend(sft.getTypeName(), Transaction.AUTO_COMMIT)) {
                for (SimpleFeature feature : features) {
                    // WRITE A FEATURE TO HBASE

                    // using a geotools writer, you have to get a feature, modify it, then commit it
                    // appending writers will always return 'false' for haveNext, so we don't need to bother checking
                    SimpleFeature toWrite = writer.next();

                    // copy attributes
                    toWrite.setAttributes(feature.getAttributes());

                    // if you want to set the feature ID, you have to cast to an implementation class
                    // and add the USE_PROVIDED_FID hint to the user data
//                    ((FeatureIdImpl) toWrite.getIdentifier()).setID(feature.getID());
//                    toWrite.getUserData().put(Hints.USE_PROVIDED_FID, Boolean.TRUE);

                    // alternatively, you can use the PROVIDED_FID hint directly
//                    toWrite.getUserData().put(Hints.PROVIDED_FID, feature.getID());

                    // if no feature ID is set, a UUID will be generated for you

                    // make sure to copy the user data, if there is any
                    toWrite.getUserData().putAll(feature.getUserData());

                    // write the feature
                    writer.write();
                }
            }
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
            new GeomesaInsert(args, new AccumuloDataStoreFactory().getParametersInfo(), new InsarPointInfo())
                    .run();
        } catch (ParseException e) {
            System.exit(1);
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(2);
        }
        System.exit(0);
    }
}
