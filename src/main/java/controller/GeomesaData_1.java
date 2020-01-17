package controller;

import org.apache.commons.cli.ParseException;
import org.geomesa.example.data.GDELTData;
import org.geomesa.example.quickstart.GeoMesaQuickStart;
import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreFactory;

public class GeomesaData_1 extends GeoMesaQuickStart {

    // uses gdelt data
    public GeomesaData_1(String[] args)  throws ParseException {
        super(args, new AccumuloDataStoreFactory().getParametersInfo(), new GDELTData());
    }

    public static void main(String[] args) {
        try {
            args = new String[]{
                    "--accumulo.instance.id","hdp-accumulo-instance",
                    "--accumulo.zookeepers","a0.hdp:2181,a1.hdp:2181,a2.hdp:2181",
                    "--accumulo.user","geomesa",
                    "--accumulo.password", "geomesa",
                    "--accumulo.catalog","geomesa.test_geomesa"
            };
            new GeomesaData_1(args).run();
        } catch (ParseException e) {
            System.exit(1);
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(2);
        }
        System.exit(0);
    }
}
