package app;

import com.alibaba.fastjson.JSON;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import controller.GeomesaSearch;
import controller.InsarPointInfo;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreFactory;
import org.locationtech.jts.geom.Point;
import org.opengis.feature.simple.SimpleFeature;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@WebServlet("/api/search")
public class ServerSearch extends HttpServlet {
    public static Log log = LogFactory.getLog(ServerSearch.class);
    private static String[] args = new String[]{
        "--accumulo.instance.id","hdp-accumulo-instance",
                "--accumulo.zookeepers","a0.hdp:2181,a1.hdp:2181,a2.hdp:2181",
                "--accumulo.user","geomesa",
                "--accumulo.password", "geomesa",
                "--accumulo.catalog","geomesa.geodata_insar"
    };

    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        try {
            /* 设置响应头允许ajax跨域访问 */
            response.setHeader("Access-Control-Allow-Origin", "*");
            /* 星号表示所有的异域请求都可以接受， */
            response.setHeader("Access-Control-Allow-Methods", "GET,POST");

            String contentType = request.getContentType();
            System.out.println(contentType);

            Map<String, String> map = new HashMap<>();
            String paramJson = IOUtils.toString(request.getInputStream(), "UTF-8");
            System.out.println(paramJson);
            Map parseObject = JSON.parseObject(paramJson, Map.class);
            map.putAll(parseObject);


            String startTs = "";
            String endTs = "";
            String startRate = "";
            String endRate = "";
            String pointID = "";
            String sortBy = "";

            // Search
            String baseDay = map.getOrDefault("baseDay", "");
            String days = map.getOrDefault("days", "");
            String areaString = map.getOrDefault("areaString","");
            String maxView = map.getOrDefault("maxView","");

            System.out.println(days + " " + baseDay + " " + startTs + " " + endTs +
                    " " + startRate + " " + endRate + " " + pointID + " " + areaString +
                    " " + maxView + " " + sortBy + " ");

            // First search base values
            List<SimpleFeature> featuresBase = new GeomesaSearch(args, new AccumuloDataStoreFactory().getParametersInfo(), new InsarPointInfo())
                    .searchEntries(baseDay, baseDay, startTs, endTs, startRate, endRate,
                            pointID, areaString, maxView, sortBy, true);
            Map<String, Double> baseMap= new HashMap<>();
            for(SimpleFeature s: featuresBase){
                baseMap.put(s.getProperty("pointID").getValue().toString(), (Double) s.getProperty("ts").getValue());
            }

            // Then search each day and minus the base value
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            String[] dayList = days.split(",");
            for (String day: dayList){
                List<SimpleFeature> featuresRes = new GeomesaSearch(args, new AccumuloDataStoreFactory().getParametersInfo(), new InsarPointInfo())
                        .searchEntries(day, day, startTs, endTs, startRate, endRate,
                                pointID, areaString, maxView, sortBy, true);
                // Head
                baos.write(GeomesaSearch.getBytes(Integer.parseInt(day)));
                baos.write(GeomesaSearch.getBytes(featuresRes.size()));
                // Body
                for(SimpleFeature s: featuresRes){
                    // pointID
                    String pointId = s.getProperty("pointID").getValue().toString();
                    baos.write(GeomesaSearch.getBytes(Integer.parseInt(pointId)));

                    // coord
                    Point o = (Point) s.getProperty("coord").getValue();
                    baos.write(GeomesaSearch.getBytes(Float.floatToIntBits((float) o.getX()))); // lon
                    baos.write(GeomesaSearch.getBytes(Float.floatToIntBits((float) o.getY()))); // lat

                    // delta_ts
                    double ts = (Double) s.getProperty("ts").getValue();
                    double delta_ts = ts - baseMap.get(pointId);
                    baos.write(GeomesaSearch.getBytes(Float.floatToIntBits((float) delta_ts)));// ts
                }
            }

            ServletOutputStream out = response.getOutputStream();
            out.write(baos.toByteArray());
        } catch (Exception e) {
            response.setStatus(500);
            PrintWriter pw = response.getWriter();
            e.printStackTrace(pw);
        }
    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String dayFile = "D:\\Experiment\\point_db\\beijing\\day.txt";
        BufferedReader bf = new BufferedReader(new FileReader(dayFile));
        String line = null;
        List<String> res = new ArrayList<>();
        while (null != (line = bf.readLine())){
            res.add(line);
        }

        response.setContentType("text/json");
        PrintWriter out=response.getWriter();
        out.println("{\"code\":200, \"result\": [ " + StringUtils.join(res, ",") + "] }");
    }
}
