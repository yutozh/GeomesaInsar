package app;

import controller.GeomesaSearch;
import controller.InsarPointInfo;
import org.apache.commons.lang.StringUtils;
import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreFactory;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

@WebServlet("/api/rate")
public class ServerRate extends HttpServlet {
    private static String[] args = new String[]{
            "--accumulo.instance.id","hdp-accumulo-instance",
            "--accumulo.zookeepers","a0.hdp:2181,a1.hdp:2181,a2.hdp:2181",
            "--accumulo.user","geomesa",
            "--accumulo.password", "geomesa",
            "--accumulo.catalog","geomesa.geodata_insar"
    };

    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        try {
            /* 设置响应头允许ajax跨域访问 */
            response.setHeader("Access-Control-Allow-Origin", "*");
            /* 星号表示所有的异域请求都可以接受， */
            response.setHeader("Access-Control-Allow-Methods", "GET,POST");

            String rateFile = "D:\\Experiment\\point_db\\beijing\\def_rate.txt";
            String xyFile = "D:\\Experiment\\point_db\\beijing\\lonlat.txt";
            BufferedReader bf1 = new BufferedReader(new FileReader(rateFile));
            BufferedReader bf2 = new BufferedReader(new FileReader(xyFile));
            String line1 = null;
            String line2 = null;
            List<String> res = new ArrayList<>();
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            while (null != (line1 = bf1.readLine())){
                line2 = bf2.readLine();
                String lon = line2.split(",")[0];
                String lat = line2.split(",")[1];
                baos.write(GeomesaSearch.getBytes(Float.floatToIntBits(Float.parseFloat(lon))));
                baos.write(GeomesaSearch.getBytes(Float.floatToIntBits(Float.parseFloat(lat))));
                baos.write(GeomesaSearch.getBytes(Float.floatToIntBits(Float.parseFloat(line1))));
            }

//            response.setContentType("text/json");
//            PrintWriter out=response.getWriter();
//            out.println("{\"code\":200, \"result\": [ " + StringUtils.join(res, "|") + "] }");

            ServletOutputStream out = response.getOutputStream();
            out.write(baos.toByteArray());
        } catch (Exception e) {
            response.setStatus(500);
            PrintWriter pw = response.getWriter();
            e.printStackTrace(pw);
        }
    }
}