package app;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import controller.GeomesaSearch;
import controller.InsarPointInfo;
import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreFactory;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Type;
import java.util.Map;

@WebServlet("/api/statistic")
public class ServerStatistic extends HttpServlet {
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

            String statisticRes = new GeomesaSearch(args, new AccumuloDataStoreFactory().getParametersInfo(), new InsarPointInfo())
                    .getStatistic();
            response.setContentType("application/json");
            PrintWriter pw = response.getWriter();
            pw.write(statisticRes);
        } catch (Exception e) {
            PrintWriter pw = response.getWriter();
            e.printStackTrace(pw);
        }
    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        /* 设置响应头允许ajax跨域访问 */
        response.setHeader("Access-Control-Allow-Origin", "*");
        /* 星号表示所有的异域请求都可以接受， */
        response.setHeader("Access-Control-Allow-Methods", "GET,POST");

        response.setContentType("text/html");
        PrintWriter out=response.getWriter();
        out.println("this is servlet");
    }
}