import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.annotation.*;
import java.io.BufferedReader;
import java.io.IOException;

@WebServlet(name = "SkierServlet", value = "/skiers/*")
public class SkierServlet extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");
        response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().write("{\"message\": \"GET request successful\"}");
    }
    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");

        String pathInfo = request.getPathInfo();
        String[] pathParts = pathInfo.split("/");

        if (pathParts.length != 8) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.getWriter().write("{\"error\": \"Invalid URL format\"}");
            return;
        }

        try {
            int resortID = Integer.parseInt(pathParts[1]);
            String seasonID = pathParts[3];
            int dayID = Integer.parseInt(pathParts[5]);
            int skierID = Integer.parseInt(pathParts[7]);

            if (!isValidPathParams(dayID, skierID)) {
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                response.getWriter().write("{\"error\": \"Invalid path parameters\"}");
                return;
            }

            StringBuilder stringBuilder = new StringBuilder();
            BufferedReader reader = request.getReader();
            String line;
            while ((line = reader.readLine()) != null) {
                stringBuilder.append(line);
            }
            String requestBody = stringBuilder.toString();

            int liftID = extractJsonIntValue(requestBody, "liftID");
            int time = extractJsonIntValue(requestBody, "time");

            if (liftID < 1 || time < 1) {
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                response.getWriter().write("{\"error\": \"Invalid liftID or time\"}");
                return;
            }

            response.setStatus(HttpServletResponse.SC_CREATED);
            response.getWriter().write("{\"message\": \"Lift ride recorded successfully\"}");

        } catch (Exception e) {
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            response.getWriter().write("{\"error\": \"Internal server error\"}");
        }
    }
    private boolean isValidPathParams(int dayID, int skierID) {
        return dayID >= 1 && dayID <= 366 && skierID > 0;
    }

    private int extractJsonIntValue(String json, String key) {
        String searchKey = "\"" + key + "\":";
        int startIndex = json.indexOf(searchKey) + searchKey.length();
        int endIndex = json.indexOf(",", startIndex);
        if (endIndex == -1) {
            endIndex = json.indexOf("}", startIndex);
        }
        return Integer.parseInt(json.substring(startIndex, endIndex).trim());
    }
}