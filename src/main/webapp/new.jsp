<%--
  Created by IntelliJ IDEA.
  User: X1
  Date: 2020/1/2
  Time: 14:42
  To change this template use File | Settings | File Templates.
--%>
<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<html>
<body>
<h2>This is new Page</h2>
username: <%=request.getParameter("username") %>
<br>
password: <%=request.getParameter("password") %>
</body>
</html>