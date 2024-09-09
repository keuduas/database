import java.sql.*; 
public class test {
 
    static final String JDBC_DRIVER = "org.postgresql.Driver";  
    static final String DB_URL = "jdbc:postgresql://120.46.50.51:26000/zhangker_db?ApplicationName=app1";
      // 数据库的用户名与密码，需要根据自己的设置
    static final String USER = "zhangker";
    static final String PASS = "zhangker@123";
     public static void main(String[] args) {
        Connection conn = null;
        Statement stmt = null;
        try{
            // 注册 JDBC 驱动
            Class.forName(JDBC_DRIVER);
        
            // 打开链接
            System.out.println("连接数据库...");
            conn = DriverManager.getConnection(DB_URL,USER,PASS);
        
            
            System.out.println(" 实例化Statement对象...");
            stmt = conn.createStatement();
            System.out.println(" 执行SQL语句...");

            //创建表
            String createTableSQL = "CREATE TABLE STUDENT (" +
                "SNO CHAR(8) NOT NULL, " +
                "SNAME VARCHAR(20) NOT NULL, " +
                "SEX CHAR(4) NOT NULL, " +
                "BDATE DATE NOT NULL, " +
                "HEIGHT DECIMAL(3,2) DEFAULT 0.0, " +
                "DORM VARCHAR(10) NOT NULL, " +
                "PRIMARY KEY(SNO))";
            stmt.executeUpdate(createTableSQL);

            String createTableSQL_course = "CREATE TABLE COURSE (" +
                "CNO CHAR(10) NOT NULL, " +
                "CNAME VARCHAR(30) NOT NULL, " +
                "PERIOD INT NOT NULL, " +
                "CREDIT SMALLINT NOT NULL, " +
                "TEACHER VARCHAR(20) NOT NULL, " +
                "PRIMARY KEY(CNO))";
            stmt.executeUpdate(createTableSQL_course);

            String createTableSQL_sc = "CREATE TABLE SC (" +
                "SNO CHAR(8) NOT NULL, " +
                "CNO CHAR(10) NOT NULL, " +
                "GRADE DECIMAL(3,1) DEFAULT NULL, " +
                "PRIMARY KEY(SNO, CNO), " +
                "FOREIGN KEY(SNO) REFERENCES STUDENT(SNO) ON DELETE CASCADE, " +
                "FOREIGN KEY(CNO) REFERENCES COURSE(CNO) ON DELETE RESTRICT) ";
            stmt.executeUpdate(createTableSQL_sc);
            
            //更新表
            String[] sqlStatements = {
                "INSERT INTO STUDENT VALUES('01032010', '王强', '男', '2003-04-05', 1.72, '东6舍221')",
                "INSERT INTO STUDENT VALUES('01032555', '王涛', '男', '2003-04-05', 1.72, '东6舍221')",
                "UPDATE STUDENT SET SNAME = '王强' WHERE SNO = '01032010'"
            };
            for (String sql : sqlStatements) {
                stmt.executeUpdate(sql);
            }

            // 查询表
            String sql;
                sql = "SELECT * FROM zhangker.STUDENT";
            ResultSet rs = stmt.executeQuery(sql);
            
            // 展开结果集数据库
            while(rs.next()){
                // // 通过字段检索
                // int id  = rs.getInt("id");
                // String name = rs.getString("name");
                // String url = rs.getString("url");
    
                // // 输出数据
                // System.out.print("ID: " + id);
                // System.out.print(", 站点名称: " + name);
                // System.out.print(", 站点 URL: " + url);
                // System.out.print("\n");
                String sno = rs.getString("SNO");
                String sname = rs.getString("SNAME");
                String sex = rs.getString("SEX");
                Date bdate = rs.getDate("BDATE");
                double height = rs.getDouble("HEIGHT");
                String dorm = rs.getString("DORM");

                System.out.println("SNO: " + sno + ", SNAME: " + sname + ", SEX: " + sex
                        + ", BDATE: " + bdate + ", HEIGHT: " + height + ", DORM: " + dorm);
            
            }
            // 完成后关闭
            rs.close();
            stmt.close();
            conn.close();
        }catch(SQLException se){
            // 处理 JDBC 错误
            se.printStackTrace();
        }catch(Exception e){
            // 处理 Class.forName 错误
            e.printStackTrace();
        }finally{
            // 关闭资源
            try{
                if(stmt!=null) stmt.close();
            }catch(SQLException se2){
            }// 什么都不做
            try{
                if(conn!=null) conn.close();
            }catch(SQLException se){
                se.printStackTrace();
            }
        }
        System.out.println("Goodbye!");
    }
}
