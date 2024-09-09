import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.text.ParseException;

public class openGaussDemo {
 
    static final String JDBC_DRIVER = "org.postgresql.Driver";  
    static final String DB_URL = "jdbc:postgresql://120.46.50.51:26000/alan_db?ApplicationName=app1&batchMode=off&allowEncodingChanges=true&characterEncoding=UTF-8";
      // 数据库的用户名与密码，需要根据自己的设置
    static final String USER = "alan";
    static final String PASS = "alan@123";
     public static void main(String[] args) {
        System.out.println("hello");
        drop_Table();
        createTable();
        //insertData();
        insert_C030();
        insert_S030();
        insert_sc();
        //deleteLowGrades();
        //queryTable();
        
    }

    public static Connection getConnection() {
        Connection conn = null;
        try {
            Class.forName(JDBC_DRIVER);
            System.out.println("连接数据库...");
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return conn;
    }
    public static void closeConnection(Connection conn, Statement stmt, ResultSet rs) {
        try {
            if (rs != null) {
                rs.close();
            }
            if (stmt != null) {
                stmt.close();
            }
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException se) {
            se.printStackTrace();
        }
    }
    public static void createTable() {
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = getConnection();
            stmt = conn.createStatement();
            System.out.println("start create table");
            String createTableSQL_S030 = "CREATE TABLE S030 (" +
                "SNO CHAR(8) NOT NULL, " +//char
                "SNAME VARCHAR(20) NOT NULL, " +
                "SEX CHAR(8) NOT NULL, " +//考虑汉字大小
                "BDATE DATE NOT NULL, " +
                "HEIGHT DECIMAL(3,2) DEFAULT 0.0, " +
                "DORM VARCHAR(20) NOT NULL, " +
                "PRIMARY KEY(SNO))";
            stmt.executeUpdate(createTableSQL_S030);
            
            String createTableSQL_C030 = "CREATE TABLE C030 (" +
                "CNO CHAR(10) NOT NULL, " +//char
                "CNAME VARCHAR(100) NOT NULL, " +
                "PERIOD SMALLINT NOT NULL, " +
                "CREDIT TINYINT NOT NULL, " +
                "TEACHER VARCHAR(20) NOT NULL, " +
                "PRIMARY KEY(CNO))";
            stmt.executeUpdate(createTableSQL_C030);

            String createTableSQL_sc = "CREATE TABLE SC030 (" +
                "SNO CHAR(8) NOT NULL, " +
                "CNO CHAR(10) NOT NULL, " +
                "GRADE DECIMAL(3,1) DEFAULT NULL, " +
                "PRIMARY KEY(SNO, CNO), " +
                "FOREIGN KEY(SNO) REFERENCES S030(SNO) ON DELETE CASCADE, " +
                "FOREIGN KEY(CNO) REFERENCES C030(CNO) ON DELETE RESTRICT) ";
            stmt.executeUpdate(createTableSQL_sc);
           
        } catch (SQLException se) {
            se.printStackTrace();
        } finally {
            closeConnection(conn, stmt, null);
        }
    }
    public static void insertData() {
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = getConnection();
            stmt = conn.createStatement();
            System.out.println("start insert data");
            conn.setAutoCommit(false);
            String[] sqlStatements_S030 = {
                "INSERT INTO S030 VALUES('01032010', '王涛', '男', '2003-04-05', 1.72, '东6舍221')",
                "INSERT INTO S030 VALUES('01032023', '孙文', '男', '2004-06-10', 1.80, '东6舍221')",
                "INSERT INTO S030 VALUES('01032001', '张晓梅', '女', '2004-11-17', 1.58, '东1舍312')",
                "INSERT INTO S030 VALUES('01032005', '刘静', '女', '2003-01-10', 1.63, '东1舍312')",
                "INSERT INTO S030 VALUES('01032112', '董蔚', '男', '2003-02-20', 1.71, '东6舍221')",
                "INSERT INTO S030 VALUES('03031011', '王倩', '女', '2004-12-20', 1.66, '东2舍104')",
                "INSERT INTO S030 VALUES('03031014', '赵思扬', '男', '2002-06-06', 1.85, '东18舍421')",
                "INSERT INTO S030 VALUES('03031051', '周剑', '男', '2002-05-08', 1.68, '东18舍422')",
                "INSERT INTO S030 VALUES('03031009', '田菲', '女', '2003-08-11', 1.60, '东2舍104')",
                "INSERT INTO S030 VALUES('03031033', '蔡明明', '男', '2003-03-12', 1.75, '东18舍423')",
                "INSERT INTO S030 VALUES('03031056', '曹子衿', '女', '2004-12-15', 1.65, '东2舍305')"
            };
            String[] sqlStatements_C030 = {
                "INSERT INTO C030 VALUES('CS-01', '数据结构', 60, 3, '张军')",
                "INSERT INTO C030 VALUES('CS-02', '计算机组成原理', 80, 4, '王亚伟')",
                "INSERT INTO C030 VALUES('CS-04', '人工智能', 40, 2, '李蕾')",
                "INSERT INTO C030 VALUES('CS-05', '深度学习', 40, 2, '崔昀')",
                "INSERT INTO C030 VALUES('EE-01', '信号与系统', 60, 3, '张明')",
                "INSERT INTO C030 VALUES('EE-02', '数字逻辑电路', 100, 5, '胡海东')",
                "INSERT INTO C030 VALUES('EE-03', '光电子学与光子学', 40, 2, '石韬')"
            };

            String[] sqlStatements_sc = {
                "INSERT INTO SC030 VALUES('01032010', 'CS-01', 82.0)",
                "INSERT INTO SC030 VALUES('01032010', 'CS-02', 91.0)",
                "INSERT INTO SC030 VALUES('01032010', 'CS-04', 83.5)",
                "INSERT INTO SC030 VALUES('01032001', 'CS-01', 77.5)",
                "INSERT INTO SC030 VALUES('01032001', 'CS-02', 85.0)",
                "INSERT INTO SC030 VALUES('01032001', 'CS-04', 83.0)",
                "INSERT INTO SC030 VALUES('01032005', 'CS-01', 62.0)",
                "INSERT INTO SC030 VALUES('01032005', 'CS-02', 77.0)",
                "INSERT INTO SC030 VALUES('01032005', 'CS-04', 82.0)",
                "INSERT INTO SC030 VALUES('01032023', 'CS-01', 55.0)",
                "INSERT INTO SC030 VALUES('01032023', 'CS-02', 81.0)",
                "INSERT INTO SC030 VALUES('01032023', 'CS-04', 76.0)",
                "INSERT INTO SC030 VALUES('01032112', 'CS-01', 88.0)",
                "INSERT INTO SC030 VALUES('01032112', 'CS-02', 91.5)",
                "INSERT INTO SC030 VALUES('01032112', 'CS-04', 86.0)",
                "INSERT INTO SC030 VALUES('01032112', 'CS-05', NULL)",
                "INSERT INTO SC030 VALUES('03031033', 'EE-01', 93.0)",
                "INSERT INTO SC030 VALUES('03031033', 'EE-02', 89.0)",
                "INSERT INTO SC030 VALUES('03031009', 'EE-01', 88.0)",
                "INSERT INTO SC030 VALUES('03031009', 'EE-02', 78.5)",
                "INSERT INTO SC030 VALUES('03031011', 'EE-01', 91.0)",
                "INSERT INTO SC030 VALUES('03031011', 'EE-02', 86.0)",
                "INSERT INTO SC030 VALUES('03031051', 'EE-01', 78.0)",
                "INSERT INTO SC030 VALUES('03031051', 'EE-02', 58.0)",
                "INSERT INTO SC030 VALUES('03031014', 'EE-01', 79.0)",
                "INSERT INTO SC030 VALUES('03031014', 'EE-02', 71.0)"
            };

            for (String sql : sqlStatements_S030) {
                    stmt.addBatch(sql);
            }

            for (String sql : sqlStatements_C030) {
                    stmt.addBatch(sql);
            }
            for (String sql : sqlStatements_sc) {
                stmt.addBatch(sql);
        }
            
            int[] result = stmt.executeBatch();
            for (int i : result) {
                System.out.println("insert result: " + i);
            }
            conn.commit();  
         
        } catch (SQLException se) {
            se.printStackTrace();
            try {
                if (conn != null) {
                    conn.rollback();  
                }
            } catch (SQLException se2) {
                se2.printStackTrace();
            }
        } finally {
            try {
                conn.setAutoCommit(true);  
            } catch (SQLException se) {
                se.printStackTrace();
            }
            closeConnection(conn, stmt, null);
        }
    }
    public static void insert_S030() {
        String csvFile = "student.csv";
        String line;
        String csvSplitBy = ",";

        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(csvFile), "UTF-8"));
             Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {

            conn.setAutoCommit(false);
            // 跳过CSV文件的标题行
            br.readLine();
            System.out.println("start insert data");

            String insertSQL = "INSERT INTO S030 (sno, sname, sex,  height, dorm,bdate) VALUES (?, ?, ?, ?, ?, ?)";
            try (PreparedStatement preparedStatement = conn.prepareStatement(insertSQL)) {
                while ((line = br.readLine()) != null) {
                    // 使用逗号分隔符解析每行数据
                    String[] data = line.split(csvSplitBy);

                    preparedStatement.setString(1, data[0]);
                    preparedStatement.setString(2, data[1]);
                    preparedStatement.setString(3, data[2]);
    
                    java.sql.Date date = convertStringToDate(data[5]);
                    if (date == null) {
                        System.err.println("Skipping invalid date: " + data[5]);
                        continue;
                    }
                    preparedStatement.setDate(6, date);
    
                    try {
                        preparedStatement.setBigDecimal(4, new java.math.BigDecimal(data[3]));
                    } catch (NumberFormatException e) {
                        System.err.println("Skipping invalid height: " + data[3]);
                        continue;
                    }
                    preparedStatement.setString(5, data[4]);
                   

                    // 执行插入操作
                    preparedStatement.addBatch();
                }

                // 执行批处理
                preparedStatement.executeBatch();

                // 提交事务
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                e.printStackTrace();
            }
        } catch (IOException | SQLException e) {
            e.printStackTrace();
        }
    }
    private static java.sql.Date convertStringToDate(String dateString) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            java.util.Date parsedDate = dateFormat.parse(dateString);
            return new java.sql.Date(parsedDate.getTime());
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        }
    }
    public static void insert_C030() {
        String csvFile = "course.csv";
        String line;
        String csvSplitBy = ",";

        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(csvFile), "UTF-8"));
             Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {

            conn.setAutoCommit(false);
            // 跳过CSV文件的标题行
            br.readLine();
            System.out.println("start insert data");

            String insertSQL = "INSERT INTO C030 (cno, cname, period, credit, teacher) VALUES ( ?, ?, ?, ?, ?)";
            try (PreparedStatement preparedStatement = conn.prepareStatement(insertSQL)) {
                while ((line = br.readLine()) != null) {
                    // 使用逗号分隔符解析每行数据
                    String[] data = line.split(csvSplitBy);

                    preparedStatement.setString(1, data[0]);
                    preparedStatement.setString(2, data[1]);

                    try {
                        int period = Integer.parseInt(data[2]);
                        short periodShort = (short) period;
                        preparedStatement.setShort(3, periodShort);
                    } catch (NumberFormatException e) {
                        System.err.println("Skipping invalid PERIOD: " + data[2]);
                        continue;
                    }
                    try {
                        double creditDouble = Double.parseDouble(data[3]);
                        if (creditDouble < Byte.MIN_VALUE || creditDouble > Byte.MAX_VALUE) {
                            System.err.println("CREDIT value out of range: " + data[3]);
                            continue;
                        }
                        byte credit = (byte) Math.round(creditDouble);
                        preparedStatement.setByte(4, credit);
                    } catch (NumberFormatException e) {
                        System.err.println("Skipping invalid CREDIT: " + data[3]);
                        continue;
                    }

                    preparedStatement.setString(5, data[4]);
                    // 执行插入操作
                    preparedStatement.addBatch();
                }

                // 执行批处理
                preparedStatement.executeBatch();

                // 提交事务
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                e.printStackTrace();
            }
        } catch (IOException | SQLException e) {
            e.printStackTrace();
        }
    }
    public static void insert_sc() {
        String csvFile = "sc.csv";
        String line;
        String csvSplitBy = ",";
        
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(csvFile), "UTF-8"));
             Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {

            conn.setAutoCommit(false);
            // 跳过CSV文件的标题行
            br.readLine();
            System.out.println("start insert data");

            String insertSQL = "INSERT into SC030 (sno,cno, grade) VALUES ( ?, ?, ?)";
            try (PreparedStatement preparedStatement = conn.prepareStatement(insertSQL)) {
                while ((line = br.readLine()) != null) {
                    // 使用逗号分隔符解析每行数据
                    String[] data = line.split(csvSplitBy);

                    preparedStatement.setString(1, data[0]);
                    preparedStatement.setString(2, data[1]);
    
                    if (data[2].isEmpty()) {
                        preparedStatement.setNull(3, java.sql.Types.DECIMAL);
                    } else {
                        try {
                            preparedStatement.setBigDecimal(3, new java.math.BigDecimal(data[2]));
                        } catch (NumberFormatException e) {
                            System.err.println("Skipping invalid GRADE: " + data[2]);
                            continue;
                        }
                    }
                    // 执行插入操作
                    preparedStatement.addBatch();
                   
                }

                // 执行批处理
                preparedStatement.executeBatch();
                
                // 提交事务
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                e.printStackTrace();
            }
        } catch (IOException | SQLException e) {
            e.printStackTrace();
        }
    }
    public static void insert_sc_test() {
        String csvFile = "sc.csv";
        String line;
        String csvSplitBy = ",";
        int maxInsertCount = 500;
        int insertCount = 0;

        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(csvFile), "UTF-8"));
             Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {

            conn.setAutoCommit(false);
            // 跳过CSV文件的标题行
            br.readLine();
            System.out.println("Start inserting data...");

            String insertSQL = "INSERT INTO SC030 (sno, cno, grade) VALUES (?, ?, ?)";
            try (PreparedStatement preparedStatement = conn.prepareStatement(insertSQL)) {
                while ((line = br.readLine()) != null && insertCount < maxInsertCount) {
                    // 使用逗号分隔符解析每行数据
                    String[] data = line.split(csvSplitBy);

                    preparedStatement.setString(1, data[0]);
                    preparedStatement.setString(2, data[1]);

                    if (data[2].isEmpty()) {
                        preparedStatement.setNull(3, java.sql.Types.DECIMAL);
                    } else {
                        try {
                            preparedStatement.setBigDecimal(3, new BigDecimal(data[2]));
                        } catch (NumberFormatException e) {
                            System.err.println("Skipping invalid GRADE: " + data[2]);
                            continue;
                        }
                    }
                    // 执行插入操作
                    preparedStatement.addBatch();
                    // 打印插入信息
                    System.out.println("Inserted record: " + data[0] + ", " + data[1] + ", " + data[2]);
                    insertCount++;

                    if (insertCount % 10 == 0) {
                        preparedStatement.executeBatch();
                        conn.commit();
                    }
                }

                // 执行剩余的批处理
                if (insertCount % 10 != 0) {
                    preparedStatement.executeBatch();
                    conn.commit();
                }

                System.out.println("Inserted " + insertCount + " records into SC030 table.");
            } catch (SQLException e) {
                conn.rollback();
                e.printStackTrace();
            }
        } catch (IOException | SQLException e) {
            e.printStackTrace();
        }
    }

    public static void drop_Table() {
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = getConnection();
            stmt = conn.createStatement();
            System.out.println("start drop table");
            String drop_C030 = "drop table C030";
            String drop_sc = "drop table SC030";
            String drop_S030 = "drop table S030";
            stmt.executeUpdate(drop_sc);
            stmt.executeUpdate(drop_C030);
            stmt.executeUpdate(drop_S030);

           
        } catch (SQLException se) {
            se.printStackTrace();
        } finally {
            closeConnection(conn, stmt, null);
        }
    }
    public static void queryTable() {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            conn = getConnection();
            stmt = conn.createStatement();
            System.out.println("start query table");
            String sql = "SELECT * FROM S030";
            rs = stmt.executeQuery(sql);
    
            while (rs.next()) {
                String sno = rs.getString("SNO");
                String sname = rs.getString("SNAME");
                String sex = rs.getString("SEX");
                Date bdate = rs.getDate("BDATE");
                double height = rs.getDouble("HEIGHT");
                String dorm = rs.getString("DORM");

                System.out.println("SNO: " + sno + ", SNAME: " + sname + ", SEX: " + sex
                + ", BDATE: " + bdate + ", HEIGHT: " + height + ", DORM: " + dorm);
            }
        } catch (SQLException se) {
            se.printStackTrace();
        } finally {
            closeConnection(conn, stmt, rs);
        }
    }
    
    public static void deleteLowGrades() {
        int deleteCount = 200; // 每次删除一条数据，并重复50次
        try (Connection conn = getConnection();
             Statement statement = conn.createStatement()) {
            conn.setAutoCommit(true);
            System.out.println("Start deleting records...");

            for (int i = 0; i < deleteCount; i++) {
                String deleteSQL = "DELETE FROM SC030 WHERE ctid IN (SELECT ctid FROM SC030 WHERE grade < 60 LIMIT 1)";
                int rowsDeleted = statement.executeUpdate(deleteSQL);
                System.out.println("Deleted a record with grade < 60.");
                if (rowsDeleted > 0) {
                    System.out.println("Deleted a record with grade < 60.");
                    conn.commit();
                } else {
                    System.out.println("No records to delete with grade < 60 remaining.");
                    break;
                }
                try {
                    Thread.sleep(100); // sleep for a brief moment (100 milliseconds)
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            conn.commit();
            System.out.println("Deletion completed.");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
} 

