import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.PreparedStatement;

public class SQLonRDS {
    private Connection con;
    private String url = "";
    private String uid = "";
    private String pw = "";

    public static void main(String[] args) {
        SQLonRDS q = new SQLonRDS();
        try {
            q.connect();
            q.create(); // TODO AS PER THE QUESTION
            q.drop(); // TODO AS PER THE QUESTION USING ResultSet type as return
            q.insert(); // TODO AS PER THE QUESTION
            q.queryOne(); // TODO AS PER THE QUESTION
            q.queryTwo(); // TODO AS PER THE QUESTION
            q.queryThree(); // TODO AS PER THE QUESTION
            q.close();
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void connect() throws SQLException, ClassNotFoundException {
        // Load MySQL JDBC Driver
        Class.forName("com.mysql.cj.jdbc.Driver");
        String jdbcUrl = "jdbc:mysql://" + url + "/nitinbhoyate-a3?user=" + uid + "&password=" + pw;
        System.out.println("Connecting to database");
        con = DriverManager.getConnection(jdbcUrl);
        System.out.println("Connection Successful");
    }

    public void close() throws SQLException {
        if (con != null) {
            con.close();
        }
    }

    public void drop() throws SQLException {
        Statement stmt = con.createStatement();
        try {
            // Drop stockprice table first due to foreign key constraint
            stmt.executeUpdate("DROP TABLE IF EXISTS stockprice");
            System.out.println("Dropped stockprice table");
            
            // Drop company table
            stmt.executeUpdate("DROP TABLE IF EXISTS company");
            System.out.println("Dropped company table");
        } finally {
            stmt.close();
        }
    }

    public void create() throws SQLException {
        Statement stmt = con.createStatement();
        try {
            // Create company table
            String createCompanyTable = "CREATE TABLE company (" +
                "id INT PRIMARY KEY, " +
                "name VARCHAR(50), " +
                "ticker CHAR(10), " +
                "annualRevenue DECIMAL(14,2), " +
                "numEmployees INT" +
                ")";
            stmt.executeUpdate(createCompanyTable);
            System.out.println("Created company table");

            // Create stockprice table
            String createStockPriceTable = "CREATE TABLE stockprice (" +
                "companyId INT, " +
                "priceDate DATE, " +
                "openPrice DECIMAL(10,2), " +
                "highPrice DECIMAL(10,2), " +
                "lowPrice DECIMAL(10,2), " +
                "closePrice DECIMAL(10,2), " +
                "volume INT, " +
                "PRIMARY KEY (companyId, priceDate), " +
                "FOREIGN KEY (companyId) REFERENCES company(id)" +
                ")";
            stmt.executeUpdate(createStockPriceTable);
            System.out.println("Created stockprice table");
        } finally {
            stmt.close();
        }
    }

    public void insert() throws SQLException {
        Statement stmt = con.createStatement();
        try {
            // Insert company data
            String[] companyInserts = {
                "INSERT INTO company VALUES (1, 'Apple', 'AAPL', 387540000000.00, 154000)",
                "INSERT INTO company VALUES (2, 'GameStop', 'GME', 611000000.00, 12000)",
                "INSERT INTO company VALUES (3, 'Handy Repair', NULL, 2000000, 50)",
                "INSERT INTO company VALUES (4, 'Microsoft', 'MSFT', 198270000000.00, 221000)",
                "INSERT INTO company VALUES (5, 'StartUp', NULL, 50000, 3)"
            };

            for (String insert : companyInserts) {
                stmt.executeUpdate(insert);
            }
            System.out.println("Inserted company data");

            // Insert stockprice data
            String[] stockPriceInserts = {
                "INSERT INTO stockprice VALUES (1, '2022-08-15', 171.52, 173.39, 171.35, 173.19, 54091700)",
                "INSERT INTO stockprice VALUES (1, '2022-08-16', 172.78, 173.71, 171.66, 173.03, 56377100)",
                "INSERT INTO stockprice VALUES (1, '2022-08-17', 172.77, 176.15, 172.57, 174.55, 79542000)",
                "INSERT INTO stockprice VALUES (1, '2022-08-18', 173.75, 174.90, 173.12, 174.15, 62290100)",
                "INSERT INTO stockprice VALUES (1, '2022-08-19', 173.03, 173.74, 171.31, 171.52, 70211500)",
                "INSERT INTO stockprice VALUES (1, '2022-08-22', 169.69, 169.86, 167.14, 167.57, 69026800)",
                "INSERT INTO stockprice VALUES (1, '2022-08-23', 167.08, 168.71, 166.65, 167.23, 54147100)",
                "INSERT INTO stockprice VALUES (1, '2022-08-24', 167.32, 168.11, 166.25, 167.53, 53841500)",
                "INSERT INTO stockprice VALUES (1, '2022-08-25', 168.78, 170.14, 168.35, 170.03, 51218200)",
                "INSERT INTO stockprice VALUES (1, '2022-08-26', 170.57, 171.05, 163.56, 163.62, 78823500)",
                "INSERT INTO stockprice VALUES (1, '2022-08-29', 161.15, 162.90, 159.82, 161.38, 73314000)",
                "INSERT INTO stockprice VALUES (1, '2022-08-30', 162.13, 162.56, 157.72, 158.91, 77906200)",
                "INSERT INTO stockprice VALUES (2, '2022-08-15', 39.75, 40.39, 38.81, 39.68, 5243100)",
                "INSERT INTO stockprice VALUES (2, '2022-08-16', 39.17, 45.53, 38.60, 42.19, 23602800)",
                "INSERT INTO stockprice VALUES (2, '2022-08-17', 42.18, 44.36, 40.41, 40.52, 9766400)",
                "INSERT INTO stockprice VALUES (2, '2022-08-18', 39.27, 40.07, 37.34, 37.93, 8145400)",
                "INSERT INTO stockprice VALUES (2, '2022-08-19', 35.18, 37.19, 34.67, 36.49, 9525600)",
                "INSERT INTO stockprice VALUES (2, '2022-08-22', 34.31, 36.20, 34.20, 34.50, 5798600)",
                "INSERT INTO stockprice VALUES (2, '2022-08-23', 34.70, 34.99, 33.45, 33.53, 4836300)",
                "INSERT INTO stockprice VALUES (2, '2022-08-24', 34.00, 34.94, 32.44, 32.50, 5620300)",
                "INSERT INTO stockprice VALUES (2, '2022-08-25', 32.84, 32.89, 31.50, 31.96, 4726300)",
                "INSERT INTO stockprice VALUES (2, '2022-08-26', 31.50, 32.38, 30.63, 30.94, 4289500)",
                "INSERT INTO stockprice VALUES (2, '2022-08-29', 30.48, 32.75, 30.38, 31.55, 4292700)",
                "INSERT INTO stockprice VALUES (2, '2022-08-30', 31.62, 31.87, 29.42, 29.84, 5060200)",
                "INSERT INTO stockprice VALUES (4, '2022-08-15', 291.00, 294.18, 290.11, 293.47, 18085700)",
                "INSERT INTO stockprice VALUES (4, '2022-08-16', 291.99, 294.04, 290.42, 292.71, 18102900)",
                "INSERT INTO stockprice VALUES (4, '2022-08-17', 289.74, 293.35, 289.47, 291.32, 18253400)",
                "INSERT INTO stockprice VALUES (4, '2022-08-18', 290.19, 291.91, 289.08, 290.17, 17186200)",
                "INSERT INTO stockprice VALUES (4, '2022-08-19', 288.90, 289.25, 285.56, 286.15, 20557200)",
                "INSERT INTO stockprice VALUES (4, '2022-08-22', 282.08, 282.46, 277.22, 277.75, 25061100)",
                "INSERT INTO stockprice VALUES (4, '2022-08-23', 276.44, 278.86, 275.40, 276.44, 17527400)",
                "INSERT INTO stockprice VALUES (4, '2022-08-24', 275.41, 277.23, 275.11, 275.79, 18137000)",
                "INSERT INTO stockprice VALUES (4, '2022-08-25', 277.33, 279.02, 274.52, 278.85, 16583400)",
                "INSERT INTO stockprice VALUES (4, '2022-08-26', 279.08, 280.34, 267.98, 268.09, 27532500)",
                "INSERT INTO stockprice VALUES (4, '2022-08-29', 265.85, 267.40, 263.85, 265.23, 20338500)",
                "INSERT INTO stockprice VALUES (4, '2022-08-30', 266.67, 267.05, 260.66, 262.97, 22767100)"
            };

            for (String insert : stockPriceInserts) {
                stmt.executeUpdate(insert);
            }
            System.out.println("Inserted stockprice data");
        } finally {
            stmt.close();
        }
    }

    public void delete() throws SQLException {
        Statement stmt = con.createStatement();
        try {
            // Delete stock price records where date is before 2022-08-20 or company is GameStop
            String deleteQuery = "DELETE FROM stockprice WHERE priceDate < '2022-08-20' OR companyId = 2";
            int deletedRows = stmt.executeUpdate(deleteQuery);
            System.out.println("Deleted " + deletedRows + " stock price records");
        } finally {
            stmt.close();
        }
    }

    public ResultSet queryOne() throws SQLException {
        Statement stmt = con.createStatement();
        String query = "SELECT name, annualRevenue, numEmployees " +
                      "FROM company " +
                      "WHERE numEmployees > 10000 OR annualRevenue < 1000000 " +
                      "ORDER BY name ASC";
        return stmt.executeQuery(query);
    }

    public ResultSet queryTwo() throws SQLException {
        Statement stmt = con.createStatement();
        String query = "SELECT c.name, c.ticker, " +
                      "MIN(sp.lowPrice) as lowest_price, " +
                      "MAX(sp.highPrice) as highest_price, " +
                      "AVG(sp.closePrice) as avg_closing_price, " +
                      "AVG(sp.volume) as avg_volume " +
                      "FROM company c " +
                      "JOIN stockprice sp ON c.id = sp.companyId " +
                      "WHERE sp.priceDate BETWEEN '2022-08-22' AND '2022-08-26' " +
                      "GROUP BY c.id, c.name, c.ticker " +
                      "ORDER BY avg_volume DESC";
        return stmt.executeQuery(query);
    }

    public ResultSet queryThree() throws SQLException {
        Statement stmt = con.createStatement();
        String query = "SELECT c.name, c.ticker, sp.closePrice " +
                      "FROM company c " +
                      "LEFT JOIN stockprice sp ON c.id = sp.companyId AND sp.priceDate = '2022-08-30' " +
                      "WHERE c.ticker IS NULL " +
                      "   OR (sp.closePrice IS NOT NULL AND sp.closePrice >= " +
                      "       (SELECT AVG(closePrice) * 0.9 " +
                      "        FROM stockprice sp2 " +
                      "        WHERE sp2.companyId = c.id " +
                      "        AND sp2.priceDate BETWEEN '2022-08-15' AND '2022-08-19')) " +
                      "ORDER BY c.name ASC";
        return stmt.executeQuery(query);
    }

    public static String resultSetToString(ResultSet rst, int maxrows) throws SQLException {
        StringBuffer buf = new StringBuffer(5000);
        int rowCount = 0;
        if (rst == null)
            return "ERROR: No ResultSet";
        ResultSetMetaData meta = rst.getMetaData();
        buf.append("Total columns: " + meta.getColumnCount());
        buf.append('\n');
        if (meta.getColumnCount() > 0)
            buf.append(meta.getColumnName(1));
        for (int j = 2; j <= meta.getColumnCount(); j++)
            buf.append(", " + meta.getColumnName(j));
        buf.append('\n');
        while (rst.next()) {
            if (rowCount < maxrows) {
                for (int j = 0; j < meta.getColumnCount(); j++) {
                    Object obj = rst.getObject(j + 1);
                    buf.append(obj);
                    if (j != meta.getColumnCount() - 1)
                        buf.append(", ");
                }
                buf.append('\n');
            }
            rowCount++;
        }
        buf.append("Total results: " + rowCount);
        return buf.toString();
    }

    public static String resultSetMetaDataToString(ResultSetMetaData meta) throws SQLException {
        StringBuffer buf = new StringBuffer(5000);
        buf.append(meta.getColumnName(1) + " (" + meta.getColumnLabel(1) + ", " + meta.getColumnType(1) + "-" + meta.getColumnTypeName(1) + ", " + meta.getColumnDisplaySize(1) + ", " + meta.getPrecision(1) + ", " + meta.getScale(1) + ")");

        for (int j = 2; j <= meta.getColumnCount(); j++)
            buf.append(", " + meta.getColumnName(j) + " (" + meta.getColumnLabel(j) + ", " + meta.getColumnType(j) + "-" + meta.getColumnTypeName(j) + ", " + meta.getColumnDisplaySize(j) + ", " + meta.getPrecision(j) + ", " + meta.getScale(j) + ")");
        return buf.toString();
    }
}
