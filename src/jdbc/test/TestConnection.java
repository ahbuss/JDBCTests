package jdbc.test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 *
 * @author Arnold Buss
 */
public class TestConnection {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Class.forName("oracle.jdbc.driver.OracleDriver");
        String url = "jdbc:oracle:thin:@localhost:1521:xe";

        Properties properties = new Properties();
        properties.setProperty("user", "navarm");
        properties.setProperty("password", "navarm");

        Connection connection = DriverManager.getConnection(url, properties);

        DatabaseMetaData dbmd = connection.getMetaData();
        ResultSet tableRS = dbmd.getTables(null, null, null, null);

        List<String> tables = new ArrayList<>();
        while (tableRS.next()) {
            if (tableRS.getString("TABLE_TYPE").equals("TABLE")) {
                tables.add(tableRS.getString("TABLE_NAME"));
            }
        }
        tableRS.close();

        Statement statement = connection.createStatement();

        String tableName = "Test";
        String selectString = "SELECT * FROM " + tableName;
        ResultSet rs = statement.executeQuery(selectString);

        System.out.println("Table: " + tableName);
        ResultSetMetaData rsmd = rs.getMetaData();
        for (int column = 1; column <= rsmd.getColumnCount(); ++column) {
            System.out.print("\t" + rsmd.getColumnName(column));
        }
        System.out.println();

        while (rs.next()) {
            for (int column = 1; column <= rsmd.getColumnCount(); ++column) {
                System.out.print("\t" + rs.getObject(column));
            }
            System.out.println();
        }

        rs.close();

        String newTableName = "NewTable";
        if (tables.contains(newTableName.toUpperCase())) {
            String dropTable = "DROP TABLE " + newTableName.toUpperCase();
            statement.executeUpdate(dropTable);
        }

        String createTable = "CREATE TABLE " + newTableName + " (UNIQUE_ID INTEGER NOT NULL, "
                + "SITE_ID VARCHAR(32) NOT NULL)";
        statement.executeUpdate(createTable);

        String insert = "INSERT INTO " + newTableName + " VALUES (1, 'Site1')";
        statement.executeUpdate(insert);
        insert = "INSERT INTO " + newTableName + " (SITE_ID, UNIQUE_ID) VALUES ('Site2', 2)";
        statement.executeUpdate(insert);

        selectString = "SELECT * FROM " + newTableName;
        rs = statement.executeQuery(selectString);
        rsmd = rs.getMetaData();

        System.out.println("Table: " + newTableName);
        for (int column = 1; column <= rsmd.getColumnCount(); ++column) {
            System.out.print("\t" + rsmd.getColumnName(column));
        }
        System.out.println();

        while (rs.next()) {
            for (int column = 1; column <= rsmd.getColumnCount(); ++column) {
                System.out.print("\t" + rs.getObject(column));
            }
            System.out.println();
        }
        statement.close();
        connection.close();
    }

}
