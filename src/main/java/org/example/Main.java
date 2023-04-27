package org.example;
import java.sql.*;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class Main {

    Main(){

    }

    public static Set<String> getCommonAttributes(Connection conn, String table1, String table2) throws SQLException {
        //takes two tables and return their common attribute
        DatabaseMetaData metaData = conn.getMetaData();
        ResultSet columnsTable1 = metaData.getColumns(null, null, table1, null);
        ResultSet columnsTable2 = metaData.getColumns(null, null, table2, null);
        Set<String> colsTable1 = new HashSet<String>();
        Set<String> colsTable2 = new HashSet<String>();

        while(columnsTable1.next()){
            String col = columnsTable1.getString("COLUMN_NAME");
            colsTable1.add(col);
        }

        while(columnsTable2.next()){
            String col = columnsTable2.getString("COLUMN_NAME");
            colsTable2.add(col);
        }

        Set<String> commonCols = new HashSet<String>(colsTable1);
        commonCols.retainAll(colsTable2);
        System.out.println("table1: " + colsTable1);
        System.out.println("table2: " + colsTable2);
        System.out.println("common: " + commonCols);

        return commonCols;
    }

    public static Set<String> getPrimaryKey(Connection conn, String table) throws SQLException {
        Set<String> pKeys = new HashSet<String>();
        DatabaseMetaData metaData = conn.getMetaData();
        ResultSet primaryKeys = metaData.getPrimaryKeys(null, null, table);
        while(primaryKeys.next()){
            String pkey = primaryKeys.getString("COLUMN_NAME");
            pKeys.add(pkey);
        }
        return pKeys;
    }

    public static boolean isKey(String table1, String table2){

        return true;
    }

    public static int estimatedJoinSize(){
        return 0;
    }

    public static int actualJoinSize(){
        return 0;
    }

    public static int EstimationError(){
        return 0;
    }

    public static void main(String[] args) throws SQLException {
        String url = "jdbc:postgresql://localhost/uni_bo96";
        Properties props = new Properties();
        props.setProperty("user", "postgres");
        props.setProperty("password", "N3tl@b");
        Connection conn = DriverManager.getConnection(url, props);
        System.out.println("Successful connection to postgres");

        String table1 = "takes";
        String table2 = "student";

        //DatabaseMetaData metaData = conn.getMetaData();
        Set<String> tada = getCommonAttributes(conn, table1, table2);
        Set<String> pkeys = getPrimaryKey(conn, table1);
        System.out.printf("primary key: %s",pkeys);








        conn.close();
    }
}