package org.example;
import java.sql.*;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class Main {

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
        System.out.printf("table1: %s(%s) \n" , table1, String.join(", ", colsTable1));
        System.out.printf("table2: %s(%s) \n" , table2, String.join(", ", colsTable2));
        System.out.println("common attributes: " + commonCols);

        return commonCols;
    }

    public static boolean isKey(Connection conn, String table, Set<String> commonCol) throws SQLException {
        boolean isKey = false;
        Set<String> pKeys = new HashSet<String>();
        DatabaseMetaData metaData = conn.getMetaData();
        ResultSet primaryKeys = metaData.getPrimaryKeys(null, null, table);
        while(primaryKeys.next()){
            String pkey = primaryKeys.getString("COLUMN_NAME");
            pKeys.add(pkey);
        }
        if (commonCol.containsAll(pKeys)){
            isKey = true;
        }

        return isKey;
    }

    public static boolean isForeignKey(Connection conn, String table1, String table2, Set<String> commonCol) throws SQLException {
        //check if the common attribute is a foreign key, what's the referencing relation?
        boolean isFK = false;
        DatabaseMetaData metaData = conn.getMetaData();
        ResultSet iKeys = metaData.getImportedKeys(null, null, table2);
        Set<String> FK = new HashSet<>();
        Set<String> referencedRelation = new HashSet<>();
        while(iKeys.next()){
            String fk = iKeys.getString("FKCOLUMN_NAME");
            String rr = iKeys.getString("PKTABLE_NAME");
            FK.add(fk);
            referencedRelation.add(rr);
        }
        if (FK.containsAll(commonCol) && (referencedRelation.contains(table1))){
            isFK = true;
        }

        System.out.printf("Foreign key on %s: %s \n", table2, FK);
        System.out.printf("Referenced relation: %s \n", referencedRelation);
        return isFK;
    }

    public static int estimatedJoinSize(Connection conn, String table1, String table2, Set<String> commonCol,
                                        boolean fk1, boolean fk2, boolean isKey1, boolean isKey2) throws SQLException {

        String queryTable1 = "SELECT COUNT(*) FROM " + table1;
        String queryTable2 = "SELECT COUNT(*) FROM " + table2;

        Statement statement1 = conn.createStatement();
        Statement statement2 = conn.createStatement();
        ResultSet rTable1 = statement1.executeQuery(queryTable1);
        ResultSet rTable2 = statement2.executeQuery(queryTable2);

        rTable1.next();
        rTable2.next();

        int sizeTable1 = rTable1.getInt(1);
        int sizeTable2 = rTable2.getInt(1);

        if (commonCol.isEmpty()) {
            return sizeTable1 * sizeTable2;
        }
        else if (fk1) {
            return sizeTable2;
        }
        else if (fk2) {
            return sizeTable1;
        }
        else if (isKey1) {
            return sizeTable2;
        }
        else if (isKey2) {
            return sizeTable1;
        }
        else {
            int noDistinctA = numberOfDistinctA(conn, table1, commonCol);
            int noDistinctB = numberOfDistinctA(conn, table2, commonCol);
            int n = Math.max(noDistinctA, noDistinctB);
            return (sizeTable2 * sizeTable1)/n;
        }
    }

    public static int numberOfDistinctA(Connection conn, String table, Set<String> commonCol) throws SQLException {
        String commonAttributes = String.join(", ", commonCol);
        String sql = String.format("SELECT COUNT(*)\n" +
                                    "FROM (SELECT %s \n" +
                                    "\t  FROM %s) AS t", commonAttributes, table);
        Statement statement = conn.createStatement();
        ResultSet n = statement.executeQuery(sql);
        n.next();
        return n.getInt(1);
    }


    public static int actualJoinSize(Connection conn, String table1, String table2) throws SQLException {
        String sql = "SELECT COUNT(*) FROM " + table1 + " NATURAL JOIN " + table2;
        Statement statement = conn.createStatement();
        ResultSet joinSize = statement.executeQuery(sql);
        joinSize.next();
        return joinSize.getInt(1);
    }

    public static int estimationError(int estimated, int actual){
        return estimated - actual;
    }

    public static void main(String[] args) throws SQLException {
        String url = "jdbc:postgresql://localhost/uni_bo96";
        Properties props = new Properties();
        props.setProperty("user", "postgres");
        props.setProperty("password", "N3tl@b");
        Connection conn = DriverManager.getConnection(url, props);
        System.out.println("Successful connection to postgres");

        String table1 = "takes";
        String table2 = "advisor";
//        String table1 = args[0];
//        String table2 = args[1];

        Set<String> commonColumns = getCommonAttributes(conn, table1, table2);
        boolean isKey1 = isKey(conn, table1, commonColumns);
        boolean isKey2 = isKey(conn, table2, commonColumns);

        boolean fk1 = isForeignKey(conn, table1, table2, commonColumns);
        boolean fk2 = isForeignKey(conn, table2, table1, commonColumns);

        //Question 1: Estimated Join Size
        int estimatedJoinSize = estimatedJoinSize(conn, table1, table2, commonColumns, fk1, fk2, isKey1, isKey2);
        if (!isKey1 && !isKey2){
            System.out.printf("1. The estimated Join Size = %d \n\n", estimatedJoinSize);
        }
        else {
            System.out.printf("1. The estimated Join Size is no greater than: %d \n\n", estimatedJoinSize);
        }

        //Question 2: Actual Join Size
        int actualJoinSize = actualJoinSize(conn, table1, table2);
        System.out.printf("2. The actual Join Size = %d \n\n", actualJoinSize);

        //Question 3: Estimation Error
        int estimationError = estimationError(estimatedJoinSize, actualJoinSize);
        System.out.printf("3. The estimation error = %d\n", estimationError);

        conn.close();
    }
}