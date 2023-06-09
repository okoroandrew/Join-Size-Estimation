package org.example;
import java.sql.*;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class JoinSize {
    /**
     * A class: Estimates the size of a natural join operation, compares it with the actual size and gives the
     *          estimation error
     * @param conn: jdbc connection to postgres database
     * @param table1: first relation, input from the terminal
     * @param table2: second relation, input from the terminal
     * @return 1. Estimated join size 2. Actual join size  3. Estimation error
     * @throws SQLException:
     *
     * @author: Andrew Okoro
     * @date: 05/04/2023
     * @title: Database programming homework
     */

    public static Set<String> getCommonAttributes(Connection conn, String table1, String table2) throws SQLException {
        /**
         * Given two relations: R and S, this method returns the attributes that both R and S have in common
         *
         * @param conn: jdbc connection to the database
         * @param table1: a relation
         * @param table2: a relation
         * @return the common columns in the two relations
         */
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
        /**
         * Checks if the common attributes is a key in the given relation
         *
         * @param conn: jdbc connection to the database
         * @param table: a relation
         * @param commonCol: attributes
         *
         * @return true if the commonCol is a key for the table
         */

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
        /**
         * Checks if the common attributes is a foreign key in the given relation
         *
         * @param conn: jdbc connection to the database
         * @param table1: a referenced relation
         * @param table2: a referencing relation
         * @param commonCol: common attributes of table1 and table2
         *
         * @return true if the commonCol is a foreign key for the table
         */

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
        /**
         * Returns the estimated size of a join
         *
         * @param conn: jdbc connection to the database
         * @param table1: a referenced relation
         * @param table2: a referencing relation
         * @param commonCol: common attributes of table1 and table2
         * @param fk1: true if the common attribute is a foreign key in table2, otherwise false
         * @param fk2: true if the common attribute is a foreign key in table1, otherwise false
         * @param isKey1: true or false based on the common attribute
         * @param isKey2: true or false based on the common attribute
         *
         * @return the estimated join size
         */

        String queryTable1 = String.format("SELECT COUNT(*) FROM %s", table1);
        String queryTable2 = String.format("SELECT COUNT(*) FROM %s", table2);

        Statement statement1 = conn.createStatement();
        Statement statement2 = conn.createStatement();
        ResultSet rTable1 = statement1.executeQuery(queryTable1);
        ResultSet rTable2 = statement2.executeQuery(queryTable2);

        rTable1.next();
        rTable2.next();

        int sizeTable1 = rTable1.getInt(1);
        int sizeTable2 = rTable2.getInt(1);

        // when R n S is empty, the size of the relation becomes the product of the two, just like cartesian product
        if (commonCol.isEmpty()) {
            return sizeTable1 * sizeTable2;
        }
        // when R n S = A, and A is a foreign key in table2 referencing table1, then the size is exactly the size of table2
        else if (fk1) {
            return sizeTable2;
        }
        // when R n S = A, and A is a foreign key in table1 referencing table2, then the size is exactly the size of table1
        else if (fk2) {
            return sizeTable1;
        }
        // when R n S = A and A is a key in table1. The size of the join will be less than or equal to the size of table2
        else if (isKey1) {
            return sizeTable2;
        }
        // when R n S = A and A is a key in table2. The size of the join will be less than or equal to the size of table1
        else if (isKey2) {
            return sizeTable1;
        }
        // when R n S = A and A is not a key in R or S, the size of join = size(R)*size(S)/max(n(A,R), n(A,S))
        else {
            int noDistinctA = numberOfDistinctA(conn, table1, commonCol);
            int noDistinctB = numberOfDistinctA(conn, table2, commonCol);
            int n = Math.max(noDistinctA, noDistinctB);   //n is the max of the two numbers, leads to the min join size
            return (sizeTable2 * sizeTable1)/n;
        }
    }

    public static int numberOfDistinctA(Connection conn, String table, Set<String> commonCol) throws SQLException {
        /**
         * Returns the number of distinct A in the relation r. i.e n(A, r) = projection of A on r
         *
         * @param conn: jdbc connection to the database
         * @param table: a relation
         * @param commonCol: attributes
         *
         * @return the number of distinct A(commonCol) in r(table)
         */

        String commonAttributes = String.join(", ", commonCol);
        String sql = String.format("SELECT COUNT(*)\n" +
                                    "FROM (SELECT DISTINCT (%s) \n" +
                                    "\t  FROM %s) AS t", commonAttributes, table);
        Statement statement = conn.createStatement();
        ResultSet n = statement.executeQuery(sql);
        n.next();
        return n.getInt(1);
    }


    public static int actualJoinSize(Connection conn, String table1, String table2) throws SQLException {
        /**
         * Returns the actual size of the natural join of table1 and table2
         *
         * @param conn: jdbc connection to the database
         * @param table1: a relation
         * @param table2: a relation
         *
         * @return the actual size of the natural join of the two relations
         */
        String sql = String.format("SELECT COUNT(*) FROM %s NATURAL JOIN %s", table1, table2);
        Statement statement = conn.createStatement();
        ResultSet joinSize = statement.executeQuery(sql);
        joinSize.next();
        return joinSize.getInt(1);
    }

    public static int estimationError(int estimated, int actual){
        /**
         * Returns the estimation error
         * @param estimated: the estimated size of the join
         * @param actual: the actual size of the join
         *
         * @return the difference between the estimated and the actual
         */
        return estimated - actual;
    }

    public static void main(String[] args) throws SQLException {
        // jdbc connection to postgres server, using the url, username, and password
        String url = "jdbc:postgresql://localhost/uni_bo96";
        Properties props = new Properties();
        props.setProperty("user", "postgres");
        props.setProperty("password", "N3tl@b");
        Connection conn = DriverManager.getConnection(url, props);
        System.out.println("Successful connection to postgres");

        // Relations to join: input from the terminal
        String table1 = args[0];
        String table2 = args[1];

        // get the common attributes between the two tables. i.e R n S
        Set<String> commonColumns = getCommonAttributes(conn, table1, table2);

        // check if the common attribute(s) is a key in any of the relations
        boolean isKey1 = isKey(conn, table1, commonColumns);
        boolean isKey2 = isKey(conn, table2, commonColumns);

        // check whether the common attribute is a foreign key in one of the input relations referencing the other input relation
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

        // close the connection
        conn.close();
    }
}