package org.example;
import java.sql.*;
import java.util.Properties;

public class Main {

    public static int estimatedJoinSize(){
        return 0;
    }

    public static int actualJoinSize(){
        return 0;
    }

    public static int EstimationError(){
        return 0;
    }

    public static void main(String[] args) {
        String url = "jdbc:postgresql://localhost/uni_bo96";
        Properties props = new Properties();
        props.setProperty("user", "postgres");
        props.setProperty("password", "N3tl@b");

        try {
            Connection conn = DriverManager.getConnection(url, props);
            System.out.println("Successful connection to postgres");

            String sql = "SELECT * FROM student";
            Statement statement = conn.createStatement();
            ResultSet result = statement.executeQuery(sql);
            while(result.next()){
                int id = result.getInt("id");
                System.out.println(id);
            }


            conn.close();
        } catch (SQLException e) {
            System.out.println("Error connecting to postgres");
            throw new RuntimeException(e);
        }

    }
}