To Run from the terminal:

1. Have the JoinSize and postgresql-42.6.0.jar (or any suitable postgres driver) in the same directory
2. cd to the directory where you have the .java file and the postgres driver
2. Run the following command:
        java -cp .:postgresql-42.6.0.jar JoinSize.java <args[0]> <args[1]>
        Note: args[0] and args[1] are the names of the relations you want to estimate their join size.
        Eg. java -cp .:postgresql-42.6.0.jar JoinSize.java students takes