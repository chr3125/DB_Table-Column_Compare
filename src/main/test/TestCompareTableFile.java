package test;

import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.database.QueryDataSet;
import org.dbunit.dataset.Column;
import org.dbunit.ext.oracle.Oracle10DataTypeFactory;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestCompareTableFile {


    @Test
    public void testSourceGetDBTable() throws Exception {

        Class driverClass = Class.forName("oracle.jdbc.driver.OracleDriver");
        String user = "";
        ResultSet rs = null;
        Connection jdbcConnection = DriverManager.getConnection("jdbc:oracle:thin:@", user, "");

        Statement stmt = null;

        stmt = jdbcConnection.createStatement();

        String sql = "SELECT UT.OBJECT_NAME AS TABLE_NAME\n" +
                "            FROM   ALL_OBJECTS UT,\n" +
                "                    ALL_TAB_COMMENTS UTC\n" +
                "            WHERE UT.OBJECT_NAME = UTC.TABLE_NAME\n" +
                "            AND UT.OWNER = UTC.OWNER\n" +
                "            AND UT.OBJECT_TYPE IN ('TABLE')\n" +
                "            AND UT.OWNER ='"+user.toUpperCase()+"'\n" +
                "\n" +
                "            AND UT.OBJECT_NAME NOT LIKE 'BIN$%'\n" +
                "            ORDER BY TABLE_NAME";

        rs = stmt.executeQuery(sql);


        IDatabaseConnection connection = new DatabaseConnection(jdbcConnection);
        DatabaseConfig config = connection.getConfig();
        config.setProperty(DatabaseConfig.PROPERTY_DATATYPE_FACTORY, new Oracle10DataTypeFactory());
        config.setFeature(DatabaseConfig.FEATURE_SKIP_ORACLE_RECYCLEBIN_TABLES,Boolean.TRUE);



        Map<String, List<Map<String,Object>>> tableMap = new HashMap<String, List<Map<String,Object>>>();

        while(rs.next()){
            QueryDataSet partialDataSet = new QueryDataSet(connection);
            List<Map<String,Object>> columnListMap = new ArrayList<Map<String, Object>>();

            String tableName = rs.getString("TABLE_NAME");
            partialDataSet.addTable(tableName);

            System.out.println("TABLE NAME :"+tableName);

            Column[] columns = partialDataSet.getTables()[0].getTableMetaData().getColumns();


            for(Column column : columns){
                Map<String,Object> columnMap = new HashMap<String, Object>();
                columnMap.put("column_nm",column.getColumnName());
                columnMap.put("column_data_type",column.getDataType().getTypeClass().getName());
                columnMap.put("column_nullable",column.getNullable());
                columnListMap.add(columnMap);
            }

            tableMap.put(tableName,columnListMap);
        }



        FileWriter outputTableFile = null;

        try {

            for (String getTableName : tableMap.keySet()){

                outputTableFile = new FileWriter(new File("C:\\temp\\compareTable\\source"+ File.separator + getTableName +".txt"), false);

                List<Map<String,Object>> columnList = tableMap.get(getTableName);


                String columnInfoString = "";
                for(Map<String,Object> columnInfo : columnList){

                    columnInfoString += columnInfo.get("column_nm") +"\n";
                    columnInfoString += columnInfo.get("column_data_type") +"\n";
                    columnInfoString += columnInfo.get("column_nullable") +"\n";
                    columnInfoString += "\n\n";

                }
                outputTableFile.write(columnInfoString);
                outputTableFile.flush();

            }

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if(null != outputTableFile) outputTableFile.close();
        }

        System.out.println("END");
    }



    @Test
    public void testTargetGetDBTable() throws Exception {



        Class driverClass = Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        String user = "sa";
        ResultSet rs = null;
        Connection jdbcConnection = DriverManager.getConnection("jdbc:sqlserver://", user, "1004");

        Statement stmt = null;

        stmt = jdbcConnection.createStatement();

        String sql = "SELECT * FROM INFORMATION_SCHEMA.TABLES";

        rs = stmt.executeQuery(sql);


        IDatabaseConnection connection = new DatabaseConnection(jdbcConnection);
        DatabaseConfig config = connection.getConfig();
        config.setProperty(DatabaseConfig.PROPERTY_DATATYPE_FACTORY, new Oracle10DataTypeFactory());
        config.setFeature(DatabaseConfig.FEATURE_SKIP_ORACLE_RECYCLEBIN_TABLES,Boolean.TRUE);



        Map<String,List<Map<String,Object>>> tableMap = new HashMap<String, List<Map<String,Object>>>();

        while(rs.next()){
            QueryDataSet partialDataSet = new QueryDataSet(connection);

            List<Map<String,Object>> columnListMap = new ArrayList<Map<String, Object>>();

            String tableName = rs.getString("TABLE_NAME");
            partialDataSet.addTable(tableName);

            System.out.println("TABLE NAME :"+tableName);

            Column[] columns = partialDataSet.getTables()[0].getTableMetaData().getColumns();


            for(Column column : columns){
                Map<String,Object> columnMap = new HashMap<String, Object>();
                columnMap.put("column_nm",column.getColumnName());
                columnMap.put("column_data_type",column.getDataType().getTypeClass().getName());
                columnMap.put("column_nullable",column.getNullable());
                columnListMap.add(columnMap);
            }

            tableMap.put(tableName,columnListMap);
        }



        FileWriter outputTableFile = null;

        try {

            for (String getTableName : tableMap.keySet()){

                outputTableFile = new FileWriter(new File("C:\\temp\\compareTable\\target"+ File.separator + getTableName +".txt"), false);

                List<Map<String,Object>> columnList = tableMap.get(getTableName);


                String columnInfoString = "";
                for(Map<String,Object> columnInfo : columnList){

                    columnInfoString += columnInfo.get("column_nm") +"\n";
                    columnInfoString += columnInfo.get("column_data_type") +"\n";
                    columnInfoString += columnInfo.get("column_nullable") +"\n";
                    columnInfoString += "\n\n";

                }
                outputTableFile.write(columnInfoString);
                outputTableFile.flush();

            }

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if(null != outputTableFile) outputTableFile.close();
        }
        System.out.println("END");

    }

    public static void main(String[] arg){
        TestCompareTableFile compareTable = new TestCompareTableFile();
        try{
            compareTable.testSourceGetDBTable();
            compareTable.testTargetGetDBTable();
        }catch (Exception e){
            e.printStackTrace();
        }
    }


}
