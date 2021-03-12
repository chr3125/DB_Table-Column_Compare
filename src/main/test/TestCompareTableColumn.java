package test;

import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.database.QueryDataSet;
import org.dbunit.dataset.Column;
import org.dbunit.ext.mssql.MsSqlDataTypeFactory;
import org.dbunit.ext.mysql.MySqlDataTypeFactory;
import org.dbunit.ext.oracle.Oracle10DataTypeFactory;
import org.dbunit.ext.oracle.OracleDataTypeFactory;
import org.dbunit.ext.postgresql.PostgresqlDataTypeFactory;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestCompareTableColumn {

    public final String SQLTYPE_ORACLE ="oracle";
    public final String SQLTYPE_MSSQL ="mssql";
    public final String SQLTYPE_MARIADB ="mariadb";
    public final String SQLTYPE_POSTGRESQL ="postgresql";



    public Map<String, List<Map<String,Object>>> testSourceGetDBTable() throws Exception {

        Class driverClass = Class.forName("oracle.jdbc.driver.OracleDriver");
        String user = "srm9dv";
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

        System.out.println("SOURCE TABLE SEARCH END");

        return tableMap;
    }


    public Map<String, List<Map<String,Object>>> testTargetGetDBTable() throws Exception {

        Class driverClass = Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        String jdbcType = "mssql";
        String user = "sa";
        ResultSet rs = null;
        Connection jdbcConnection = DriverManager.getConnection("jdbc:sqlserver://", user, "1004");

        Statement stmt = null;

        stmt = jdbcConnection.createStatement();

        String sql = "SELECT * FROM INFORMATION_SCHEMA.TABLES";

        rs = stmt.executeQuery(sql);


        IDatabaseConnection connection = new DatabaseConnection(jdbcConnection);
        DatabaseConfig config = connection.getConfig();
        if(jdbcType.equals(SQLTYPE_ORACLE)){
            config.setProperty(DatabaseConfig.PROPERTY_DATATYPE_FACTORY, new OracleDataTypeFactory());
            config.setFeature(DatabaseConfig.FEATURE_SKIP_ORACLE_RECYCLEBIN_TABLES, Boolean.TRUE);
            jdbcConnection.prepareStatement("SET CONSTRAINTS ALL DEFERRED").execute();

        }else if(jdbcType.equals(SQLTYPE_MARIADB)){
            config.setProperty(DatabaseConfig.PROPERTY_DATATYPE_FACTORY, new MySqlDataTypeFactory());

        }else if(jdbcType.equals(SQLTYPE_MSSQL)){
            config.setProperty(DatabaseConfig.PROPERTY_DATATYPE_FACTORY, new MsSqlDataTypeFactory());

        }else if(jdbcType.equals(SQLTYPE_POSTGRESQL)){
            config.setProperty(DatabaseConfig.PROPERTY_DATATYPE_FACTORY, new PostgresqlDataTypeFactory());
        }


        Map<String,List<Map<String,Object>>> tableMap = new HashMap<String, List<Map<String,Object>>>();

        while(rs.next()){
            QueryDataSet partialDataSet = new QueryDataSet(connection);

            List<Map<String,Object>> columnListMap = new ArrayList<Map<String, Object>>();

            String tableName = rs.getString("TABLE_NAME");
            partialDataSet.addTable(tableName);


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


        System.out.println("TARGET TABLE SEARCH END");

        return tableMap;
    }


    @Test
    public void compareTableMap() throws Exception{

        Map<String,List<Map<String,Object>>> sourceTableMap = this.testSourceGetDBTable();
        Map<String,List<Map<String,Object>>> targetTableMap = this.testTargetGetDBTable();


        List<String> tableMissingList = new ArrayList<String>();


        for(String tableName : sourceTableMap.keySet()){

            //table 이 없을 경우
            if(!targetTableMap.containsKey(tableName)){
                tableMissingList.add(tableName);
            }else{  //table은 있고, 비교

                List<Map<String,Object>> sourceColumnList = sourceTableMap.get(tableName);
                List<Map<String,Object>> targetColumnList = targetTableMap.get(tableName);

                for(Map<String,Object> sourceColumnInfo : sourceColumnList){
                    boolean checkColumnNm = false; //column 이름 check
                    boolean checkColumnType = false; // column type 비교
                    boolean checkColumnNull = false; // column null 비교
                    String sourceColumnNm = sourceColumnInfo.get("column_nm") == null?  "" : sourceColumnInfo.get("column_nm").toString();
                    String sourceColumnType = sourceColumnInfo.get("column_data_type") == null?  "" : sourceColumnInfo.get("column_data_type").toString();
                    String sourceColumnNull = sourceColumnInfo.get("column_nullable") == null?  "" : sourceColumnInfo.get("column_nullable").toString();

                    for(Map<String,Object> targetColumnInfo : targetColumnList){
                        String targetColumnNm = targetColumnInfo.get("column_nm") == null?  "" : targetColumnInfo.get("column_nm").toString();
                        String targetColumnType = targetColumnInfo.get("column_data_type") == null?  "" : targetColumnInfo.get("column_data_type").toString();
                        String targetColumnNull = targetColumnInfo.get("column_nullable") == null?  "" : targetColumnInfo.get("column_nullable").toString();

                        //column name이 맞지 않으면 continue
                        if(!sourceColumnNm.equals(targetColumnNm)) continue;

                        //column name이 지금부터 비교 가능하기에,
                        checkColumnNm = true;

                        // column type 비교
                        if(sourceColumnType.equals(targetColumnType)) checkColumnType=true;

                        // column null able 비교
                        if(sourceColumnNull.equals(targetColumnNull)) checkColumnNull=true;
                    }

                    //한개라도 비교했는데 이슈가 있을 경우
                    if(!(checkColumnNm && checkColumnNull && checkColumnType)){
                        System.out.println(" --------------------------------------------------- ");
                        System.out.println("[TABLE NAME] = "+tableName);

                        if(!checkColumnNm){
                            System.out.println("[NOT COLUMN]");
                            System.out.println("[COLUMN NAME] = "+sourceColumnNm);
                        }

                        if(!checkColumnType){
                            System.out.println("[NOT MAPPING COLUMN DATA TYPE]");
                            System.out.println("[COLUMN NAME] = "+sourceColumnNm);
                        }

                        if(!checkColumnNull){
                            System.out.println("[NOT MAPPING COLUMN NULL ABLE]");
                            System.out.println("[COLUMN NAME] = "+sourceColumnNm);
                        }

                        System.out.println(" --------------------------------------------------- ");

                    }
                }
            }
        }
    }



}
