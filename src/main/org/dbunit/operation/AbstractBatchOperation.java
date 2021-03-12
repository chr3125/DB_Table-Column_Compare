/*
 *
 * The DbUnit Database Testing Framework
 * Copyright (C)2002-2004, DbUnit.org
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 */

package org.dbunit.operation;

import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.database.statement.IPreparedBatchStatement;
import org.dbunit.database.statement.IStatementFactory;
import org.dbunit.dataset.*;
import org.dbunit.dataset.datatype.TypeCastException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Base implementation for database operation that are executed in batch.
 *
 * @author Manuel Laflamme
 * @version $Revision: 1225 $
 * @since Feb 19, 2002
 */
public abstract class AbstractBatchOperation extends AbstractOperation
{

    /**
     * Logger for this class
     */
    private static final Logger logger = LoggerFactory.getLogger(org.dbunit.operation.AbstractBatchOperation.class);

    private static final BitSet EMPTY_BITSET = new BitSet();
    protected boolean _reverseRowOrder = false;

    static boolean isEmpty(ITable table) throws DataSetException
    {
        logger.debug("isEmpty(table={}) - start", table);

        Column[] columns = table.getTableMetaData().getColumns();

        // No columns = empty
        if (columns.length == 0)
        {
            return true;
        }

        // Try to fetch first table value
        try
        {
            table.getValue(0, columns[0].getColumnName());
            return false;
        }
        catch (RowOutOfBoundsException e)
        {
            // Not able to access first row thus empty
            return true;
        }
    }

    /**
     * Returns list of tables this operation is applied to. This method
     * allow subclass to do filtering.
     */
    protected ITableIterator iterator(IDataSet dataSet) throws DatabaseUnitException
    {
        return dataSet.iterator();
    }

    /**
     * Returns mapping of columns to ignore by this operation. Each bit set represent
     * a column to ignore.
     */
    BitSet getIgnoreMapping(ITable table, int row) throws DataSetException
    {
        return EMPTY_BITSET;
    }

    /**
     * Returns false if the specified table row have a different ignore mapping
     * than the specified mapping.
     */
    boolean equalsIgnoreMapping(BitSet ignoreMapping, ITable table, int row) throws DataSetException
    {
        return true;
    }

    abstract OperationData getOperationData(ITableMetaData metaData,
                                            BitSet ignoreMapping, IDatabaseConnection connection) throws DataSetException;

    ////////////////////////////////////////////////////////////////////////////
    // DatabaseOperation class

    @Override
	protected String getQualifiedName(String prefix, String name, IDatabaseConnection connection) {
		return super.getQualifiedName(null, name, connection);
	}

    public void execute(IDatabaseConnection connection, IDataSet dataSet)
            throws DatabaseUnitException, SQLException
    {
        /*SQLExtract sqlExtract = SQLExtract.getInstance();

        if(sqlExtract.getExportSqlXmlType() == sqlExtract.XML_EXPORT_FOR_SQL){
            this.executeForSQLExport(connection, dataSet);
        }else if(sqlExtract.getExportSqlXmlType() == sqlExtract.XML_EXPORT_FOR_DBUNIT_INSERT){
            this.executeForDBUnitInsert(connection, dataSet);
        }*/
        this.executeForDBUnitInsert(connection, dataSet);
    }



    public void executeForSQLExport(IDatabaseConnection connection, IDataSet dataSet)
            throws DatabaseUnitException, SQLException
    {
        logger.debug("execute(connection={}, dataSet={}) - start", connection, dataSet);

        DatabaseConfig databaseConfig = connection.getConfig();
        IStatementFactory factory = (IStatementFactory)databaseConfig.getProperty(DatabaseConfig.PROPERTY_STATEMENT_FACTORY);



        // for each table
        ITableIterator iterator = iterator(dataSet);
        while (iterator.next())
        {
            ITable table = iterator.getTable();

            String tableName=table.getTableMetaData().getTableName();
            logger.trace("execute: processing table='{}'", tableName);
            List<String> setSqlLst = new ArrayList<String>();
            setSqlLst.add("delete "+tableName+";\n");


            // Do not process empty table
            if (isEmpty(table))
            {
                continue;
            }

            ITableMetaData metaData = getOperationMetaData(connection, table.getTableMetaData());
            BitSet ignoreMapping = null;
            OperationData operationData = null;
            FileWriter sqlFileWriter = null;

            try
            {
                // For each row
                int start = _reverseRowOrder ? table.getRowCount() - 1 : 0;
                int increment = _reverseRowOrder ? -1 : 1;

                try
                {

                    for (int i = start; ; i = i + increment)
                    {
                        int row = i;

                        // If current row have a different ignore value mapping than
                        // previous one, we generate a new statement
                        if (ignoreMapping == null || !equalsIgnoreMapping(ignoreMapping, table, row))
                        {
                            ignoreMapping = getIgnoreMapping(table, row);
                            operationData = getOperationData(metaData, ignoreMapping, connection);
                        }

                        Map<String,Object> setColumnList = new HashMap<String, Object>();

                        String getSql = operationData.getSql();

                        // for each column
                        Column[] columns = operationData.getColumns();
                        for (int j = 0; j < columns.length; j++)
                        {
                            // Bind value only if not in ignore mapping
                            if (!ignoreMapping.get(j))
                            {
                                Column column = columns[j];
                            	try
								{

								    String tavleValue = table.getValue(row, column.getColumnName()) == null? "" : table.getValue(row, column.getColumnName()).toString();

								    // replaceFirst
								    tavleValue = Matcher.quoteReplacement(tavleValue);

								    if((column.getDataType().isDateTime() || column.getDataType().isNumber()) && tavleValue.isEmpty()){
                                        tavleValue = null;
                                    }

								    String value = DataSetUtils.getSqlValueString(tavleValue, column.getDataType()) == null? "" : DataSetUtils.getSqlValueString(tavleValue, column.getDataType());

								    /**
								     * method refactoring 필요.
								     */
								    if(column.getDataType().isDateTime()){
									    Pattern p = Pattern.compile("'[^''$]*'");
									    String phrase = new String(value);
									    Matcher m = p.matcher(phrase);
									    while (m.find()) {
									        value = m.group();
									    }
//									    System.out.println(phrase + " ==> "+ value);
								    }

								    if(value.indexOf("?") > -1){
								       value =  value.replace("?","{QUESTION_REPLACE}");
                                    }


								    if(getSql.indexOf("?") > -1){
                                        getSql = getSql.replaceFirst("\\?",value);
                                    }


                                    // column datatype에 맞는 값
                                    //DataSetUtils.getSqlValueString(table.getValue(row, column.getColumnName()), column.getDataType())
                                    //setColumnList.put(column.getColumnName(),DataSetUtils.getSqlValueString(table.getValue(row, column.getColumnName()), column.getDataType()));
								}
                                catch (TypeCastException e)
								{
				                	throw new TypeCastException("Error casting value for table '" + table.getTableMetaData().getTableName()
				                			+"' and column '" + column.getColumnName() + "'", e);
								}
                            }
                        }

                        getSql = getSql.replace("{QUESTION_REPLACE}","?");

                        setSqlLst.add(getSql+";"+"\n");

                    }
                }
                catch (RowOutOfBoundsException e)
                {
                	// This exception occurs when records are exhausted
                	// and we reach the end of the table.  Ignore this error

                    // end of table
                }

            }
            catch (Exception e){
                e.printStackTrace();
            }
/*
            try{


                SQLExtract sqlExtract = SQLExtract.getInstance();

                if(sqlExtract.getExportTableType()){
                    sqlExtract.appendFile(setSqlLst);
                }else{
                    try{
                        String grpCd = sqlExtract.getSqlFileGrpCd();

                        String outputFileDirPath = sqlExtract.INPUT_FILE_PATH_SQL_TABLENAME;
                        String projectName = sqlExtract.getProjectName();
                        File outputSqlFilePath = new File(outputFileDirPath  + File.separator + projectName + File.separator + tableName+".sql");
                        sqlFileWriter= new FileWriter(outputSqlFilePath,false);
                        sqlFileWriter.write("");
                        for(String sql : setSqlLst){
                            sqlFileWriter.write(sql);
                        }
                        sqlFileWriter.flush();


                        String fileNm = outputSqlFilePath.getName();

                        SimpleMultipartFileItem fileItem = new SimpleMultipartFileItem(UUID.randomUUID().toString(),grpCd, fileNm,  FilenameUtils.getExtension(fileNm), outputSqlFilePath.length(), null, null, outputSqlFilePath);

                        fileItem.setMultipartFile(new MockMultipartFile(fileNm,new FileInputStream(outputSqlFilePath)));

                        sqlExtract.createFiles(fileItem);

                    }catch (Exception e){
                        e.printStackTrace();
                    }
                }
            }catch (Exception e){
                e.printStackTrace();
            }*/





        }
    }

    private void executeForDBUnitInsert(IDatabaseConnection connection, IDataSet dataSet)
            throws DatabaseUnitException, SQLException
    {
        logger.debug("execute(connection={}, dataSet={}) - start", connection, dataSet);

        DatabaseConfig databaseConfig = connection.getConfig();
        IStatementFactory factory = (IStatementFactory)databaseConfig.getProperty(DatabaseConfig.PROPERTY_STATEMENT_FACTORY);

        // for each table
        ITableIterator iterator = iterator(dataSet);
        while (iterator.next())
        {
            ITable table = iterator.getTable();

            String tableName=table.getTableMetaData().getTableName();
            logger.trace("execute: processing table='{}'", tableName);

            // Do not process empty table
            if (isEmpty(table))
            {
                continue;
            }

            ITableMetaData metaData = getOperationMetaData(connection, table.getTableMetaData());
            BitSet ignoreMapping = null;
            OperationData operationData = null;
            IPreparedBatchStatement statement = null;

            try
            {
                // For each row
                int start = _reverseRowOrder ? table.getRowCount() - 1 : 0;
                int increment = _reverseRowOrder ? -1 : 1;

                try
                {
                    for (int i = start; ; i = i + increment)
                    {
                        int row = i;

                        // If current row have a different ignore value mapping than
                        // previous one, we generate a new statement
                        if (ignoreMapping == null || !equalsIgnoreMapping(ignoreMapping, table, row))
                        {
                            // Execute and close previous statement
                            if (statement != null)
                            {
                                //statement.executeBatch();
                                statement.clearBatch();
                                statement.close();
                            }

                            ignoreMapping = getIgnoreMapping(table, row);
                            operationData = getOperationData(metaData, ignoreMapping, connection);
                            statement = factory.createPreparedBatchStatement(operationData.getSql(), connection);
                        }

                        Map<String,Object> setColumnList = new HashMap<String, Object>();

                        // for each column
                        Column[] columns = operationData.getColumns();
                        for (int j = 0; j < columns.length; j++)
                        {
                            // Bind value only if not in ignore mapping
                            if (!ignoreMapping.get(j))
                            {
                                Column column = columns[j];
                            	try
								{
	                                statement.addValue(table.getValue(row, column.getColumnName()), column.getDataType());

								}
                                catch (TypeCastException e)
								{
				                	throw new TypeCastException("Error casting value for table '" + table.getTableMetaData().getTableName()
				                			+"' and column '" + column.getColumnName() + "'", e);
								}
                            }
                        }
                        statement.addBatch();
                    }
                }
                catch (RowOutOfBoundsException e)
                {
                	// This exception occurs when records are exhausted
                	// and we reach the end of the table.  Ignore this error

                    // end of table
                }

                //statement.executeBatch();
                statement.clearBatch();
            }
            catch (SQLException e)
            {
                final String msg =
                    "Exception processing table name='" + tableName + "'";
                throw new DatabaseUnitException(msg, e);
            }
            catch (Exception e){
                e.printStackTrace();
            }
            finally
            {
            	if (statement != null)
                {
                    statement.close();
                }
            }
        }
    }

    public String toString()
    {
    	StringBuffer sb = new StringBuffer();
    	sb.append(getClass().getName()).append("[");
    	sb.append("_reverseRowOrder=").append(this._reverseRowOrder);
    	sb.append(", super=").append(super.toString());
    	sb.append("]");
    	return sb.toString();
    }

}
