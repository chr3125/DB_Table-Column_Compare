package org.dbunit.dataset.stream;

import org.dbunit.dataset.*;
import org.dbunit.dataset.stream.DefaultConsumer;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.dbunit.dataset.stream.IDataSetProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataSetProducerAdapter implements IDataSetProducer {

    /**
     * Logger for this class
     */
    public static final Logger logger = LoggerFactory.getLogger(org.dbunit.dataset.stream.DataSetProducerAdapter.class);

    public static final IDataSetConsumer EMPTY_CONSUMER = new DefaultConsumer();

    public final ITableIterator _iterator;
    public IDataSetConsumer _consumer = EMPTY_CONSUMER;

    public DataSetProducerAdapter(ITableIterator iterator)
    {
        _iterator = iterator;
    }

    public DataSetProducerAdapter(IDataSet dataSet) throws DataSetException
    {
        _iterator = dataSet.iterator();
    }

    @Override
    public void setConsumer(IDataSetConsumer consumer) throws DataSetException {
        logger.debug("setConsumer(consumer) - start");

        _consumer = consumer;
    }

    @Override
    public void produce() throws DataSetException
    {
        logger.debug("produce() - start");

        _consumer.startDataSet();
        while(_iterator.next())
        {
            ITable table = _iterator.getTable();
            ITableMetaData metaData = table.getTableMetaData();

            _consumer.startTable(metaData);
            try
            {
                Column[] columns = metaData.getColumns();
                if (columns.length == 0)
                {
                    _consumer.endTable();
                    continue;
                }

                for (int i = 0; ; i++)
                {
                    Object[] values = new Object[columns.length];
                    for (int j = 0; j < columns.length; j++)
                    {
                        Column column = columns[j];

                        if(column.getDataType().isNumber() || column.getDataType().isDateTime()){
                            values[j] = table.getValue(i, column.getColumnName()) == null? null : table.getValue(i, column.getColumnName());
                        }else{
                            values[j] = table.getValue(i, column.getColumnName()) == null? "" : table.getValue(i, column.getColumnName());
                        }

                    }
                    _consumer.row(values);
                }
            }
            catch (RowOutOfBoundsException e)
            {
                // This exception occurs when records are exhausted
                // and we reach the end of the table.  Ignore this error
                // and close table.

                // end of table
                _consumer.endTable();
            }
        }
        _consumer.endDataSet();
    }
}
