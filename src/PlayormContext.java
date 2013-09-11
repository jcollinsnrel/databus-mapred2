import java.lang.reflect.InvocationTargetException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.base.Bootstrap;
import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.alvazan.orm.api.z3api.NoSqlTypedSession;
import com.alvazan.orm.api.z5api.NoSqlSession;
import com.alvazan.orm.api.z8spi.action.Column;
import com.alvazan.orm.api.z8spi.conv.StandardConverters;
import com.alvazan.orm.api.z8spi.meta.DboColumnIdMeta;
import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;
import com.alvazan.orm.api.z8spi.meta.TypedRow;
import com.alvazan.play.NoSql;



public class PlayormContext implements IPlayormContext {
	
	static final Logger log = LoggerFactory.getLogger(PlayormContext.class);

	private NoSqlEntityManager sourceMgr;
    private NoSqlEntityManager destMgr;
    private static final int BATCH_SIZE=500;
    private int batchCount = 0;
    
    public PlayormContext() {
    	
    }
    
    public void initialize(String keyspace, String cluster1, String seeds1, String port1, String keyspace2, String cluster2, String seeds2, String port2) {
    	Map<String, Object> props = new HashMap<String, Object>();
		props.put(Bootstrap.TYPE, "cassandra");
		props.put(Bootstrap.CASSANDRA_KEYSPACE, keyspace);
		props.put(Bootstrap.CASSANDRA_CLUSTERNAME, cluster1);
		props.put(Bootstrap.CASSANDRA_SEEDS, seeds1);
		props.put(Bootstrap.CASSANDRA_THRIFT_PORT, port1);
		props.put(Bootstrap.AUTO_CREATE_KEY, "create");
		//props.put(Bootstrap.LIST_OF_EXTRA_CLASSES_TO_SCAN_KEY, classes);

		NoSqlEntityManagerFactory factory1 = Bootstrap.create(props, Thread.currentThread().getContextClassLoader());  //that 'null' is a classloader that supposed to come from play...  does null work?
		sourceMgr = factory1.createEntityManager();
		
		Map<String, Object> props2 = new HashMap<String, Object>();
		props2.put(Bootstrap.TYPE, "cassandra");
		props2.put(Bootstrap.CASSANDRA_KEYSPACE, keyspace2);
		props2.put(Bootstrap.CASSANDRA_CLUSTERNAME, cluster2);
		props2.put(Bootstrap.CASSANDRA_SEEDS, seeds2);
		props2.put(Bootstrap.CASSANDRA_THRIFT_PORT, port2);
		props2.put(Bootstrap.AUTO_CREATE_KEY, "create");
		//props2.put(Bootstrap.LIST_OF_EXTRA_CLASSES_TO_SCAN_KEY, classes);

		NoSqlEntityManagerFactory factory2 = Bootstrap.create(props2, Thread.currentThread().getContextClassLoader());  //that 'null' is a classloader that supposed to come from play...  does null work?
		destMgr = factory2.createEntityManager();
    }
    
    public String getSrcTableDesc(String tableNameIfVirtual) {
    	DboTableMeta meta = sourceMgr.find(DboTableMeta.class, tableNameIfVirtual);
    	String tableDesc = "";
    	tableDesc = tableNameIfVirtual+": "+meta.toString()+", isTimeSeries:"+meta.isTimeSeries()+", partitionSize: "+meta.getTimeSeriesPartionSize();
    	return tableDesc;
    }
    
    public String getDestTableDesc(String tableNameIfVirtual) {
    	DboTableMeta meta = destMgr.find(DboTableMeta.class, tableNameIfVirtual);
    	String tableDesc = "";
    	tableDesc = tableNameIfVirtual+": "+meta.toString()+", isTimeSeries:"+meta.isTimeSeries()+", partitionSize: "+meta.getTimeSeriesPartionSize();
    	return tableDesc;
    }

    
    public String getTableNameFromKey(byte[] key) {
    	return DboColumnIdMeta.fetchTableNameIfVirtual(key);
    }
    
    public boolean sourceTableIsStream(String tableNameIfVirtual, byte[] key) {
    	DboTableMeta meta = sourceMgr.find(DboTableMeta.class, tableNameIfVirtual);
		DboColumnMeta[] allColumns = meta.getAllColumns().toArray(new DboColumnMeta[]{});

		String idColumnName = meta.getIdColumnMeta().getColumnName();
    	if (allColumns.length==1 && "value".equals(allColumns[0].getColumnName()) && "time".equals(idColumnName)) 
    		return true;
    	return false;
	}

    @Override
    public void postNormalTable(Map<String, Object> values, String tableNameIfVirtual, Object pkValue) {
    	NoSqlTypedSession typedSession = destMgr.getTypedSession();
    	DboTableMeta table = destMgr.find(DboTableMeta.class, tableNameIfVirtual);
    	
    	if (log.isInfoEnabled())
			log.info("normal table name = '" + table.getColumnFamily() + "'");
		
		DboColumnMeta idColumnMeta = table.getIdColumnMeta();
		Object rowKey = convertToStorage(idColumnMeta, pkValue);
		String cf = table.getColumnFamily();

		TypedRow row = typedSession.createTypedRow(table.getColumnFamily());
		row.setRowKey(rowKey);			

		Collection<DboColumnMeta> cols = table.getAllColumns();
		
		long timestamp = System.currentTimeMillis();
		for(DboColumnMeta col : cols) {
			Object node = values.get(col.getColumnName());
			if(node == null) {
//				if (log.isWarnEnabled())
//	        		log.warn("The table you are inserting '"+tableNameIfVirtual+"' requires column='"+col.getColumnName()+"' to be set and is not found in source data");
//				throw new RuntimeException("The table you are inserting '"+tableNameIfVirtual+"' requires column='"+col.getColumnName()+"' to be set and is not found in source data");
				continue;
			}

			addColumnData(row, col, node, timestamp);
		}
		
		//This method also indexes according to the meta data as well
		typedSession.put(cf, row);
		batchCount++;
		if (batchCount >= BATCH_SIZE) {
			typedSession.flush();
			batchCount = 0;
		}
	}
    
    private void addColumnData(TypedRow row, DboColumnMeta col, Object node, long time) {
		Object newValue = convertToStorage(col, node);
		row.addColumn(col.getColumnName(), newValue);
	}
    
    public void postTimeSeriesToDest(String tableNameIfVirtual, Object pkValue, String valueAsString) {

    	NoSqlTypedSession typedSession = destMgr.getTypedSession();
    	DboTableMeta table = destMgr.find(DboTableMeta.class, tableNameIfVirtual);
    	if (table == null) {
    		if (log.isInfoEnabled()) 
    			log.info("--- owning table "+tableNameIfVirtual+" on dest side does not exist, this probably means that the row we are copying belongs to a table taht did not get ported... skipping row.");
    		return;
    	}
		if (log.isInfoEnabled())
			log.info("writing to Timeseries, table name!!!!!!! = '" + tableNameIfVirtual + "' table is "+ table);
		String cf = table.getColumnFamily();

		DboColumnMeta idColumnMeta = table.getIdColumnMeta();
		//rowKey better be BigInteger
		Object timeStamp = convertToStorage(idColumnMeta, pkValue);
		byte[] colKey = idColumnMeta.convertToStorage2(timeStamp);
		BigInteger time = (BigInteger) timeStamp;
		long longTime = time.longValue();
		//find the partition
		Long partitionSize = table.getTimeSeriesPartionSize();
		long partitionKey = calculatePartitionId(longTime, partitionSize);

		TypedRow row = typedSession.createTypedRow(table.getColumnFamily());
		BigInteger rowKey = new BigInteger(""+partitionKey);
		row.setRowKey(rowKey);

		DboTableMeta meta = destMgr.find(DboTableMeta.class, "partitions");
		byte[] partitionsRowKey = StandardConverters.convertToBytes(table.getColumnFamily());
		byte[] partitionBytes = StandardConverters.convertToBytes(rowKey);
		Column partitionIdCol = new Column(partitionBytes, null);
		NoSqlSession session = destMgr.getSession();
		List<Column> columns = new ArrayList<Column>();
		columns.add(partitionIdCol);
		session.put(meta, partitionsRowKey, columns);
		
		Collection<DboColumnMeta> cols = table.getAllColumns();
		DboColumnMeta col = cols.iterator().next();
		Object newValue = convertToStorage(col, valueAsString);
		if (newValue == null)
			log.warn("GOT A NULL value posting to timeseries!  newValue is "+newValue+" valueAsString is "+valueAsString+
					" tableNameIfVirtual is "+tableNameIfVirtual+" partitionKey is "+partitionKey+" col is "+col.getColumnName());

		byte[] val = col.convertToStorage2(newValue);
		if (val==null || val.length==0)
			log.warn("GOT A NULL OR EMPTY byte[] value posting to timeseries!  val is '"+val+"' newValue is "+newValue+" valueAsString is "+valueAsString+
				" tableNameIfVirtual is "+tableNameIfVirtual+" partitionKey is "+partitionKey+" col is "+col.getColumnName());
		row.addColumn(colKey, val, null);

		batchCount++;
		if (batchCount >= BATCH_SIZE) {
			//This method also indexes according to the meta data as well
			typedSession.put(cf, row);
			session.flush();
			typedSession.flush();
			batchCount = 0;
		}
	}
    
    public long calculatePartitionId(long longTime, Long partitionSize) {
		long partitionId = (longTime / partitionSize) * partitionSize;
		if(partitionId < 0) {
			//if partitionId is less than 0, it incorrectly ends up in the higher partition -20/50*50 = 0 and 20/50*50=0 when -20/50*50 needs to be -50 partitionId
			if(Long.MIN_VALUE+partitionSize >= partitionId)
				partitionId = Long.MIN_VALUE;
			else
				partitionId -= partitionSize; //subtract one partition size off of the id
		}

		return partitionId;
	}
    
    public Object convertToStorage(DboColumnMeta col, Object someVal) {
		try {
			if(someVal == null)
				return null;
			else if("null".equals(someVal))
				return null; //a fix for when they pass us "null" instead of null
			
			String val = ""+someVal;
			if(val.length() == 0)
				val = null;
			return col.convertStringToType(val);
		} catch(Exception e) { 
			//Why javassist library throws a checked exception, I don't know as we can't catch a checked exception here
			if(e instanceof InvocationTargetException &&
					e.getCause() instanceof NumberFormatException) {
				if (log.isWarnEnabled())
	        		log.warn("Cannot convert value="+someVal+" for column="+col.getColumnName()+" table="+col.getOwner().getRealColumnFamily()+" as it needs to be type="+col.getClassType(), e.getCause());
			}
			throw new RuntimeException(e);
		}
	}
    
    public String getSourceIdColumnValue(String tableNameIfVirtual, byte[] key) {
    	DboTableMeta meta = sourceMgr.find(DboTableMeta.class, tableNameIfVirtual);
    	byte[] nonvirtkey = meta.getIdColumnMeta().unformVirtRowKey(key);
		return ""+meta.getIdColumnMeta().convertFromStorage2(nonvirtkey);
    }
    
	public String getSourceIdColumnName(String tableNameIfVirtual) {
		DboTableMeta meta = sourceMgr.find(DboTableMeta.class, tableNameIfVirtual);
		return meta.getIdColumnMeta().getColumnName();
	}

	public Object sourceConvertFromBytes(String tableNameIfVirtual, String columnName,
			byte[] valuearray) {
		DboTableMeta meta = sourceMgr.find(DboTableMeta.class, tableNameIfVirtual);
		DboColumnMeta columnMeta = meta.getColumnMeta(columnName);
		return columnMeta.convertFromStorage2(valuearray);
	}
	
	public String bytesToString(byte[] namearray) {
		return StandardConverters.convertFromBytes(String.class, namearray);
	}
	
	public void flushAll() {
    	NoSqlTypedSession typedSession = destMgr.getTypedSession();
		NoSqlSession session = destMgr.getSession();

		session.flush();
		typedSession.flush();
		batchCount = 0;
	}

}
