import java.io.IOException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;

import org.apache.cassandra.db.IColumn;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fromporm.conv.StandardConverters;


public class DatabusCopyMapperImpl {
	static final Logger log = LoggerFactory.getLogger(DatabusCopyMapperImpl.class);

	static private IPlayormContext playorm = null;
	static long mapcounter=0;
	static final String KEYSPACE = "databus5";
	static final String KEYSPACE2 = "databus";
	
	private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    
    private String[] streamColNames=new String[]{"time", "value"};


	public DatabusCopyMapperImpl () {
		//String cluster1 = "QACluster";
		String cluster1 = "DatabusCluster";
		//String seeds1 = "sdi-prod-01:9160,sdi-prod-02:9160,sdi-prod-03:9160,sdi-prod-04:9160";
		String seeds1 = "a1.bigde.nrel.gov:9160,a2.bigde.nrel.gov:9160,a3.bigde.nrel.gov:9160,a4.bigde.nrel.gov:9160,a5.bigde.nrel.gov:9160,a6.bigde.nrel.gov:9160,a7.bigde.nrel.gov:9160,a8.bigde.nrel.gov:9160,a9.bigde.nrel.gov:9160,a10.bigde.nrel.gov:9160,a11.bigde.nrel.gov:9160,a12.bigde.nrel.gov:9160";
		String port1 = "9160";
		
		//String cluster2 = "QAClusterB";
		String cluster2 = "DatabusClusterB";
		//String seeds2 = "sdi-prod-01:9158,sdi-prod-02:9158,sdi-prod-03:9158,sdi-prod-04:9158";
		String seeds2 = "a1.bigde.nrel.gov:9158,a2.bigde.nrel.gov:9158,a3.bigde.nrel.gov:9158,a4.bigde.nrel.gov:9158,a5.bigde.nrel.gov:9158,a6.bigde.nrel.gov:9158,a7.bigde.nrel.gov:9158,a8.bigde.nrel.gov:9158,a9.bigde.nrel.gov:9158,a10.bigde.nrel.gov:9158,a11.bigde.nrel.gov:9158,a12.bigde.nrel.gov:9158";
		String port2 = "9158";
		try{
//			System.out.println("the current contextclassloader is "+Thread.currentThread().getContextClassLoader()+" this thread is "+Thread.currentThread());
			Class playormcontextClass = Thread.currentThread().getContextClassLoader().loadClass("PlayormContext");
//			System.out.println("loaded the class for PlayormContext it is "+playormcontextClass);
			Object playormContextObj = playormcontextClass.newInstance();
			Method initmethod = playormcontextClass.getDeclaredMethod("initialize", String.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class);
			initmethod.invoke(playormContextObj, KEYSPACE, cluster1, seeds1, port1, KEYSPACE2, cluster2, seeds2, port2);
			playorm = (IPlayormContext)playormContextObj;
		}
		catch (Exception e){
			e.printStackTrace();
			throw new RuntimeException(e);
		}

	}

	
	public void map(ByteBuffer keyData, SortedMap<ByteBuffer, IColumn> columns, Context context) throws IOException, InterruptedException
    {
		
		byte[] key = new byte[keyData.remaining()];
		keyData.get(key);
		if (key.length==0) {
			log.error("GOT A KEY THAT IS SIZE 0!!  WHAT DOES THAT MEAN?");
			return;
		}
    	mapcounter++;
    	String tableNameIfVirtual = playorm.getTableNameFromKey(key);
    	
    	if (mapcounter%1000 == 1) {
    		log.info("called map "+mapcounter+" times.");
    		//when this was writing to context instead of doing the copy directly in the map phase it was 
    		//timing out, this prevents that.  Now that we are copying the data in the map phase it is not needed:
    		//context.progress();
    	}
		
		if (playorm.sourceTableIsStream(tableNameIfVirtual, key)) {
			transferStream(key, columns, tableNameIfVirtual, context);
		}
		else {
			transferOrdinary(key, columns, tableNameIfVirtual, context);
		}
		
		
    }
    

	private void transferOrdinary(byte[] key, SortedMap<ByteBuffer, IColumn> columns, String tableNameIfVirtual, Context context) throws IOException, InterruptedException {
		
		String idValue = playorm.getSourceIdColumnValue(tableNameIfVirtual, key);
		String idColName = playorm.getSourceIdColumnName(tableNameIfVirtual);
		//log.info("HOW EXCITING!!!  WE GOT A RELATIONAL ROW! for table "+tableNameIfVirtual+" keyColumn = "+idColName+" value="+idValue);
	
		Map<String, Object> values = new HashMap<String, Object>();
		for (IColumn col:columns.values()) {    		
			byte[] namearray = new byte[col.name().remaining()];
    		col.name().get(namearray);
    		byte[] valuearray = new byte[col.value().remaining()];
    		col.value().get(valuearray);
			String colName = playorm.bytesToString(namearray); 
			Object objVal = playorm.sourceConvertFromBytes(tableNameIfVirtual, colName, valuearray);
			values.put(colName, objVal);
		}
		String pkValue = playorm.getSourceIdColumnValue(tableNameIfVirtual, key);

		playorm.postNormalTable(values, tableNameIfVirtual, pkValue);
		word.set(tableNameIfVirtual);
        context.write(word, one);
	}


	private void transferStream(byte[] key, SortedMap<ByteBuffer, IColumn> columns, String tableNameIfVirtual, Context context) throws IOException, InterruptedException {
		String time = playorm.getSourceIdColumnValue(tableNameIfVirtual, key);
		String valueAsString = null;
		
		//we are only in here because this is a stream, there is only one column and it's name is "value":
		int index = 0;
		for (IColumn col:columns.values()) {
			index++;
			//EXPERIMENTAL!  'time' should always be the first col.  I don't want to read it to find out because that slows us down, 
			//so try just assuming that it actually is always first and skip it:
			if (index == 1)
				continue;
			
    		byte[] valuearray = new byte[col.value().remaining()];
    		col.value().get(valuearray);

    		String colName = streamColNames[index-1];
    		try {
    			valueAsString = ""+playorm.sourceConvertFromBytes(tableNameIfVirtual, "value", valuearray);
    			//try to account for every case of 'null' or empty we can think of:
    			if (valueAsString == null || "".equals(valueAsString) || "null".equalsIgnoreCase(valueAsString))
    				log.warn("got a null or empty value in a timeseries! valueAsString is '"+valueAsString+"', tableNameIfVirtual is "+tableNameIfVirtual+" valuearray is "+valuearray);
    		}
    		catch (Exception e) {
    			log.error("failed getting value from bytes!!!!! val[] len is "+valuearray.length+" column is "+colName+" table name is "+tableNameIfVirtual+" now attempting both bigint and bigdec");
    			System.err.println("failed getting value from bytes!!!!! val[] len is "+valuearray.length+" column is "+colName+" table name is "+tableNameIfVirtual+" now attempting both bigint and bigdec");	
    			throw new RuntimeException(e);
    		}
		}
		
		if ((""+Integer.MAX_VALUE).equals(valueAsString)) {
			log.info("NOT POSTING TO TIMESERIES BECAUSE VALUE IS Integer.MAX_VALUE!!!! from table='"+ playorm.getSrcTableDesc(tableNameIfVirtual)+" to table="+playorm.getDestTableDesc(tableNameIfVirtual) +"' key="+time+", value="+valueAsString+" mapcounter is "+mapcounter);
			word.set(tableNameIfVirtual+" not written because MAX_VALUE");
	        context.write(word, one);
	        return;
		}
		
		playorm.postTimeSeriesToDest(tableNameIfVirtual, time, valueAsString);
		word.set(tableNameIfVirtual);
        context.write(word, one);
	}
	
	public void cleanup() {
		playorm.flushAll();
	}

}
