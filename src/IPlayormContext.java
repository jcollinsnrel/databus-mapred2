import java.util.Map;

import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;


public interface IPlayormContext {

    public void initialize(String keyspace, String cluster1, String seeds1, String port1, String keyspace2, String cluster2, String seeds2, String port2);
    public String getTableNameFromKey(byte[] key);
    public boolean sourceTableIsStream(String tableNameIfVirtual, byte[] key);
    public void postTimeSeriesToDest(String tableNameIfVirtual, Object pkValue, String valueAsString);
    public Object convertToStorage(DboColumnMeta col, Object someVal);
    public String getSourceIdColumnValue(String tableNameIfVirtual, byte[] key);
	public String getSourceIdColumnName(String tableNameIfVirtual);
	public Object sourceConvertFromBytes(String tableNameIfVirtual, String columnName, byte[] valuearray);
	public String bytesToString(byte[] namearray);
	public void postNormalTable(Map<String, Object> values, String tableNameIfVirtual, Object pkValue);
	
    public String getSrcTableDesc(String tableNameIfVirtual);
    public String getDestTableDesc(String tableNameIfVirtual);
	public void flushAll();


}
