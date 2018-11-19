/**
 * Created by liuzehui on 2018/8/29.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;


import java.io.IOException;

public class HBaseTableConnect {

    public String hbaseTableName;
    public String zkAddress;

    private static Configuration hBaseConf = HBaseConfiguration.create();

    public HBaseTableConnect(String zkAddr, String tableName) {
        this.hbaseTableName = tableName;
        this.zkAddress = zkAddr;
    }



    public Table create_connection(){
        Table hbaseTable = null;
        try {
            Connection connection = new HBaseCommon(zkAddress).HBaseConnect();
            hbaseTable = connection.getTable(TableName.valueOf(hbaseTableName));
        } catch (IOException e) {
            System.out.println("hbaseTable init error: "+ e);
        }
        return hbaseTable;
    }

}
