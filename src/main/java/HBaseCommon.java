import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * Created by liuzehui on 2018/8/30.
 */
public class HBaseCommon {

    private String zkAddress;

    public HBaseCommon(String zkAddress) {
        this.zkAddress=zkAddress;
    }

    public  Connection  HBaseConnect() throws IOException{
        Configuration config = HBaseConfiguration.create();
        config.set(HConstants.ZOOKEEPER_QUORUM, zkAddress);
        Connection connection = ConnectionFactory.createConnection(config);
        return connection;
    }

    public void  createTable(String tableName,String familyName) throws IOException{
        Connection connection = HBaseConnect();
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        tableDescriptor.addFamily(new HColumnDescriptor(familyName));
        System.out.print("Creating table:"+tableName);
        Admin admin = connection.getAdmin();
        admin.createTable(tableDescriptor);
        System.out.println(" Done.");
    }


}
