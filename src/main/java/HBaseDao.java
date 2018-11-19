
import org.apache.hadoop.hbase.client.*;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by liuzehui on 2018/8/29.
 */
public class HBaseDao {

    private static Logger logger = Logger.getLogger(HBaseDao.class);
    private String zkAddr;
    private String TABLE_NAME;
    private String FAMILY_NAME;
    private String ContentCol;
    private String HashCodeCol;
    private  String IndexCol;
    private Table hbaseTable;

    public HBaseDao(String zkAddr,String family_name,String table_name,String contentCol,String hashCodeCol,String indexCol) {
        this.zkAddr=zkAddr;
        this.FAMILY_NAME=family_name;
        this.TABLE_NAME=table_name;
        this.ContentCol=contentCol;
        this.HashCodeCol=hashCodeCol;
        this.IndexCol=indexCol;
        this.hbaseTable=new HBaseTableConnect(zkAddr,TABLE_NAME).create_connection();
    }



    public void writeData(byte[] rowKey, byte[] Data, byte[] hashCode, byte[] indexInfo) {
        if (rowKey == null || rowKey.length == 0)
            return;

        Put put = new Put(rowKey);
        put.addColumn(FAMILY_NAME.getBytes(), ContentCol.getBytes(), Data);
        put.addColumn(FAMILY_NAME.getBytes(), HashCodeCol.getBytes(), hashCode);
        if (indexInfo.length > 0)
            put.addColumn(FAMILY_NAME.getBytes(), IndexCol.getBytes(), indexInfo);
        try {
            hbaseTable.put(put);
        } catch (Exception e) {
            logger.error("Write  data into hbase error: " + e);
        }
    }

    public byte[] getData(byte[] rowKey) {
        if (rowKey == null || rowKey.length == 0)
            return null;
        try {
            Get get = new Get(rowKey);
            Result rs = hbaseTable.get(get);
            return rs.getValue(FAMILY_NAME.getBytes(), ContentCol.getBytes());
        } catch (IOException e) {
            logger.error("Read  data of " + rowKey.toString() + " from hbase error: " + e);
        }
        return null;
    }

    public List<byte[]> getDataList(List<byte[]> rowKeyList) {
        try {
            List<Get> getList = new ArrayList();
            for (byte[] rowkey : rowKeyList) {
                Get get = new Get(rowkey);
                getList.add(get);
            }
            Result[] results = hbaseTable.get(getList);
            List<byte[]> resultList = new ArrayList<byte[]>();
            for (Result result : results) {
                resultList.add(result.getValue(FAMILY_NAME.getBytes(), ContentCol.getBytes()));
            }
            return resultList;
        } catch (IOException e) {
            logger.error("Read data of row key list from hbase error: " + e);
        }

        return null;
    }
    public List<byte[]> getAllData() throws IOException {
        Scan scan = new Scan();
        scan.setBatch(1000);
        ResultScanner rs =hbaseTable.getScanner(scan);
        Iterator<Result> iter = rs.iterator();
        int iCount = 0;
        while (iter.hasNext()) {
            Result res = iter.next();
            iCount ++;
            System.out.println("row key:" + res.getRow().toString());
            System.out.println("dr_data:" + res.getValue(FAMILY_NAME.getBytes(),ContentCol.getBytes()).toString());
        }
        logger.info("Total record:" + iCount);
        return null;
    }
}
