package org.pcchen.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * HBase工具类
 *
 * @author ceek
 * @create 2020-06-03 9:06
 **/
public class HBaseUtils {
    private static ThreadLocal<Connection> connHolder = new ThreadLocal<Connection>();

    private HBaseUtils() {
    }

    public static void makeHBaseConnection() throws IOException {
        Connection conn = connHolder.get();

        if (null == conn) {
            Configuration conf = HBaseConfiguration.create();

            //可以通过conf代码指定，也可以通过在classpath目录下添加hbase-site.xml文件中指定
            conf.set("hbase.zookeeper.quorum", "10.10.32.61");
//        conf.set("hbase.zookeeper.property.clientPort", "2181");
            conf.set("hbase.zookeeper.property.clientPort", "2181");

            conn = ConnectionFactory.createConnection(conf);
            connHolder.set(conn);
        }
    }

    public static void insertData(String tableName, String rowKey, String family, String qualify, String value) throws IOException {
        Connection conn = connHolder.get();

        Table table = conn.getTable(TableName.valueOf(Bytes.toBytes(tableName)));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualify), Bytes.toBytes(value));

        table.put(put);
        table.close();
    }

    public static void close() throws IOException {
        if (null != connHolder.get()) {
            connHolder.get().close();
            connHolder.remove();
        }
    }
}