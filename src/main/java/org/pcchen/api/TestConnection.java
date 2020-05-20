package org.pcchen.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

/**
 * 测试连接类
 *
 * @author ceek
 * @create 2020-05-19 16:22
 **/
public class TestConnection {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "10.10.32.59");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        Connection connection = ConnectionFactory.createConnection(conf);
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();

        boolean student = admin.tableExists("student");
        Configuration configuration = admin.getConfiguration();
        System.out.println(configuration.toString());
    }

    public static boolean isTableExist(String tableName) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
        //在HBase中管理、访问表需要先创建HBaseAdmin对象
        Configuration conf = HBaseConfiguration.create();
        HBaseAdmin admin = new HBaseAdmin(conf);
        return admin.tableExists(tableName);
    }
}
