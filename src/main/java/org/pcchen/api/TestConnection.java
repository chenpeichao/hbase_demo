package org.pcchen.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.List;

/**
 * 测试连接类
 * 获取不到bdtest-02：注意修改本地hosts中ip映射
 *
 * @author ceek
 * @create 2020-05-19 16:22
 **/
public class TestConnection {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();

        //可以通过conf代码指定，也可以通过在classpath目录下添加hbase-site.xml文件中指定
        conf.set("hbase.zookeeper.quorum", "10.10.32.61");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
//        conf.set("hbase.master", "10.10.32.61:60000");
        Connection connection = ConnectionFactory.createConnection(conf);
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();

        Table student = connection.getTable(TableName.valueOf("student"));
        //查询t01表中所有行数据
        Scan scan = new Scan();
        ResultScanner result = student.getScanner(scan);//得到的是数据集
        Result itemResult = null;
        while ((itemResult = result.next()) != null) {
            List<Cell> cells = itemResult.listCells();//将每条数据存到Cell对象里
            for (Cell cell : cells) {
                System.out.println(new String(CellUtil.cloneFamily(cell)) + new String(CellUtil.cloneQualifier(cell)) + new String(CellUtil.cloneValue(cell)));
            }
        }


//        boolean studentBoolean = admin.tableExists("student");
//        Configuration configuration = admin.getConfiguration();
//        System.out.println(configuration.toString());
    }
}
