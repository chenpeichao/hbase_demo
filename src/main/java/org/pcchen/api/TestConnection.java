package org.pcchen.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.util.Bytes;

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
//        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
//        conf.set("hbase.master", "10.10.32.61:60000");
        //1、获取连接对象
        Connection connection = ConnectionFactory.createConnection(conf);
        //2、获取操作对象：admin
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();

        //3、操作数据库
        //3.1、判断命名空间
        try {
            admin.getNamespaceDescriptor("pcchen");
        } catch (IOException e) {
            NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create("pcchen").build();
            admin.createNamespace(namespaceDescriptor);
        }

        //3.2、判断hbase中是否存在某张表
        TableName tableName = TableName.valueOf("pcchen:student");
        boolean flag = admin.tableExists(tableName);
        System.out.println("表是否存在：" + flag);

        if (!flag) {
            //当表不存在时
            //创建表，添加列族
            HColumnDescriptor hd = new HColumnDescriptor("info");
            HTableDescriptor td = new HTableDescriptor(tableName);
            td.addFamily(hd);

            admin.createTable(td);
            System.out.println("创建表【" + tableName + "】成功!");
        } else {
            String rowKey = "1001";

            //3.3、当表存在时，查询数据以及添加数据
            Table table = connection.getTable(tableName);

            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);

            if (!result.isEmpty()) {
                System.out.println("表【" + tableName + "】已经存在rowkey为【" + rowKey + "】的数据!");
                for (Cell cell : result.listCells()) {
                    System.out.println("rowKey：" + Bytes.toString(CellUtil.cloneRow(cell)));
                    System.out.println("Family：" + Bytes.toString(CellUtil.cloneFamily(cell)));
                    System.out.println("Qualifier：" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                    System.out.println("Value：" + Bytes.toString(CellUtil.cloneValue(cell)));
                }
            } else {
                System.out.println("表【" + tableName + "】已经不存在rowkey为【" + rowKey + "】的数据!");
                Put put = new Put(Bytes.toBytes(rowKey));
                System.out.println(new String(put.getRow()));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name2"), Bytes.toBytes("wangwu"));

                table.put(put);
            }

            //删除指定rowkey数据
            new TestConnection().deleteByRowkey((HTable) table, "info");
            //获取scan全部数据
            new TestConnection().getScanner((HTable) table);
            table.close();
            connection.close();
        }

    }

    /**
     * 判断指定表是否存在
     *
     * @param admin
     * @param tableName
     * @return
     */
    public boolean isTableExist(HBaseAdmin admin, String tableName) {
        try {
            return admin.tableExists(tableName);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 当表不存在时，创建表
     *
     * @param admin
     * @param tableName
     */
    public void createTable(HBaseAdmin admin, String tableName, String... columnFailies) {
        try {
            if (!admin.tableExists(tableName)) {
                HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

                //向表中插入列族
                for (String columnFaily : columnFailies) {
                    hTableDescriptor.addFamily(new HColumnDescriptor(columnFaily.getBytes("utf-8")));
                }

                admin.createTable(hTableDescriptor);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除表
     * 删除表之前需要先将表disable
     *
     * @param admin
     * @param tableName
     */
    public void deleteTable(HBaseAdmin admin, String tableName) {
        try {
            if (isTableExist(admin, tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
                System.out.println("表【" + tableName + "】删除成功");
            } else {
                System.out.println("表【" + tableName + "】不存在");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 根据rowkey删除表中记录
     */
    public void deleteByRowkey(HTable table, String rowKey) {
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        try {
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除指定列族下的指定字段的记录
     * @param table
     * @param rowKey
     * @param family
     * @param qualify
     */
    public void deleteByQualify(HTable table, String rowKey, String family, String qualify) {
        Delete delete = new Delete(Bytes.toBytes(rowKey));

        delete.addColumns(Bytes.toBytes(family), Bytes.toBytes(qualify));

        try {
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 遍历所有数据
     *
     * @param table
     */
    public void getScanner(HTable table) {
        Scan scan = new Scan();
        try {
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                if (!result.isEmpty()) {
                    for (Cell cell : result.listCells()) {
                        System.out.print("row: " + Bytes.toString(CellUtil.cloneRow(cell)));
                        System.out.print("; family: " + Bytes.toString(CellUtil.cloneFamily(cell)));
                        System.out.print("; qualifier: " + Bytes.toString(CellUtil.cloneQualifier(cell)));
                        System.out.print("; value: " + Bytes.toString(CellUtil.cloneValue(cell)));
                        System.out.println("-------------------------------------");
                    }
                }
                System.out.println("================================");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
