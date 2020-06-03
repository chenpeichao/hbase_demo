package org.pcchen.api;

import org.pcchen.util.HBaseUtils;

import java.io.IOException;

/**
 * 测试HBase工具类
 *
 * @author ceek
 * @create 2020-06-03 9:50
 **/
public class TestConnectionUtils {
    public static void main(String[] args) throws IOException {
        HBaseUtils.makeHBaseConnection();

        HBaseUtils.insertData("pcchen:student", "1003", "info", "gender", "male");

        HBaseUtils.close();
    }
}
