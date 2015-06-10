package com.ljq.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * 该类的主要功能是负责建立与 Hive 和 MySQL 的连接, 由于每个连接的开销比较大, 所以此类的设计采用设计模式中的单例模式。
 */
class DBHelper {
        private static Connection connToHive = null;
        private static Connection connToMySQL = null;

        private DBHelper() {
        }

        // 获得与 Hive 连接,如果连接已经初始化,则直接返回
        public static Connection getHiveConn() throws SQLException {
                if (connToHive == null) {
                        try {
                                Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");
                        } catch (ClassNotFoundException err) {
                                err.printStackTrace();
                                System.exit(1);
                        }
                        connToHive = DriverManager.getConnection("jdbc:hive://192.168.11.157:10000/default", "hive", "mysql");
                }
                return connToHive;
        }

        // 获得与 MySQL 连接
        public static Connection getMySQLConn() throws SQLException {
                if (connToMySQL == null) {
                        try {
                                Class.forName("com.mysql.jdbc.Driver");
                        } catch (ClassNotFoundException err) {
                                err.printStackTrace();
                                System.exit(1);
                        }

                        connToMySQL = DriverManager.getConnection("jdbc:mysql://192.168.11.157:3306/hive?useUnicode=true&characterEncoding=UTF8",
                                        "root", "mysql"); //编码不要写成UTF-8
                }
                return connToMySQL;
        }

        public static void closeHiveConn() throws SQLException {
                if (connToHive != null) {
                        connToHive.close();
                }
        }

        public static void closeMySQLConn() throws SQLException {
                if (connToMySQL != null) {
                        connToMySQL.close();
                }
        }
        
        public static void main(String[] args) throws SQLException {
                System.out.println(getMySQLConn());
                closeMySQLConn();
        }

}