package com.example;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseExample {
    public static void main(String[] args) {
        // Configurazione HBase
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "alex-VirtualBox,biar,gab-IdeaPad-3-15ADA05"); // Imposta il tuo hostname dello zookeeper
        config.set("hbase.zookeeper.property.clientPort", "2181");

        try (Connection connection = ConnectionFactory.createConnection(config)) {/* */
            // Creazione della tabellaz
            TableName tableName = TableName.valueOf("javaTable");/*
            TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("cf")).build())
                    .build();
            connection.getAdmin().createTable(tableDescriptor);
            */

            // Inserimento di dati
            Table table = connection.getTable(tableName);
            /*
            Put put = new Put(Bytes.toBytes("row1"));
            put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("qualifier1"), Bytes.toBytes("value1"));
            table.put(put);
            */
            
            // Lettura di dati
            Scan scan = new Scan();
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                byte[] value = result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("qualifier1"));
                System.out.println("Found row: " + result + ", with value: " + Bytes.toString(value));
            }

            scanner.close();
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
