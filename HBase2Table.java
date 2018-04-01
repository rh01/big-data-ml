/*
 * Output the content of an hbase table as '|' separated file
 *
 * Compile:
 *  javac HBase2Table.java
 *
 * Run: 
 *  java HBaseTest <hbase-table> <cf1:col1> ... <cfk:colk>
 *
 *  scan the given hbase-table. for each row, extract the 
 *  specified k columns and output them as a '|' separated record.
 */

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;

import org.apache.log4j.*;

public class HBase2Table {

  public static void main(String[] args) 
         throws MasterNotRunningException, ZooKeeperConnectionException, 
                IOException 
  {
    boolean debugging= true;

    Logger.getRootLogger().setLevel(Level.WARN);

    // 1. parse command line parameters
    if (args.length < 2) {
       System.out.println("Usage: java HBaseTest <hbase-table> <cf1:col1> ... <cfk:colk>");
       return;
    }

    String table_name= args[0];

    int    num_cols= args.length-1;
    String[] cfs= new String[num_cols];
    String[] cols= new String[num_cols];

    for (int i=0; i<num_cols; i++) {
       String cf_col= args[i+1];
       int colon= cf_col.indexOf(':');
       if (colon == -1) {
         System.out.println("Invalid input: " + cf_col);
         return;
       }
       cfs[i]= cf_col.substring(0, colon);
       cols[i]= cf_col.substring(colon+1);
    }

    if (debugging) {
       System.out.println("hbase table name: " + table_name);
       for (int i=0; i<num_cols; i++) {
           System.out.println("Column family: " + cfs[i] + ", column: " + cols[i]);
       }
    }

    // 2. open table
    Configuration configuration = HBaseConfiguration.create();
    HTable table = new HTable(configuration, table_name);

    // 3. scan table
    byte[][] cfs_bytes= new byte[num_cols][];
    byte[][] cols_bytes= new byte[num_cols][];

    Scan scan = new Scan();
    for (int i=0; i<num_cols; i++) {
        cfs_bytes[i]= cfs[i].getBytes();
        cols_bytes[i]= cols[i].getBytes();
        scan.addColumn(cfs_bytes[i], cols_bytes[i]);
    }

    // 3.1 print header
    if (debugging) {
       for (int i=0; i<num_cols; i++) {
           System.out.print(cfs[i] + ":" + cols[i] + "|");
       }
       System.out.println();
    }

    // 3.2 for each row, extract and output
    ResultScanner rs = table.getScanner(scan);
    for (Result r = rs.next(); r != null; r = rs.next()) {
       for (int i=0; i<num_cols; i++) {
           byte[] value= r.getValue(cfs_bytes[i], cols_bytes[i]);
           if (value != null) {
              System.out.print(new String(value)); 
           }
           System.out.print("|");
       }
       System.out.println();
    }
    rs.close();

    // 4. close table
    table.close();

    return;
  }
} // end of HBase2Table
