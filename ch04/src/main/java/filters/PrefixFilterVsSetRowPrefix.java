package filters;

// cc PrefixFilterExample Example using the prefix based filter
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

public class PrefixFilterVsSetRowPrefix {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    String tbl = "t100";
    helper.dropTable(tbl);
    
    byte[][] splitKeys = { Bytes.toBytes("row-10"), Bytes.toBytes("row-20"), Bytes.toBytes("row-30"), Bytes.toBytes("row-40") };
    byte[] prefix = Bytes.toBytes("row-21");

    helper.createTable(tbl, splitKeys, "cf1");

    System.out.println("Adding rows to table...");
    helper.fillTable(tbl, 0, 100, 1, "cf1");

    Connection connection = ConnectionFactory.createConnection(conf);
    Table table = connection.getTable(TableName.valueOf("testtable"));

    // vv PrefixFilterExample

    System.out.println("Scan with Prefix Filter and no Start Row:");
    Scan scan = new Scan();

    Filter filter = new PrefixFilter(prefix);
    scan.setFilter(filter);
    
    scan.setScanMetricsEnabled(true);
    ResultScanner scanner = table.getScanner(scan);

    System.out.println("Results of scan:");
    for (Result result : scanner) {
      helper.dumpResult(result);
    }
    System.out.println("Metrics from scan with PrefixFilter");
    helper.dumpScanMetrics(scan);
    
    System.out.println("Scan with setRowPrefixFilter");
    Scan scan2 = new Scan();
    scan2.setRowPrefixFilter(prefix);
    scan2.setScanMetricsEnabled(true);
    scanner = table.getScanner(scan2);
    System.out.println("Results of scan:");
    for (Result result : scanner) {
      helper.dumpResult(result);
    }
    System.out.println("Metrics from scan with setRowPrefixFilter");
    helper.dumpScanMetrics(scan2);

    scanner.close();

  }
  
  
  
  
  
}
