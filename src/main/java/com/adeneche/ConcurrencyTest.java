package com.adeneche;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Reproduction class for DRILL-3771
 *
 */
public class ConcurrencyTest implements Runnable {

  Connection conn = null;

  ConcurrencyTest(Connection conn) {
    this.conn = conn;
  }

  public void run() {
    try {
      selectData();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  // SELECT data
  public void selectData() {
    try {
      executeQuery("SELECT key1, key2 FROM `twoKeyJsn.json` where key2 = 'm'");
    } catch(Exception e) {
      e.printStackTrace();
    }
  }

  // Execute Query
  public void executeQuery(String query) {
    long numBatches = 0;
    try {
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery(query);

      while (rs.next()) {
        numBatches++;
      }

      System.out.printf("Total batches fetches: %d%n", numBatches);

      rs.close();
      stmt.close();
      conn.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static class Options {
    @Option(name = "-d")
    String drillbit;
  }

  private static Options parseArguments(String[] args) {
    final Options options = new Options();
    final CmdLineParser parser = new CmdLineParser(options);

    try {
      parser.parseArgument(args);
    } catch (CmdLineException e) {
      // if there's a problem in the command line,
      // you'll get this exception. this will report
      // an error message.
      System.err.println(e.getMessage());
      System.exit(-1);
    }

    return options;
  }

  public static void main(String args[]) throws Exception {
    final Options options = parseArguments(args);

    final String URL_STRING = "jdbc:drill:schema=dfs.tmp;drillbit=" + options.drillbit;
    Class.forName("org.apache.drill.jdbc.Driver").newInstance();
    Connection conn = DriverManager.getConnection(URL_STRING, "", "");

    ExecutorService executor = Executors.newFixedThreadPool(16);
    try {
      for (int i = 1; i <= 100; i++) {
        executor.submit(new ConcurrencyTest(conn));
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}