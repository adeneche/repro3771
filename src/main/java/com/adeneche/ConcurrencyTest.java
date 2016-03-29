package com.adeneche;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Reproduction class for DRILL-3763:
 * - submit N queries in parallel
 * - shouldCancel one query after X seconds
 *
 */
public class ConcurrencyTest implements Runnable {

  final int queryNum;
  final Connection conn;
  final CountDownLatch doneSignal;
  final boolean shouldCancel;

  ConcurrencyTest(int queryNum, Connection conn, CountDownLatch doneSignal, boolean cancel) {
    this.queryNum = queryNum;
    this.conn = conn;
    this.doneSignal = doneSignal;
    this.shouldCancel = cancel;
  }

  public void run() {
    try {
      executeQuery("SELECT key1, key2 FROM `twoKeyJsn.json` where key2 = 'm'");
    } catch (Exception e) {
      System.err.printf("[query=%d]: ", queryNum);
      e.printStackTrace();
    }
  }

  // Execute Query
  private void executeQuery(String query) throws SQLException {
    long numRows = 0;
    try {
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery(query);

      while (rs.next()) {
        numRows++;
        if (shouldCancel && numRows == 10000) {
          stmt.cancel();
          break;
        }
      }

      if (shouldCancel) {
        System.out.printf("[queryId=%d]: Query cancelled after fetching %d rows%n", queryNum, numRows);
      } else {
        System.out.printf("[queryId=%d]: Query completed after fetching %d rows%n", queryNum, numRows);
      }

      rs.close();
      stmt.close();
      conn.close();
    } finally {
      doneSignal.countDown();
    }
  }

  private static class Options {
    @Option(name = "-d", required = true)
    String drillbit;
    @Option(name = "-n")
    int numQueries = 10;
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

    ExecutorService executor = Executors.newFixedThreadPool(options.numQueries);
    CountDownLatch doneSignal = new CountDownLatch(options.numQueries);
    try {
      for (int i = 0; i < options.numQueries; i++) {
        final Connection conn = DriverManager.getConnection(URL_STRING, "", "");
        executor.submit(new ConcurrencyTest(i, conn, doneSignal, i == options.numQueries-1));
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }

    doneSignal.await();
    System.out.println("Shutting down...");
    executor.shutdown();
  }
}
