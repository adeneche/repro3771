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
import java.util.concurrent.ThreadFactory;

/**
 * Reproduction class for DRILL-3771
 *
 */
public class ConcurrencyTest implements Runnable {

  final int queryNum;
  final Connection conn;
  final CountDownLatch doneSignal;

  ConcurrencyTest(int queryNum, Connection conn, CountDownLatch doneSignal) {
    this.queryNum = queryNum;
    this.conn = conn;
    this.doneSignal = doneSignal;
  }

  public void run() {
    try {
      selectData();
    } catch (Exception e) {
      System.err.printf("[%s][query=%d]: ", Thread.currentThread().getName(), queryNum);
      e.printStackTrace();
    }
  }

  // SELECT data
  public void selectData() throws SQLException {
    executeQuery("SELECT key1, key2 FROM `twoKeyJsn.json` where key2 = 'm'");
  }

  // Execute Query
  public void executeQuery(String query) throws SQLException {
    long numRows = 0;
    try {
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery(query);

      while (rs.next()) {
        numRows++;
      }

      System.out.printf("[%s][queryId=%d]: batches fetched: %d%n", Thread.currentThread().getName(), queryNum, numRows);

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
    @Option(name = "-t")
    int numThreads = 16;
    @Option(name = "-n")
    int numQueries = 100;
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

    ExecutorService executor = Executors.newFixedThreadPool(options.numThreads, new ThreadFactory() {
      int id = 1;
      public Thread newThread(Runnable r) {
        return new Thread(null, r, "THREAD-" + id++);
      }
    });
    CountDownLatch doneSignal = new CountDownLatch(options.numQueries);
    try {
      for (int i = 1; i <= options.numQueries; i++) {
        executor.submit(new ConcurrencyTest(i, conn, doneSignal));
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

    doneSignal.await();
    System.out.println("Shutting down...");
    executor.shutdown();
  }
}
