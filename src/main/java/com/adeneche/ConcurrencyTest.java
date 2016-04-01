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

  final int id;
  final String query;
  final Connection conn;
  final CountDownLatch doneSignal;
  final int rowsBeforeCancel;

  ConcurrencyTest(int id, final String query, final Connection conn, final CountDownLatch doneSignal, final int rowsBeforeCancel) {
    this.id = id;
    this.query = query;
    this.conn = conn;
    this.doneSignal = doneSignal;
    this.rowsBeforeCancel = rowsBeforeCancel;
  }

  public void run() {
    try {
      executeQuery(query);
    } catch (Exception e) {
      System.err.printf("[query=%d]: ", id);
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
        if (numRows == rowsBeforeCancel) {
          stmt.cancel();
          break;
        }
      }

      rs.close();
      stmt.close();
      conn.close(); // TODO move connection closing after all threads are done
    } finally {
      doneSignal.countDown();
    }
  }

  private static class Options {
    @Option(name = "-n")
    int numQueries = 10;
    @Option(name = "-u", required = true)
    String url;
    @Option(name = "-q", required = true)
    String query;
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

    Class.forName("org.apache.drill.jdbc.Driver").newInstance();

    ExecutorService executor = Executors.newFixedThreadPool(options.numQueries);
    CountDownLatch doneSignal = new CountDownLatch(options.numQueries);
    try {
      for (int i = 0; i < options.numQueries; i++) {
        final Connection conn = DriverManager.getConnection(options.url, "", "");
        final int rowsBeforeCancel = (i == options.numQueries-1) ? 10000:-1;
        executor.submit(new ConcurrencyTest(i, options.query, conn, doneSignal, rowsBeforeCancel));
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
