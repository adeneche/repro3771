package com.adeneche;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Reproduction class for DRILL-3763:
 * - submit N queries in parallel
 * - shouldCancel one query after X seconds
 *
 */
public class ConcurrencyTest {

  private static class Options {
    @Option(name = "-u", required = true)
    String url;
    @Option(name = "-q", required = true)
    String query;
  }

  // Execute Query
  private static void executeQuery(final Connection conn, final String query, int rowsBeforeCancel) throws SQLException {
    long numRows = 0;

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

    final Connection conn = DriverManager.getConnection(options.url, "", "");
    executeQuery(conn, options.query, 10000);
  }
}
