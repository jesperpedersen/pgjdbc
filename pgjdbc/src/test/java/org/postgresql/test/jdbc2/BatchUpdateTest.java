/*
 * Copyright (c) 2018, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.test.jdbc2;

import org.postgresql.test.TestUtil;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.BatchUpdateException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

/*
 * 
 */
public class BatchUpdateTest extends BaseTest4 {

  @Override
  public void setUp() throws Exception {
    super.setUp();

    // Drop the test table if it already exists for some reason. It is
    // not an error if it doesn't exist.
    TestUtil.createTable(con, "testbatch", "pk INTEGER PRIMARY KEY, col INTEGER");

    // Generally recommended with batch updates. By default we run all
    // tests in this test case with autoCommit disabled.
    con.setAutoCommit(false);
  }

  // Tear down the fixture for this test case.
  @Override
  public void tearDown() throws SQLException {
    con.setAutoCommit(true);

    TestUtil.dropTable(con, "testbatch");
    super.tearDown();
  }

  @Test
  public void testStatementSelect() throws Exception {
    Statement stmt = con.createStatement();
    stmt.addBatch("SELECT 1");
    try {
       int[] updates = stmt.executeBatch();
       Assert.fail("Statement doesn't support SELECT");
    } catch (BatchUpdateException bue) {
       // Ok
    } finally {
       stmt.close();
    }
  }

  @Test
  public void testPreparedStatementSelect() throws Exception {
    PreparedStatement ps = con.prepareStatement("SELECT ?");
    ps.setInt(1,1);
    ps.addBatch();
    try {
       int[] updates = ps.executeBatch();
       Assert.fail("PreparedStatement doesn't support SELECT");
    } catch (BatchUpdateException bue) {
       // Ok
    } finally {
       ps.close();
    }
  }

  @Test
  public void testInvalidInsert() throws Exception {
    int[] expected = {1,Statement.EXECUTE_FAILED,1};
    int[] updates = {0,0,0};
    PreparedStatement ps = con.prepareStatement("INSERT INTO testbatch VALUES (?,?)");
    // Valid
    ps.setInt(1, 1);
    ps.setInt(2, 1);
    ps.addBatch();
    // Invalid
    ps.setInt(1, 1);
    ps.setInt(2, 1);
    ps.addBatch();
    // Valid
    ps.setInt(1, 2);
    ps.setInt(2, 1);
    ps.addBatch();

    try {
       updates = ps.executeBatch();
       Assert.fail("PK violation");
    } catch (BatchUpdateException bue) {
       // Ok
       updates = bue.getUpdateCounts();
       Assert.assertArrayEquals(expected, updates);
    } finally {
       ps.close();
       con.rollback();

       Statement stmt = con.createStatement();
       stmt.execute("DELETE FROM testbatch");
       stmt.close();
    }
  }

  @Test
  public void testSequenceSelect() throws Exception {
    int[] expected = {1,1,1,Statement.EXECUTE_FAILED};
    int[] updates = {0,0,0,0};
    Statement stmt = con.createStatement();
    stmt.addBatch("INSERT INTO testbatch VALUES (1,1)");
    stmt.addBatch("UPDATE testbatch SET col = 2 WHERE pk = 1;");
    stmt.addBatch("DELETE FROM testbatch WHERE pk = 1");
    stmt.addBatch("SELECT pk, col FROM testbatch");

    try {
       updates = stmt.executeBatch();
       Assert.fail("Statement doesn't support SELECT");
    } catch (BatchUpdateException bue) {
       // Ok
       updates = bue.getUpdateCounts();
       Assert.assertArrayEquals(expected, updates);
    } finally {
       stmt.close();
       con.rollback();

       stmt = con.createStatement();
       stmt.execute("DELETE FROM testbatch");
       stmt.close();
    }
  }
}
