/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.qa.jdbc;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.junit.Before;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.elasticsearch.xpack.sql.qa.rest.RestSqlTestCase.assertNoSearchContexts;

/**
 * Tests for setting {@link Statement#setFetchSize(int)} and
 * {@link ResultSet#getFetchSize()}.
 */
public class FetchSizeTestCase extends JdbcIntegrationTestCase {
    @Before
    public void createTestIndex() throws IOException {
        Request request = new Request("PUT", "/test");
        XContentBuilder createIndex = JsonXContent.contentBuilder().startObject();
        createIndex.startObject("mappings");
        {
            createIndex.startObject("properties");
            {
                createIndex.startObject("nested").field("type", "nested");
                createIndex.startObject("properties");
                createIndex.startObject("inner_field").field("type", "integer").endObject();
                createIndex.endObject();
                createIndex.endObject();
            }
            createIndex.endObject();
        }
        createIndex.endObject().endObject();
        request.setJsonEntity(Strings.toString(createIndex));
        client().performRequest(request);
        
        request = new Request("PUT", "/test/_bulk");
        request.addParameter("refresh", "true");
        StringBuilder bulk = new StringBuilder();
        StringBuilder bulkLine;
        for (int i = 0; i < 20; i++) {
            bulk.append("{\"index\":{}}\n");
            bulkLine = new StringBuilder("{\"test_field\":" + i);
            bulkLine.append(", \"nested\":[");
            // each document will have a nested field with 1 - 5 values
            for (int j = 0; j <= i % 5; j++) {
                bulkLine.append("{\"inner_field\":" + j + "}" + ((j == i % 5) ? "" : ","));
            }
            bulkLine.append("]");
            bulk.append(bulkLine).append("}\n");
        }
        request.setJsonEntity(bulk.toString());
        client().performRequest(request);
    }

    /**
     * Test for {@code SELECT} that is implemented as a scroll query.
     * In this case the fetch size should be entirely respected.
     */
    public void testScroll() throws SQLException {
        try (Connection c = esJdbc();
                Statement s = c.createStatement()) {
            s.setFetchSize(4);
            try (ResultSet rs = s.executeQuery("SELECT * FROM test ORDER BY test_field ASC")) {
                for (int i = 0; i < 20; i++) {
                    assertEquals(4, rs.getFetchSize());
                    assertTrue("No more entries left after " + i, rs.next());
                    assertEquals(i, rs.getInt(1));
                }
                assertFalse(rs.next());
            }
        }
    }

    /**
     * Test for {@code SELECT} that is implemented as a scroll query.
     * In this test we don't retrieve all records and rely on close() to clean the cursor
     */
    public void testIncompleteScroll() throws Exception {
        try (Connection c = esJdbc();
             Statement s = c.createStatement()) {
            s.setFetchSize(4);
            try (ResultSet rs = s.executeQuery("SELECT * FROM test ORDER BY test_field ASC")) {
                for (int i = 0; i < 10; i++) {
                    assertEquals(4, rs.getFetchSize());
                    assertTrue("No more entries left after " + i, rs.next());
                    assertEquals(i, rs.getInt(1));
                }
                assertTrue(rs.next());
            }
        }
        assertNoSearchContexts();
    }


    /**
     * Test for {@code SELECT} that is implemented as an aggregation.
     */
    public void testAggregation() throws SQLException {
        try (Connection c = esJdbc();
                Statement s = c.createStatement()) {
            s.setFetchSize(4);
            try (ResultSet rs = s.executeQuery("SELECT test_field, COUNT(*) FROM test GROUP BY test_field")) {
                for (int i = 0; i < 20; i++) {
                    assertEquals(4, rs.getFetchSize());
                    assertTrue("No more entries left at " + i, rs.next());
                    assertEquals(i, rs.getInt(1));
                    assertEquals("Incorrect count returned", 1, rs.getInt(2));
                }
                assertFalse(rs.next());
            }
        }
    }
    
    /**
     * Test for nested documents.
     */
    public void testNestedDocuments() throws Exception {
        try (Connection c = esJdbc();
                Statement s = c.createStatement()) {
            s.setFetchSize(5);
            try (ResultSet rs = s.executeQuery("SELECT test_field, nested.* FROM test ORDER BY test_field ASC")) {
                assertTrue("Empty result set!", rs.next());
                for (int i = 0; i < 20; i++) {
                    assertEquals(15, rs.getFetchSize());
                    assertNestedDocuments(rs, i);
                }
                assertFalse(rs.next());
            }
        }
        assertNoSearchContexts();
    }

    private void assertNestedDocuments(ResultSet rs, int i) throws SQLException {
        for (int j = 0; j <= i % 5; j++) {
            assertEquals(i, rs.getInt(1));
            assertEquals(j, rs.getInt(2));
            // don't check the very last row in the result set
            assertTrue("No more entries left after row " + rs.getRow(), (i+j == 23 || rs.next()));
        }
    }
    
    /**
     * Explicit pagination test for PIVOT.
     * Checks that the paging properly consumes the necessary amount of aggregations and the
     * page size affects the result not the intermediate query.
     */
    public void testPivotPaging() throws Exception {
        addPivotData();
        
        try (Connection c = esJdbc();
             Statement s = c.createStatement()) {
            
            String query = "SELECT * FROM "
                    + "(SELECT item, amount, location FROM test_pivot)"
                    + " PIVOT (AVG(amount) FOR location IN ( 'AF', 'AS', 'EU', 'NA', 'SA', 'AQ', 'AU') )";
            // set size smaller than an agg page
            s.setFetchSize(3);
            try (ResultSet rs = s.executeQuery(query)) {
                assertEquals(8, rs.getMetaData().getColumnCount());
                for (int i = 0; i < 10; i++) {
                    assertTrue(rs.next());
                    // the page was set to a pivot row (since the initial 3 is lower as a pivot page takes number of pivot entries + 1)
                    assertEquals(1, rs.getFetchSize());
                    assertEquals(Long.valueOf(i), rs.getObject("item"));
                }
                assertFalse(rs.next());
            }
            
            // now try with a larger fetch size (8 * 2 + something) - should be 2
            s.setFetchSize(20);
            try (ResultSet rs = s.executeQuery(query)) {
                for (int i = 0; i < 10; i++) {
                    assertTrue(rs.next());
                    //
                    assertEquals(2, rs.getFetchSize());
                    assertEquals(Long.valueOf(i), rs.getObject("item"));
                }
                assertFalse(rs.next());
            }
        }
        assertNoSearchContexts();
    }
    
    
    public void testPivotPagingWithLimit() throws Exception {
        addPivotData();

        try (Connection c = esJdbc();
             Statement s = c.createStatement()) {
            
            // run a query with a limit that is not a multiple of the fetch size
            String query = "SELECT * FROM "
                    + "(SELECT item, amount, location FROM test_pivot)"
                    + " PIVOT (AVG(amount) FOR location IN ( 'EU', 'NA' ) ) LIMIT 5";
            // set size smaller than an agg page
            s.setFetchSize(20);
            try (ResultSet rs = s.executeQuery(query)) {
                assertEquals(3, rs.getMetaData().getColumnCount());
                for (int i = 0; i < 4; i++) {
                    assertTrue(rs.next());
                    assertEquals(2, rs.getFetchSize());
                    assertEquals(Long.valueOf(i), rs.getObject("item"));
                }
                // last entry
                assertTrue(rs.next());
                assertEquals(1, rs.getFetchSize());
                assertFalse("LIMIT should be reached", rs.next());
            }
        }
        assertNoSearchContexts();
    }

    private void addPivotData() throws Exception {
        Request request = new Request("PUT", "/test_pivot/_bulk");
        request.addParameter("refresh", "true");
        StringBuilder bulk = new StringBuilder();
        String[] continent = new String[] { "AF", "AS", "EU", "NA", "SA", "AQ", "AU" };
        for (int i = 0; i <= 100; i++) {
            bulk.append("{\"index\":{}}\n");
            bulk.append("{\"item\":").append(i % 10)
                .append(", \"entry\":").append(i)
                .append(", \"amount\" : ").append(randomInt(999))
                .append(", \"location\" : \"").append(continent[i % (continent.length)]).append("\"")
                .append("}\n");
        }
        request.setJsonEntity(bulk.toString());
        assertEquals(200, client().performRequest(request).getStatusLine().getStatusCode());
    }
}