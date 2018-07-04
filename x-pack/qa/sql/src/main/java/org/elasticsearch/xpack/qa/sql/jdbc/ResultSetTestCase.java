/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.qa.sql.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;

public class ResultSetTestCase extends JdbcIntegrationTestCase {
    public void testGettingTimestamp() throws Exception {
        long randomMillis = randomLongBetween(0, System.currentTimeMillis());

        index("library", "1", builder -> {
            builder.field("name", "Don Quixote");
            builder.field("page_count", 1072);
            builder.timeField("release_date", new Date(randomMillis));
            builder.timeField("republish_date", null);
        });
        index("library", "2", builder -> {
            builder.field("name", "1984");
            builder.field("page_count", 328);
            builder.timeField("release_date", new Date(-649036800000L));
            builder.timeField("republish_date", new Date(599616000000L));
        });

        try (Connection connection = esJdbc()) {
            try (PreparedStatement statement = connection.prepareStatement("SELECT name, release_date, republish_date FROM library")) {
                try (ResultSet results = statement.executeQuery()) {
                    ResultSetMetaData resultSetMetaData = results.getMetaData();

                    results.next();
                    assertEquals(3, resultSetMetaData.getColumnCount());
                    assertEquals(randomMillis, results.getTimestamp("release_date").getTime());
                    assertEquals(randomMillis, results.getTimestamp(2).getTime());
                    assertTrue(results.getObject(2) instanceof Timestamp);
                    assertEquals(randomMillis, ((Timestamp) results.getObject("release_date")).getTime());
                    
                    assertNull(results.getTimestamp(3));
                    assertNull(results.getObject("republish_date"));

                    assertTrue(results.next());
                    assertEquals(599616000000L, results.getTimestamp("republish_date").getTime());
                    assertEquals(-649036800000L, ((Timestamp) results.getObject(2)).getTime());

                    assertFalse(results.next());
                }
            }
        }
    }

    /*
     * Checks StackOverflowError fix for https://github.com/elastic/elasticsearch/pull/31735
     */
    public void testNoInfiniteRecursiveGetObjectCalls() throws SQLException, IOException {
        index("library", "1", builder -> {
            builder.field("name", "Don Quixote");
            builder.field("page_count", 1072);
        });
        Connection conn = esJdbc();
        PreparedStatement statement = conn.prepareStatement("SELECT * FROM library");
        ResultSet results = statement.executeQuery();

        try {
            results.next();
            results.getObject("name");
            results.getObject("page_count");
            results.getObject(1);
            results.getObject(1, String.class);
            results.getObject("page_count", Integer.class);
        } catch (StackOverflowError soe) {
            fail("Infinite recursive call on getObject() method");
        }
    }
}
