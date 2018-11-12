/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.qa.multi_node;

import org.elasticsearch.client.Request;
import org.elasticsearch.xpack.sql.qa.jdbc.JdbcMultiNodeIntegrationTestCase;
import org.junit.Before;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SqlJdbcStatsIT extends JdbcMultiNodeIntegrationTestCase {
    
    private List<IndexDocument> testData = Arrays.asList(
            new IndexDocument("used", "Don Quixote",     1072),
            new IndexDocument("used", "Vacuum Diagrams", 335),
            new IndexDocument("new",  "Darwin's Radio",  270),
            new IndexDocument("used", "The Martian",     387),
            new IndexDocument("new",  "Moving Mars",     495)
    );
    private int randomExecutions;
    private int randomFetchSize;
    
    @Before
    private void runQueries() throws Exception {
        setupTestData();
        randomExecutions = randomIntBetween(1, 5);
        randomFetchSize = randomIntBetween(1, testData.size());
        
        for (int i = 0; i < randomExecutions; i++) {
            try (Connection connection = esJdbc()) {
                try (PreparedStatement statement = connection.prepareStatement("SELECT name FROM library "
                        + "WHERE page_count > 100 ORDER BY page_count")) {
                    statement.setFetchSize(randomFetchSize);
                    try (ResultSet results = statement.executeQuery()) {
                        while (results.next()) {};
                    }
                }
                try (PreparedStatement statement = connection.prepareStatement("SELECT condition FROM library "
                        + "GROUP BY condition HAVING MAX(page_count) > 1000")) {
                    try (ResultSet results = statement.executeQuery()) {
                        while (results.next()) {};
                    }
                }
                try (PreparedStatement statement = 
                        connection.prepareStatement("SELECT * FROM (SELECT name FROM library)")) {
                    statement.setFetchSize(randomFetchSize);
                    try (ResultSet results = statement.executeQuery()) {
                        while (results.next()) {};
                    }
                }
                try (PreparedStatement statement = 
                        connection.prepareStatement("SELECT * FROM library LIMIT " + testData.size())) {
                    statement.setFetchSize(randomFetchSize);
                    try (ResultSet results = statement.executeQuery()) {
                        while (results.next()) {};
                    }
                }
                try (PreparedStatement statement = 
                        connection.prepareStatement("SELECT 1+2")) {
                    try (ResultSet results = statement.executeQuery()) {
                        while (results.next()) {};
                    }
                }
            }
        }
    }

    @AwaitsFix(bugUrl="something is wrong with this test")
    public void testQueriesStats() throws Exception {
        Request request = new Request("GET", "/_xpack/sql/stats");
        Map<String, Object> responseAsMap = responseToMap(client().performRequest(request));
        int pagingRequests = randomFetchSize == testData.size() ?
                0 : (int) Math.ceil((double) testData.size() / randomFetchSize) * randomExecutions;
        
        assertFeatureMetric(randomExecutions, responseAsMap, "where");
        assertFeatureMetric(randomExecutions, responseAsMap, "orderby");
        assertFeatureMetric(randomExecutions, responseAsMap, "groupby");
        assertFeatureMetric(randomExecutions, responseAsMap, "having");
        assertFeatureMetric(randomExecutions, responseAsMap, "subselect");
        assertFeatureMetric(randomExecutions, responseAsMap, "limit");
        assertFeatureMetric(randomExecutions, responseAsMap, "local");
        assertJdbcQueryMetric(3 * pagingRequests + 5 * randomExecutions, responseAsMap, "total");
        assertAllQueryMetric(3 * pagingRequests + 5 * randomExecutions, responseAsMap, "total");
        assertJdbcQueryMetric(3 * pagingRequests, responseAsMap, "paging");
        assertAllQueryMetric(3 * pagingRequests, responseAsMap, "paging");
    }

    private void setupTestData() throws IOException {
        for (IndexDocument doc : testData) {
            index("library", String.valueOf(doc.pageCount), builder -> {
                builder.field("condition", doc.condition);
                builder.field("name", doc.name);
                builder.field("page_count", doc.pageCount);
            }); 
        }
    }

    private void assertFeatureMetric(int expected, Map<String, Object> responseAsMap, String feature) throws IOException {
        List<Map<String, Map<String, Map>>> nodesListStats = (List) responseAsMap.get("stats");
        int actualMetricValue = 0;
        for (Map perNodeStats : nodesListStats) {
            Map featuresMetrics = (Map) ((Map) perNodeStats.get("stats")).get("features");
            actualMetricValue += (int) featuresMetrics.get(feature);
        }
        assertEquals(expected, actualMetricValue);
    }

    private void assertQueryMetric(int expected, Map<String, Object> responseAsMap, String queryType, String metric) throws IOException {
        List<Map<String, Map<String, Map>>> nodesListStats = (List) responseAsMap.get("stats");
        int actualMetricValue = 0;
        for (Map perNodeStats : nodesListStats) {
            Map queriesMetrics = (Map) ((Map) perNodeStats.get("stats")).get("queries");
            Map perTypeQueriesMetrics = (Map) queriesMetrics.get(queryType);
            actualMetricValue += (int) perTypeQueriesMetrics.get(metric);
        }
        assertEquals(expected, actualMetricValue);
    }
    
    private void assertJdbcQueryMetric(int expected, Map<String, Object> responseAsMap, String metric) throws IOException {
        assertQueryMetric(expected, responseAsMap, "jdbc", metric);
    }
    
    private void assertAllQueryMetric(int expected, Map<String, Object> responseAsMap, String metric) throws IOException {
        assertQueryMetric(expected, responseAsMap, "_all", metric);
    }
    
    private class IndexDocument {
        private String condition;
        private String name;
        private int pageCount;
        
        IndexDocument(String condition, String name, int pageCount) {
            this.condition = condition;
            this.name = name;
            this.pageCount = pageCount;
        }
    }
}
