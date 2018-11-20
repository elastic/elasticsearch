/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.qa.rest;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.RestClient;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public abstract class RestSqlUsageTestCase extends ESRestTestCase {
    private List<IndexDocument> testData = Arrays.asList(
            new IndexDocument("used", "Don Quixote",     1072),
            new IndexDocument("used", "Vacuum Diagrams", 335),
            new IndexDocument("new",  "Darwin's Radio",  270),
            new IndexDocument("used", "The Martian",     387),
            new IndexDocument("new",  "Moving Mars",     495)
    );
    
    // when this test runs in the entire suite of tests, it gets "interferences"
    // from other tests which, by running queries against the rest test cluster
    // will start recording metrics for those as well, not only for the ones in this test
    @AwaitsFix(bugUrl="needs test infra that will isolate this test from the rest of the suite or cleanup between tests")
    public void testSqlRestUsage() throws IOException {
        index(testData);
        int randomWhereExecutions = randomIntBetween(1, 15);
        int cliTotalQueries = randomWhereExecutions;
        int allTotalQueries = randomWhereExecutions;
        
        for (int i = 0; i < randomWhereExecutions; i++) {
            runCliSql("SELECT name FROM library WHERE page_count > 100 ORDER BY page_count");
        }
        
        Map<String, Object> responseAsMap = getStats();
        assertFeatureMetric(randomWhereExecutions, responseAsMap, "where");
        assertFeatureMetric(randomWhereExecutions, responseAsMap, "orderby");
        assertCliQueryMetric(cliTotalQueries, responseAsMap, "total");
        assertAllQueryMetric(allTotalQueries, responseAsMap, "total");
        
        int randomHavingExecutions = randomIntBetween(1, 15);
        cliTotalQueries += randomHavingExecutions;
        allTotalQueries += randomHavingExecutions;
        for (int i = 0; i < randomHavingExecutions; i++) {
            runCliSql("SELECT condition FROM library GROUP BY condition HAVING MAX(page_count) > 1000");
        }
        responseAsMap = getStats();
        assertFeatureMetric(randomHavingExecutions, responseAsMap, "having");
        assertFeatureMetric(randomHavingExecutions, responseAsMap, "groupby");
        assertCliQueryMetric(cliTotalQueries, responseAsMap, "total");
        assertAllQueryMetric(allTotalQueries, responseAsMap, "total");
        
        int randomSubselectExecutions = randomIntBetween(1, 15);
        cliTotalQueries += randomSubselectExecutions;
        allTotalQueries += randomSubselectExecutions;
        for (int i = 0; i < randomSubselectExecutions; i++) {
            runCliSql("SELECT * FROM (SELECT name FROM library)");
        }
        responseAsMap = getStats();
        assertFeatureMetric(randomSubselectExecutions, responseAsMap, "subselect");
        assertCliQueryMetric(cliTotalQueries, responseAsMap, "total");
        assertAllQueryMetric(allTotalQueries, responseAsMap, "total");
        
        int randomLimitExecutions = randomIntBetween(1, 15);
        cliTotalQueries += randomLimitExecutions;
        allTotalQueries += randomLimitExecutions;
        for (int i = 0; i < randomLimitExecutions; i++) {
            runCliSql("SELECT * FROM library LIMIT " + testData.size());
        }
        responseAsMap = getStats();
        assertFeatureMetric(randomLimitExecutions, responseAsMap, "limit");
        assertCliQueryMetric(cliTotalQueries, responseAsMap, "total");
        assertAllQueryMetric(allTotalQueries, responseAsMap, "total");
        
        int randomLocalExecutions = randomIntBetween(1, 15);
        cliTotalQueries += randomLocalExecutions;
        allTotalQueries += randomLocalExecutions;
        for (int i = 0; i < randomLocalExecutions; i++) {
            runCliSql("SELECT 1+2");
        }
        responseAsMap = getStats();
        assertFeatureMetric(randomLocalExecutions, responseAsMap, "local");
        assertCliQueryMetric(cliTotalQueries, responseAsMap, "total");
        assertAllQueryMetric(allTotalQueries, responseAsMap, "total");
    }
    
    public void testSqlRestTranslateUsage() throws IOException {
        index(testData);
        int randomTranslateExecutions = randomIntBetween(1, 15);
        
        for (int i = 0; i < randomTranslateExecutions; i++) {
            runTranslate("SELECT name FROM library WHERE page_count > 100 ORDER BY page_count");
        }
        
        Map<String, Object> responseAsMap = getStats();
        assertTranslateQueryMetric(randomTranslateExecutions, responseAsMap);
    }
    
    private void index(List<IndexDocument> docs) throws IOException {
        Request request = new Request("POST", "/library/_bulk");
        request.addParameter("refresh", "true");
        StringBuilder bulk = new StringBuilder();
        for (IndexDocument doc : docs) {
            bulk.append("{\"index\":{}}\n");
            bulk.append("{\"condition\":\"" + doc.condition + "\",\"name\":\"" + doc.name + "\",\"page_count\":" + doc.pageCount + "}\n");
        }
        request.setJsonEntity(bulk.toString());
        client().performRequest(request);
    }
    
    private Map<String, Object> getStats() throws UnsupportedOperationException, IOException {
        Request request = new Request("GET", "/_xpack/sql/stats");
        Map<String, Object> responseAsMap;
        try (InputStream content = client().performRequest(request).getEntity().getContent()) {
            responseAsMap = XContentHelper.convertToMap(JsonXContent.jsonXContent, content, false);
        }
        
        return responseAsMap;
    }
    
    private void runTranslate(String sql) throws IOException {
        Request request = new Request("POST", "/_xpack/sql/translate");
        if (randomBoolean()) {
            // We default to JSON but we force it randomly for extra coverage
            request.addParameter("format", "json");
        }
        if (randomBoolean()) {
            // JSON is the default but randomly set it sometime for extra coverage
            RequestOptions.Builder options = request.getOptions().toBuilder();
            options.addHeader("Accept", randomFrom("*/*", "application/json"));
            request.setOptions(options);
        }
        request.setEntity(new StringEntity("{\"query\":\"" + sql + "\"}", ContentType.APPLICATION_JSON));
        client().performRequest(request);
    }
    
    private void runCliSql(String sql) throws IOException {
        runSql(Mode.PLAIN.toString(), RestClient.CLI.toString(), sql);
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void assertTranslateQueryMetric(int expected, Map<String, Object> responseAsMap) throws IOException {
        List<Map<String, Map<String, Map>>> nodesListStats = (List) responseAsMap.get("stats");
        int actualMetricValue = 0;
        for (Map perNodeStats : nodesListStats) {
            Map queriesMetrics = (Map) ((Map) perNodeStats.get("stats")).get("queries");
            Map perTypeQueriesMetrics = (Map) queriesMetrics.get("translate");
            actualMetricValue += (int) perTypeQueriesMetrics.get("count");
        }
        assertEquals(expected, actualMetricValue);
    }
    
    private void runSql(String mode, String restClient, String sql) throws IOException {
        Request request = new Request("POST", "/_xpack/sql");
        request.addParameter("error_trace", "true");   // Helps with debugging in case something crazy happens on the server.
        request.addParameter("pretty", "true");        // Improves error reporting readability
        if (randomBoolean()) {
            // We default to JSON but we force it randomly for extra coverage
            request.addParameter("format", "json");
        }
        if (false == mode.isEmpty()) {
            request.addParameter("mode", mode);        // JDBC or PLAIN mode
        }
        if (false == restClient.isEmpty()) {
            request.addParameter("client", restClient);        // CLI, CANVAS, random string
        }
        if (randomBoolean()) {
            // JSON is the default but randomly set it sometime for extra coverage
            RequestOptions.Builder options = request.getOptions().toBuilder();
            options.addHeader("Accept", randomFrom("*/*", "application/json"));
            request.setOptions(options);
        }
        request.setEntity(new StringEntity("{\"query\":\"" + sql + "\"}", ContentType.APPLICATION_JSON));
        client().performRequest(request);
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void assertFeatureMetric(int expected, Map<String, Object> responseAsMap, String feature) throws IOException {
        List<Map<String, ?>> nodesListStats = (List<Map<String, ?>>) responseAsMap.get("stats");
        int actualMetricValue = 0;
        for (Map perNodeStats : nodesListStats) {
            Map featuresMetrics = (Map) ((Map) perNodeStats.get("stats")).get("features");
            actualMetricValue += (int) featuresMetrics.get(feature);
        }
        assertEquals(expected, actualMetricValue);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
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
    
    private void assertCliQueryMetric(int expected, Map<String, Object> responseAsMap, String metric) throws IOException {
        assertQueryMetric(expected, responseAsMap, "cli", metric);
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
