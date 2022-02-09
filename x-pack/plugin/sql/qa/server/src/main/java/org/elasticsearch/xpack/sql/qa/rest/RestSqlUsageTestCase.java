/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.qa.rest;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.StringUtils;
import org.elasticsearch.xpack.sql.qa.FeatureMetric;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.sql.proto.CoreProtocol.SQL_QUERY_REST_ENDPOINT;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.SQL_STATS_REST_ENDPOINT;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.SQL_TRANSLATE_REST_ENDPOINT;
import static org.elasticsearch.xpack.sql.qa.rest.BaseRestSqlTestCase.toMap;
import static org.elasticsearch.xpack.sql.qa.rest.RestSqlTestCase.query;

public abstract class RestSqlUsageTestCase extends ESRestTestCase {
    private List<IndexDocument> testData = Arrays.asList(
        new IndexDocument("used", "Don Quixote", 1072),
        new IndexDocument("used", "Vacuum Diagrams", 335),
        new IndexDocument("new", "Darwin's Radio", 270),
        new IndexDocument("used", "The Martian", 387),
        new IndexDocument("new", "Moving Mars", 495)
    );

    private enum ClientType {
        CANVAS,
        CLI,
        JDBC,
        ODBC,
        ODBC32,
        ODBC64,
        REST;

        @Override
        public String toString() {
            return this.name().toLowerCase(Locale.ROOT);
        }

    }

    private Map<String, Integer> baseMetrics = new HashMap<String, Integer>();
    private Integer baseClientTypeTotalQueries = 0;
    private Integer baseClientTypePagingQueries = 0;
    private Integer baseClientTypeFailedQueries = 0;
    private Integer baseAllTotalQueries = 0;
    private Integer baseAllPagingQueries = 0;
    private Integer baseAllFailedQueries = 0;
    private Integer baseTranslateRequests = 0;
    private String clientType;
    private String mode;
    private boolean ignoreClientType;

    /**
     * This method gets the metrics' values before the test runs, in case these values
     * were changed by other tests running in the same REST test cluster. The test itself
     * will count the new metrics' values starting from the base values initialized here.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Before
    private void getBaseMetrics() throws UnsupportedOperationException, IOException {
        Map<String, Object> baseStats = getStats();
        List<Map<String, Map<String, Map>>> nodesListStats = (List) baseStats.get("stats");

        // used for "client_id" request parameter value, but also for getting the stats from ES
        clientType = randomFrom(ClientType.values()).toString();
        ignoreClientType = randomBoolean();

        // "client_id" parameter will not be sent in the requests
        // and "clientType" will only be used for getting the stats back from ES
        if (ignoreClientType) {
            clientType = ClientType.REST.toString();
        }

        if (clientType.equals(ClientType.JDBC.toString())) {
            mode = Mode.JDBC.toString();
        } else if (clientType.startsWith(ClientType.ODBC.toString())) {
            mode = Mode.ODBC.toString();
        } else if (clientType.equals(ClientType.CLI.toString())) {
            mode = Mode.CLI.toString();
        } else {
            mode = Mode.PLAIN.toString();
        }

        for (Map perNodeStats : nodesListStats) {
            Map featuresMetrics = (Map) ((Map) perNodeStats.get("stats")).get("features");
            Map queriesMetrics = (Map) ((Map) perNodeStats.get("stats")).get("queries");
            for (FeatureMetric metric : FeatureMetric.values()) {
                baseMetrics.put(metric.toString(), (Integer) featuresMetrics.get(metric.toString()));
            }

            // initialize the "base" metric values with whatever values are already recorded on ES
            baseClientTypeTotalQueries = ((Map<String, Integer>) queriesMetrics.get(clientType)).get("total");
            baseClientTypePagingQueries = ((Map<String, Integer>) queriesMetrics.get(clientType)).get("paging");
            baseClientTypeFailedQueries = ((Map<String, Integer>) queriesMetrics.get(clientType)).get("failed");
            baseAllTotalQueries = ((Map<String, Integer>) queriesMetrics.get("_all")).get("total");
            baseAllPagingQueries = ((Map<String, Integer>) queriesMetrics.get("_all")).get("paging");
            baseAllFailedQueries = ((Map<String, Integer>) queriesMetrics.get("_all")).get("failed");
            baseTranslateRequests = ((Map<String, Integer>) queriesMetrics.get("translate")).get("count");
        }
    }

    public void testSqlRestUsage() throws IOException {
        index(testData);

        //
        // random WHERE and ORDER BY queries
        //
        int randomWhereExecutions = randomIntBetween(1, 15);
        int clientTypeTotalQueries = baseClientTypeTotalQueries + randomWhereExecutions;
        int allTotalQueries = baseAllTotalQueries + randomWhereExecutions;

        for (int i = 0; i < randomWhereExecutions; i++) {
            runSql("SELECT name FROM library WHERE page_count > 100 ORDER BY page_count");
        }

        Map<String, Object> responseAsMap = getStats();
        assertFeatureMetric(baseMetrics.get("where") + randomWhereExecutions, responseAsMap, "where");
        assertFeatureMetric(baseMetrics.get("orderby") + randomWhereExecutions, responseAsMap, "orderby");
        assertClientTypeAndAllQueryMetrics(clientTypeTotalQueries, allTotalQueries, responseAsMap);

        //
        // random HAVING and GROUP BY queries
        //
        int randomHavingExecutions = randomIntBetween(1, 15);
        clientTypeTotalQueries += randomHavingExecutions;
        allTotalQueries += randomHavingExecutions;
        for (int i = 0; i < randomHavingExecutions; i++) {
            runSql("SELECT condition FROM library GROUP BY condition HAVING MAX(page_count) > 1000");
        }
        responseAsMap = getStats();
        assertFeatureMetric(baseMetrics.get("having") + randomHavingExecutions, responseAsMap, "having");
        assertFeatureMetric(baseMetrics.get("groupby") + randomHavingExecutions, responseAsMap, "groupby");
        assertClientTypeAndAllQueryMetrics(clientTypeTotalQueries, allTotalQueries, responseAsMap);

        //
        // random LIMIT queries
        //
        int randomLimitExecutions = randomIntBetween(1, 15);
        clientTypeTotalQueries += randomLimitExecutions;
        allTotalQueries += randomLimitExecutions;
        for (int i = 0; i < randomLimitExecutions; i++) {
            runSql("SELECT * FROM library LIMIT " + testData.size());
        }
        responseAsMap = getStats();
        assertFeatureMetric(baseMetrics.get("limit") + randomLimitExecutions, responseAsMap, "limit");
        assertClientTypeAndAllQueryMetrics(clientTypeTotalQueries, allTotalQueries, responseAsMap);

        //
        // random LOCALly executed queries
        //
        int randomLocalExecutions = randomIntBetween(1, 15);
        clientTypeTotalQueries += randomLocalExecutions;
        allTotalQueries += randomLocalExecutions;
        for (int i = 0; i < randomLocalExecutions; i++) {
            runSql("SELECT 1+2");
        }
        responseAsMap = getStats();
        assertFeatureMetric(baseMetrics.get("local") + randomLocalExecutions, responseAsMap, "local");
        assertClientTypeAndAllQueryMetrics(clientTypeTotalQueries, allTotalQueries, responseAsMap);

        //
        // random COMMANDs
        //
        int randomCommandExecutions = randomIntBetween(1, 15);
        clientTypeTotalQueries += randomCommandExecutions;
        allTotalQueries += randomCommandExecutions;
        for (int i = 0; i < randomCommandExecutions; i++) {
            runSql(
                randomFrom(
                    "SHOW FUNCTIONS",
                    "SHOW COLUMNS FROM library",
                    "SHOW SCHEMAS",
                    "SHOW TABLES",
                    "SYS TABLES",
                    "SYS COLUMNS LIKE '%name'",
                    "SYS TABLES TYPE '%'",
                    "SYS TYPES"
                )
            );
        }
        responseAsMap = getStats();
        assertFeatureMetric(baseMetrics.get("command") + randomCommandExecutions, responseAsMap, "command");
        assertClientTypeAndAllQueryMetrics(clientTypeTotalQueries, allTotalQueries, responseAsMap);

        //
        // random TRANSLATE requests
        //
        int randomTranslateExecutions = randomIntBetween(1, 15);
        for (int i = 0; i < randomTranslateExecutions; i++) {
            runTranslate("SELECT name FROM library WHERE page_count > 100 ORDER BY page_count");
        }
        responseAsMap = getStats();
        assertTranslateQueryMetric(baseTranslateRequests + randomTranslateExecutions, responseAsMap);

        //
        // random failed queries
        //
        int randomFailedExecutions = randomIntBetween(1, 15);
        int clientTypeFailedQueries = baseClientTypeFailedQueries + randomFailedExecutions;
        int allFailedQueries = baseAllFailedQueries + randomFailedExecutions;
        allTotalQueries += randomFailedExecutions;
        clientTypeTotalQueries += randomFailedExecutions;
        for (int i = 0; i < randomFailedExecutions; i++) {
            // not interested in the exception type, but in the fact that the metrics are incremented
            // when an exception is thrown
            expectThrows(Exception.class, () -> {
                runSql(
                    randomFrom(
                        "SELECT missing_field FROM library",
                        "SELECT * FROM missing_index",
                        "SELECTT wrong_command",
                        "SELECT name + page_count AS not_allowed FROM library",
                        "SELECT incomplete_query FROM"
                    )
                );
            });
        }
        responseAsMap = getStats();
        assertClientTypeAndAllFailedQueryMetrics(clientTypeFailedQueries, allFailedQueries, responseAsMap);
        assertClientTypeAndAllQueryMetrics(clientTypeTotalQueries, allTotalQueries, responseAsMap);
    }

    public void testScrollUsage() throws IOException {
        index(testData);

        String cursor = runSql("SELECT page_count, name FROM library ORDER BY page_count", randomIntBetween(1, testData.size()));
        int scrollRequests = 0;

        while (cursor != null) {
            cursor = scroll(cursor);
            scrollRequests++;
        }

        Map<String, Object> responseAsMap = getStats();
        assertClientTypeQueryMetric(baseClientTypeTotalQueries + scrollRequests + 1, responseAsMap, "total");
        assertClientTypeQueryMetric(baseClientTypePagingQueries + scrollRequests, responseAsMap, "paging");

        assertAllQueryMetric(baseAllTotalQueries + scrollRequests + 1, responseAsMap, "total");
        assertAllQueryMetric(baseAllPagingQueries + scrollRequests, responseAsMap, "paging");

        assertFeatureMetric(baseMetrics.get("orderby") + 1, responseAsMap, "orderby");
    }

    // test for bug https://github.com/elastic/elasticsearch/issues/81502
    public void testUsageOfQuerySortedByAggregationResult() throws IOException {
        index(testData);

        String cursor = runSql("SELECT SUM(page_count), name FROM library GROUP BY 2 ORDER BY 1", 1);

        Map<String, Object> responseAsMap = getStats();
        assertClientTypeQueryMetric(baseClientTypeTotalQueries + 1, responseAsMap, "total");
        assertClientTypeQueryMetric(baseClientTypePagingQueries, responseAsMap, "paging");
        assertAllQueryMetric(baseAllTotalQueries + 1, responseAsMap, "total");
        assertAllQueryMetric(baseAllPagingQueries, responseAsMap, "paging");

        scroll(cursor);
        responseAsMap = getStats();
        assertClientTypeQueryMetric(baseClientTypeTotalQueries + 2, responseAsMap, "total");
        assertClientTypeQueryMetric(baseClientTypePagingQueries + 1, responseAsMap, "paging");
        assertAllQueryMetric(baseAllTotalQueries + 2, responseAsMap, "total");
        assertAllQueryMetric(baseAllPagingQueries + 1, responseAsMap, "paging");
    }

    private void assertClientTypeAndAllQueryMetrics(int clientTypeTotalQueries, int allTotalQueries, Map<String, Object> responseAsMap)
        throws IOException {
        assertClientTypeQueryMetric(clientTypeTotalQueries, responseAsMap, "total");
        assertAllQueryMetric(allTotalQueries, responseAsMap, "total");
    }

    private void assertClientTypeAndAllFailedQueryMetrics(
        int clientTypeFailedQueries,
        int allFailedQueries,
        Map<String, Object> responseAsMap
    ) throws IOException {
        assertClientTypeQueryMetric(clientTypeFailedQueries, responseAsMap, "failed");
        assertAllQueryMetric(allFailedQueries, responseAsMap, "failed");
    }

    private void index(List<IndexDocument> docs) throws IOException {
        Request request = new Request("POST", "/library/_bulk");
        request.addParameter("refresh", "true");
        StringBuilder bulk = new StringBuilder();
        for (IndexDocument doc : docs) {
            bulk.append("""
                {"index":{}}
                {"condition":"%s","name":"%s","page_count":%s}
                """.formatted(doc.condition, doc.name, doc.pageCount));
        }
        request.setJsonEntity(bulk.toString());
        client().performRequest(request);
    }

    private Map<String, Object> getStats() throws UnsupportedOperationException, IOException {
        Request request = new Request("GET", SQL_STATS_REST_ENDPOINT);
        Map<String, Object> responseAsMap;
        try (InputStream content = client().performRequest(request).getEntity().getContent()) {
            responseAsMap = XContentHelper.convertToMap(JsonXContent.jsonXContent, content, false);
        }

        return responseAsMap;
    }

    private void runTranslate(String sql) throws IOException {
        Request request = new Request("POST", SQL_TRANSLATE_REST_ENDPOINT);
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
        request.setEntity(new StringEntity(query(sql).toString(), ContentType.APPLICATION_JSON));
        client().performRequest(request);
    }

    private String runSql(String sql) throws IOException {
        return runSql(sql, null);
    }

    private String scroll(String cursor) throws IOException {
        Request request = new Request("POST", SQL_QUERY_REST_ENDPOINT);
        request.setEntity(
            new StringEntity(RestSqlTestCase.cursor(cursor).mode(mode).clientId(clientType).toString(), ContentType.APPLICATION_JSON)
        );
        return (String) toMap(client().performRequest(request), mode).get("cursor");
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

    private String runSql(String sql, Integer fetchSize) throws IOException {
        Request request = new Request("POST", SQL_QUERY_REST_ENDPOINT);
        request.addParameter("error_trace", "true");   // Helps with debugging in case something crazy happens on the server.
        request.addParameter("pretty", "true");        // Improves error reporting readability
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
        request.setEntity(
            new StringEntity(
                query(sql).fetchSize(fetchSize).mode(mode).clientId(ignoreClientType ? StringUtils.EMPTY : clientType).toString(),
                ContentType.APPLICATION_JSON
            )
        );
        return (String) toMap(client().performRequest(request), mode).get("cursor");
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

    private void assertClientTypeQueryMetric(int expected, Map<String, Object> responseAsMap, String metric) throws IOException {
        assertQueryMetric(expected, responseAsMap, clientType, metric);
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
