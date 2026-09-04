/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.qa.single_node;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.sql.qa.rest.RestSqlTestCase;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;

/**
 * Integration test for the rest sql action. The one that speaks json directly to a
 * user rather than to the JDBC driver or CLI.
 */
public class RestSqlIT extends RestSqlTestCase {
    @ClassRule
    public static final ElasticsearchCluster cluster = SqlTestCluster.getCluster();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testErrorMessageForTranslatingQueryWithWhereEvaluatingToFalse() throws IOException {
        index("{\"foo\":1}");
        expectBadRequest(
            () -> runTranslateSql(query("SELECT * FROM test WHERE foo = 1 AND foo = 2").toString()),
            containsString(
                "Cannot generate a query DSL for an SQL query that either its WHERE clause evaluates "
                    + "to FALSE or doesn't operate on a table (missing a FROM clause), sql statement: "
                    + "[SELECT * FROM test WHERE foo = 1 AND foo = 2]"
            )
        );
    }

    public void testErrorMessageForTranslatingQueryWithLocalExecution() throws IOException {
        index("{\"foo\":1}");
        expectBadRequest(
            () -> runTranslateSql(query("SELECT SIN(PI())").toString()),
            containsString(
                "Cannot generate a query DSL for an SQL query that either its WHERE clause evaluates "
                    + "to FALSE or doesn't operate on a table (missing a FROM clause), sql statement: [SELECT SIN(PI())]"
            )
        );
    }

    public void testErrorMessageForTranslatingSQLCommandStatement() throws IOException {
        index("{\"foo\":1}");
        expectBadRequest(
            () -> runTranslateSql(query("SHOW FUNCTIONS").toString()),
            containsString(
                "Cannot generate a query DSL for a special SQL command " + "(e.g.: DESCRIBE, SHOW), sql statement: [SHOW FUNCTIONS]"
            )
        );
    }

    public void testErrorMessageForInvalidParamDataType() throws IOException {
        // proto.Mode not available
        expectBadRequest(
            () -> runTranslateSql(
                query("SELECT null WHERE 0 = ?").mode("odbc").params("[{\"type\":\"invalid\", \"value\":\"irrelevant\"}]").toString()
            ),
            containsString("Invalid parameter data type [invalid]")
        );
    }

    public void testErrorMessageForInvalidParamSpec() throws IOException {
        expectBadRequest(
            () -> runTranslateSql(
                query("SELECT null WHERE 0 = ?").mode("odbc").params("[{\"type\":\"SHAPE\", \"value\":false}]").toString()
            ),
            containsString("Cannot cast value [false] of type [BOOLEAN] to parameter type [SHAPE]")
        );

    }

    public void testIncorrectAcceptHeader() throws IOException {
        index("{\"foo\":1}");
        Request request = new Request("POST", SQL_QUERY_REST_ENDPOINT);
        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.addHeader("Accept", "application/fff");
        request.setOptions(options);
        StringEntity stringEntity = new StringEntity(query("select * from test").toString(), ContentType.APPLICATION_JSON);
        request.setEntity(stringEntity);
        expectBadRequest(
            () -> toMap(client().performRequest(request), "plain"),
            containsString("Invalid request content type: Accept=[application/fff]")
        );
    }

    /**
     * Verifies the fix for https://github.com/elastic/elasticsearch/issues/137365
     * DATE_ADD with a field reference in a range bound should work via script query.
     */
    @SuppressWarnings("unchecked")
    public void testDateAddWithFieldInRangeBound() throws IOException {
        // Create index with date fields
        Request createIndex = new Request("PUT", "/test_date_add");
        createIndex.setJsonEntity("""
            {
              "mappings": {
                "properties": {
                  "event_date": { "type": "date", "format": "yyyy-MM-dd" },
                  "base_date": { "type": "date", "format": "yyyy-MM-dd" },
                  "name": { "type": "keyword" }
                }
              }
            }""");
        provisioningClient().performRequest(createIndex);

        // Index test documents
        Request bulk = new Request("POST", "/test_date_add/_bulk");
        bulk.addParameter("refresh", "true");
        bulk.setJsonEntity("""
            {"index":{}}
            {"event_date":"2024-01-15","base_date":"2024-01-05","name":"within_range"}
            {"index":{}}
            {"event_date":"2024-03-05","base_date":"2024-01-05","name":"outside_range"}
            {"index":{}}
            {"event_date":"2024-02-01","base_date":"2024-02-01","name":"same_date"}
            """);
        provisioningClient().performRequest(bulk);

        // Query: find documents where event_date is within 30 days after base_date
        // This uses DATE_ADD with a field reference, which requires script query fallback
        String mode = randomMode();
        String sql = "SELECT name FROM test_date_add "
            + "WHERE event_date BETWEEN base_date AND DATE_ADD('days', 30, base_date) ORDER BY name";
        Map<String, Object> result = runSql(new StringEntity(query(sql).mode(mode).toString(), ContentType.APPLICATION_JSON), "", mode);

        List<List<Object>> rows = (List<List<Object>>) result.get("rows");
        assertThat(rows, hasSize(2));
        assertEquals("same_date", rows.get(0).get(0));
        assertEquals("within_range", rows.get(1).get(0));

        // Cleanup
        provisioningClient().performRequest(new Request("DELETE", "/test_date_add"));
    }
}
