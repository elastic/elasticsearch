/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.functions.test;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

/**
 * Integration tests for block evaluation in the abs2() function.
 * These tests verify that the runtime-generated evaluator correctly handles:
 * &lt;ul&gt;
 *   &lt;li&gt;Null positions (value count = 0) - output null&lt;/li&gt;
 *   &lt;li&gt;Single-value positions (value count = 1) - process value normally&lt;/li&gt;
 *   &lt;li&gt;Mixed data with nulls and values&lt;/li&gt;
 * &lt;/ul&gt;
 */
public class Abs2BlockEvalIT extends ESRestTestCase {

    private static final String TEST_INDEX = "test_abs2_block_eval";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.enabled", "false")
        .setting("xpack.security.http.ssl.enabled", "false")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Before
    public void setupIndex() throws IOException {
        // Create index with integer field
        Request createIndex = new Request("PUT", "/" + TEST_INDEX);
        createIndex.setJsonEntity("""
            {
                "mappings": {
                    "properties": {
                        "value": { "type": "integer" },
                        "name": { "type": "keyword" }
                    }
                }
            }
            """);
        client().performRequest(createIndex);
    }

    @After
    public void cleanupIndex() throws IOException {
        try {
            client().performRequest(new Request("DELETE", "/" + TEST_INDEX));
        } catch (Exception e) {
            // Ignore if index doesn't exist
        }
    }

    /**
     * Helper method to execute an ES|QL query via REST API.
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> runQuery(String query) throws IOException {
        Request request = new Request("POST", "/_query");
        request.setJsonEntity("{\"query\": \"" + query + "\"}");
        request.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE).build());

        Response response = client().performRequest(request);
        return entityAsMap(response);
    }

    /**
     * Helper method to index a document.
     */
    private void indexDoc(String id, String jsonEntity) throws IOException {
        Request indexDoc = new Request("PUT", "/" + TEST_INDEX + "/_doc/" + id);
        indexDoc.setJsonEntity(jsonEntity);
        client().performRequest(indexDoc);
    }

    /**
     * Helper method to refresh the index.
     */
    private void refreshIndex() throws IOException {
        client().performRequest(new Request("POST", "/" + TEST_INDEX + "/_refresh"));
    }

    /**
     * Test that null values in a block are handled correctly.
     * Null input should produce null output.
     */
    @SuppressWarnings("unchecked")
    public void testBlockEvalWithNulls() throws IOException {
        // Index documents: [5, null, -3, null, 7]
        indexDoc("1", "{\"value\": 5, \"name\": \"a\"}");
        indexDoc("2", "{\"value\": null, \"name\": \"b\"}");
        indexDoc("3", "{\"value\": -3, \"name\": \"c\"}");
        indexDoc("4", "{\"name\": \"d\"}");  // Missing field = null
        indexDoc("5", "{\"value\": 7, \"name\": \"e\"}");
        refreshIndex();

        // Query with abs2
        Map<String, Object> result = runQuery("FROM " + TEST_INDEX + " | EVAL result = abs2(value) | KEEP name, value, result | SORT name");

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(5));

        // Row a: value=5, result=5
        assertThat(values.get(0).get(0), equalTo("a"));
        assertThat(values.get(0).get(1), equalTo(5));
        assertThat(values.get(0).get(2), equalTo(5));

        // Row b: value=null, result=null
        assertThat(values.get(1).get(0), equalTo("b"));
        assertThat(values.get(1).get(1), nullValue());
        assertThat(values.get(1).get(2), nullValue());

        // Row c: value=-3, result=3
        assertThat(values.get(2).get(0), equalTo("c"));
        assertThat(values.get(2).get(1), equalTo(-3));
        assertThat(values.get(2).get(2), equalTo(3));

        // Row d: value=null (missing), result=null
        assertThat(values.get(3).get(0), equalTo("d"));
        assertThat(values.get(3).get(1), nullValue());
        assertThat(values.get(3).get(2), nullValue());

        // Row e: value=7, result=7
        assertThat(values.get(4).get(0), equalTo("e"));
        assertThat(values.get(4).get(1), equalTo(7));
        assertThat(values.get(4).get(2), equalTo(7));
    }

    /**
     * Test that single values in a block are processed correctly.
     */
    @SuppressWarnings("unchecked")
    public void testBlockEvalWithSingleValues() throws IOException {
        // Index documents with various negative and positive values
        indexDoc("1", "{\"value\": -10, \"name\": \"a\"}");
        indexDoc("2", "{\"value\": 20, \"name\": \"b\"}");
        indexDoc("3", "{\"value\": -30, \"name\": \"c\"}");
        indexDoc("4", "{\"value\": 0, \"name\": \"d\"}");
        indexDoc("5", "{\"value\": -1, \"name\": \"e\"}");
        refreshIndex();

        // Query with abs2
        Map<String, Object> result = runQuery("FROM " + TEST_INDEX + " | EVAL result = abs2(value) | KEEP name, value, result | SORT name");

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(5));

        // Row a: value=-10, result=10
        assertThat(values.get(0).get(0), equalTo("a"));
        assertThat(values.get(0).get(1), equalTo(-10));
        assertThat(values.get(0).get(2), equalTo(10));

        // Row b: value=20, result=20
        assertThat(values.get(1).get(0), equalTo("b"));
        assertThat(values.get(1).get(1), equalTo(20));
        assertThat(values.get(1).get(2), equalTo(20));

        // Row c: value=-30, result=30
        assertThat(values.get(2).get(0), equalTo("c"));
        assertThat(values.get(2).get(1), equalTo(-30));
        assertThat(values.get(2).get(2), equalTo(30));

        // Row d: value=0, result=0
        assertThat(values.get(3).get(0), equalTo("d"));
        assertThat(values.get(3).get(1), equalTo(0));
        assertThat(values.get(3).get(2), equalTo(0));

        // Row e: value=-1, result=1
        assertThat(values.get(4).get(0), equalTo("e"));
        assertThat(values.get(4).get(1), equalTo(-1));
        assertThat(values.get(4).get(2), equalTo(1));
    }

    /**
     * Test block evaluation with a larger dataset to ensure vector optimization works.
     * When all values are single-valued, the evaluator should use the vector path.
     */
    @SuppressWarnings("unchecked")
    public void testBlockEvalWithLargeDataset() throws IOException {
        // Index 100 documents
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            int value = (i % 2 == 0) ? -i : i;
            bulk.append("{\"index\": {\"_id\": \"").append(i).append("\"}}\n");
            bulk.append("{\"value\": ").append(value).append(", \"name\": \"doc").append(i).append("\"}\n");
        }

        Request bulkRequest = new Request("POST", "/" + TEST_INDEX + "/_bulk");
        bulkRequest.setJsonEntity(bulk.toString());
        bulkRequest.addParameter("refresh", "true");
        client().performRequest(bulkRequest);

        // Query with abs2 and count results
        Map<String, Object> result = runQuery("FROM " + TEST_INDEX + " | EVAL result = abs2(value) | STATS count = COUNT(*)");

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));
        assertThat(((Number) values.get(0).get(0)).intValue(), equalTo(100));

        // Verify some specific values
        Map<String, Object> detailResult = runQuery(
            "FROM " + TEST_INDEX + " | EVAL result = abs2(value) | WHERE value == -10 | KEEP value, result"
        );
        List<List<Object>> detailValues = (List<List<Object>>) detailResult.get("values");
        assertThat(detailValues, hasSize(1));
        assertThat(detailValues.get(0).get(0), equalTo(-10));
        assertThat(detailValues.get(0).get(1), equalTo(10));
    }

    /**
     * Test block evaluation with mixed nulls and values in a larger dataset.
     * This tests the block path (not vector) when there are nulls.
     */
    @SuppressWarnings("unchecked")
    public void testBlockEvalWithMixedNullsLargeDataset() throws IOException {
        // Index documents with every 3rd being null
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < 30; i++) {
            bulk.append("{\"index\": {\"_id\": \"").append(i).append("\"}}\n");
            if (i % 3 == 0) {
                bulk.append("{\"name\": \"doc").append(i).append("\"}\n");  // null value
            } else {
                int value = (i % 2 == 0) ? -i : i;
                bulk.append("{\"value\": ").append(value).append(", \"name\": \"doc").append(i).append("\"}\n");
            }
        }

        Request bulkRequest = new Request("POST", "/" + TEST_INDEX + "/_bulk");
        bulkRequest.setJsonEntity(bulk.toString());
        bulkRequest.addParameter("refresh", "true");
        client().performRequest(bulkRequest);

        // Count non-null results
        Map<String, Object> result = runQuery(
            "FROM " + TEST_INDEX + " | EVAL result = abs2(value) | WHERE result IS NOT NULL | STATS count = COUNT(*)"
        );

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(1));
        // 30 docs, every 3rd is null = 10 nulls, so 20 non-null
        assertThat(((Number) values.get(0).get(0)).intValue(), equalTo(20));

        // Count null results
        Map<String, Object> nullResult = runQuery(
            "FROM " + TEST_INDEX + " | EVAL result = abs2(value) | WHERE result IS NULL | STATS count = COUNT(*)"
        );

        List<List<Object>> nullValues = (List<List<Object>>) nullResult.get("values");
        assertThat(nullValues, hasSize(1));
        assertThat(((Number) nullValues.get(0).get(0)).intValue(), equalTo(10));
    }

    /**
     * Test that abs2 works correctly when chained with other operations.
     */
    @SuppressWarnings("unchecked")
    public void testBlockEvalWithChainedOperations() throws IOException {
        // Index documents
        indexDoc("1", "{\"value\": -5, \"name\": \"a\"}");
        indexDoc("2", "{\"value\": null, \"name\": \"b\"}");
        indexDoc("3", "{\"value\": -10, \"name\": \"c\"}");
        refreshIndex();

        // Chain abs2 with arithmetic
        Map<String, Object> result = runQuery(
            "FROM "
                + TEST_INDEX
                + " | EVAL abs_val = abs2(value), doubled = abs2(value) * 2 | KEEP name, value, abs_val, doubled | SORT name"
        );

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(3));

        // Row a: value=-5, abs_val=5, doubled=10
        assertThat(values.get(0).get(0), equalTo("a"));
        assertThat(values.get(0).get(1), equalTo(-5));
        assertThat(values.get(0).get(2), equalTo(5));
        assertThat(values.get(0).get(3), equalTo(10));

        // Row b: value=null, abs_val=null, doubled=null
        assertThat(values.get(1).get(0), equalTo("b"));
        assertThat(values.get(1).get(1), nullValue());
        assertThat(values.get(1).get(2), nullValue());
        assertThat(values.get(1).get(3), nullValue());

        // Row c: value=-10, abs_val=10, doubled=20
        assertThat(values.get(2).get(0), equalTo("c"));
        assertThat(values.get(2).get(1), equalTo(-10));
        assertThat(values.get(2).get(2), equalTo(10));
        assertThat(values.get(2).get(3), equalTo(20));
    }

    /**
     * Test that abs2 works with filtering on the result.
     */
    @SuppressWarnings("unchecked")
    public void testBlockEvalWithFiltering() throws IOException {
        // Index documents
        indexDoc("1", "{\"value\": -5, \"name\": \"a\"}");
        indexDoc("2", "{\"value\": -15, \"name\": \"b\"}");
        indexDoc("3", "{\"value\": null, \"name\": \"c\"}");
        indexDoc("4", "{\"value\": -25, \"name\": \"d\"}");
        indexDoc("5", "{\"value\": 3, \"name\": \"e\"}");
        refreshIndex();

        // Filter on abs2 result
        Map<String, Object> result = runQuery(
            "FROM " + TEST_INDEX + " | EVAL abs_val = abs2(value) | WHERE abs_val > 10 | KEEP name, value, abs_val | SORT name"
        );

        List<List<Object>> values = (List<List<Object>>) result.get("values");
        assertThat(values, hasSize(2));

        // Row b: value=-15, abs_val=15
        assertThat(values.get(0).get(0), equalTo("b"));
        assertThat(values.get(0).get(1), equalTo(-15));
        assertThat(values.get(0).get(2), equalTo(15));

        // Row d: value=-25, abs_val=25
        assertThat(values.get(1).get(0), equalTo("d"));
        assertThat(values.get(1).get(1), equalTo(-25));
        assertThat(values.get(1).get(2), equalTo(25));
    }
}
