/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.rest;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.esql.AssertWarnings;
import org.elasticsearch.xpack.esql.CsvTestsDataLoader;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.rest.RestStatus.BAD_REQUEST;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.requestObjectBuilder;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.runEsqlSync;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.StringContains.containsString;

/**
 * Tests kNN queries on semantic_text fields. Mostly checks errors on the data node that can't be checked in other tests.
 */
public class KnnSemanticTextTestCase extends ESRestTestCase {

    @Rule(order = Integer.MIN_VALUE)
    public ProfileLogger profileLogger = new ProfileLogger();

    @Before
    public void checkCapability() {
        // TODO: not sure why this doesn't work??
        // assumeTrue("knn with semantic text not available", EsqlCapabilities.Cap.KNN_FUNCTION_V5.isEnabled());
    }

    @SuppressWarnings("unchecked")
    public void testKnnQueryWithSemanticText() throws IOException {
        String knnQuery = """
            FROM semantic-test METADATA _score
            | WHERE knn(dense_semantic, [0, 1, 2])
            | KEEP id, _score, dense_semantic
            | SORT _score DESC
            | LIMIT 10
            """;

        Map<String, Object> response = runEsqlQuery(knnQuery);
        List<Map<String, Object>> columns = (List<Map<String, Object>>) response.get("columns");
        assertThat(columns.size(), is(3));
        List<List<Object>> rows = (List<List<Object>>) response.get("values");
        assertThat(rows.size(), is(3));
        for (int row = 0; row < rows.size(); row++) {
            List<Object> rowData = rows.get(row);
            Integer id = (Integer) rowData.get(0);
            assertThat(id, is(3 - row));
        }
    }

    public void testKnnQueryOnTextField() throws IOException {
        String knnQuery = """
            FROM semantic-test METADATA _score
            | WHERE knn(text, [0, 1, 2])
            | KEEP id, _score, dense_semantic
            | SORT _score DESC
            | LIMIT 10
            """;

        ResponseException re = expectThrows(ResponseException.class, () -> runEsqlQuery(knnQuery));
        assertThat(re.getResponse().getStatusLine().getStatusCode(), is(BAD_REQUEST.getStatus()));
        assertThat(re.getMessage(), containsString("[knn] queries are only supported on [dense_vector] fields"));
    }

    public void testKnnQueryOnSparseSemanticTextField() throws IOException {
        String knnQuery = """
            FROM semantic-test METADATA _score
            | WHERE knn(sparse_semantic, [0, 1, 2])
            | KEEP id, _score, sparse_semantic
            | SORT _score DESC
            | LIMIT 10
            """;

        ResponseException re = expectThrows(ResponseException.class, () -> runEsqlQuery(knnQuery));
        assertThat(re.getResponse().getStatusLine().getStatusCode(), is(BAD_REQUEST.getStatus()));
        assertThat(re.getMessage(), containsString("Field [sparse_semantic] does not use a [text_embedding] model"));
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        setupInferenceEndpoints();
        setupIndex();
    }

    private void setupIndex() throws IOException {
        Request request = new Request("PUT", "/semantic-test");
        request.setJsonEntity("""
            {
              "mappings": {
                "properties": {
                  "id": {
                    "type": "integer"
                  },
                  "dense_semantic": {
                    "type": "semantic_text",
                    "inference_id": "test_dense_inference"
                  },
                  "sparse_semantic": {
                    "type": "semantic_text",
                    "inference_id": "test_sparse_inference"
                  },
                  "text": {
                    "type": "text",
                    "copy_to": ["dense_semantic", "sparse_semantic"]
                  }
                }
              }
            }
            """);
        assertEquals(200, client().performRequest(request).getStatusLine().getStatusCode());

        request = new Request("POST", "/_bulk?index=semantic-test&refresh=true");
        request.setJsonEntity("""
            {"index": {"_id": "1"}}
            {"id": 1, "text": "sample text"}
            {"index": {"_id": "2"}}
            {"id": 2, "text": "another sample text"}
            {"index": {"_id": "3"}}
            {"id": 3, "text": "yet another sample text"}
            """);
        assertEquals(200, client().performRequest(request).getStatusLine().getStatusCode());
    }

    private void setupInferenceEndpoints() throws IOException {
        CsvTestsDataLoader.createInferenceEndpoint(client(), CsvTestsDataLoader.INFERENCE_CONFIGS.get("test_dense_inference"));
        CsvTestsDataLoader.createInferenceEndpoint(client(), CsvTestsDataLoader.INFERENCE_CONFIGS.get("test_sparse_inference"));
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        client().performRequest(new Request("DELETE", "semantic-test"));
        CsvTestsDataLoader.deleteInferenceEndpoint(client(), "test_dense_inference");
        CsvTestsDataLoader.deleteInferenceEndpoint(client(), "test_sparse_inference");
    }

    private Map<String, Object> runEsqlQuery(String query) throws IOException {
        RestEsqlTestCase.RequestObjectBuilder builder = requestObjectBuilder().query(query);
        return runEsqlSync(builder, new AssertWarnings.NoWarnings(), profileLogger);
    }
}
