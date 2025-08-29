/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.client.Request;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.esql.AssertWarnings;
import org.elasticsearch.xpack.esql.CsvTestsDataLoader;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.qa.rest.ProfileLogger;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.requestObjectBuilder;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.runEsqlSync;
import static org.hamcrest.Matchers.is;

@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class KnnSemanticTextIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster(spec -> spec.plugin("inference-service-test"));

    @Rule(order = Integer.MIN_VALUE)
    public ProfileLogger profileLogger = new ProfileLogger();

    private int numDocs;
    private final Map<Integer, String> indexedTexts = new HashMap<>();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Before
    public void checkCapability() {
        assumeTrue("semantic text capability not available", EsqlCapabilities.Cap.KNN_FUNCTION_V4.isEnabled());
    }

    public void testKnnQuery() throws IOException {
        String knnQuery = """
            FROM semantic-test METADATA _score
            | WHERE knn(semantic, [0, 1, 2], 10)
            | KEEP id, _score, semantic
            | SORT _score DESC
            | LIMIT 10
            """;

        Map<String, Object> response = runEsqlQuery(knnQuery);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> columns = (List<Map<String, Object>>) response.get("columns");
        assertThat(columns.size(), is(3));
    }

    @Before
    public void setupIndex() throws IOException {
        Request request = new Request("PUT", "/semantic-test");
        request.setJsonEntity("""
            {
              "mappings": {
                "properties": {
                  "id": {
                    "type": "integer"
                  },
                  "semantic": {
                    "type": "semantic_text",
                    "inference_id": "test_dense_inference"
                  }
                }
              },
              "settings": {
                "index": {
                  "number_of_shards": 1,
                  "number_of_replicas": 0
                }
              }
            }
            """);
        assertEquals(200, client().performRequest(request).getStatusLine().getStatusCode());

        request = new Request("POST", "/_bulk?index=semantic-test&refresh=true");
        // 4 documents with a null in the middle, leading to 3 ESQL pages and 3 Arrow batches
        request.setJsonEntity("""
            {"index": {"_id": "1"}}
            {"id": 1, "semantic": "sample text one"}
            {"index": {"_id": "2"}}
            {"id": 2, "semantic": "sample text two"}
            {"index": {"_id": "3"}}
            {"id": 3, "semantic": "sample text three"}
            """);
        assertEquals(200, client().performRequest(request).getStatusLine().getStatusCode());
    }

    @Before
    public void setupInferenceEndpoint() throws IOException {
        CsvTestsDataLoader.createTextEmbeddingInferenceEndpoint(client());
    }

    @After
    public void removeIndexAndInferenceEndpoint() throws IOException {
        client().performRequest(new Request("DELETE", "semantic-test"));

        if (CsvTestsDataLoader.clusterHasTextEmbeddingInferenceEndpoint(client())) {
            CsvTestsDataLoader.deleteTextEmbeddingInferenceEndpoint(client());
        }
    }

    private Map<String, Object> runEsqlQuery(String query) throws IOException {
        RestEsqlTestCase.RequestObjectBuilder builder = requestObjectBuilder().query(query);
        return runEsqlSync(builder, new AssertWarnings.NoWarnings(), profileLogger);
    }
}
