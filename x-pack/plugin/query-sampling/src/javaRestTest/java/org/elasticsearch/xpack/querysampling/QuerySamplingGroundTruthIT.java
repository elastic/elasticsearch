/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Runs the whole path against a real cluster: a kNN search is captured and sampled, its ground truth is
 * computed through the REST API and the sampled query is written to the index of the sample. A sampled query
 * reaches the buffer and the index on other threads, which a YAML test cannot wait for, hence this class and its
 * polling.
 */
public class QuerySamplingGroundTruthIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("x-pack-query-sampling")
        .feature(FeatureFlag.QUERY_SAMPLING)
        .setting("xpack.security.enabled", "false")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testGroundTruthOfASampledSearchIsComputed() throws Exception {
        setUpIndexAndSampling();

        // the node keeps what it sampled between tests, so this query must be new to it and counts are relative
        long buffered = nodeStat("buffered");
        long withGroundTruth = nodeStat("with_ground_truth");
        Request search = knnSearch(randomFloat());

        // the sampler picks a new query with a probability below one, so repeat the search until it did
        assertBusy(() -> {
            client().performRequest(search);
            assertThat(nodeStat("buffered"), equalTo(buffered + 1));
        });
        assertThat(nodeStat("with_ground_truth"), equalTo(withGroundTruth));

        ObjectPath result = ObjectPath.createFromResponse(client().performRequest(new Request("POST", "/_query_sampling/ground_truth")));
        // the other test of this class may have left a sampled query without ground truth on the node
        long computed = nodeValue(result, "computed");
        assertThat(computed, greaterThanOrEqualTo(1L));
        assertThat(nodeValue(result, "failed"), equalTo(0L));
        assertThat(nodeStat("with_ground_truth"), equalTo(withGroundTruth + computed));

        // nothing is pending any more
        result = ObjectPath.createFromResponse(client().performRequest(new Request("POST", "/_query_sampling/ground_truth")));
        assertThat(nodeValue(result, "computed"), equalTo(0L));
    }

    public void testSampledQueriesAreWrittenToTheIndex() throws Exception {
        setUpIndexAndSampling();
        float x = randomFloat();
        Request search = knnSearch(x);

        // the query is picked with a probability below one and written once the flush interval has passed
        assertBusy(() -> {
            client().performRequest(search);
            assertTrue("a document has the vector of the search", isSampled(x));
        });
    }

    private void setUpIndexAndSampling() throws IOException {
        createIndex("vectors", indexSettings(1, 0).build(), """
            "properties": { "vec": { "type": "dense_vector", "dims": 2, "index": true, "similarity": "l2_norm" } }
            """);
        for (int i = 0; i < 5; i++) {
            Request index = new Request("PUT", "/vectors/_doc/" + i);
            index.setJsonEntity("{\"vec\": [" + i + ", " + i + "]}");
            client().performRequest(index);
        }
        client().performRequest(new Request("POST", "/vectors/_refresh"));

        Request settings = new Request("PUT", "/_cluster/settings");
        settings.setJsonEntity("""
            { "persistent": { "xpack.query_sampling.enabled": true, "xpack.query_sampling.capture_rate": 1.0 } }
            """);
        client().performRequest(settings);
    }

    private static Request knnSearch(float x) {
        Request search = new Request("POST", "/vectors/_search");
        search.setJsonEntity("{ \"knn\": { \"field\": \"vec\", \"query_vector\": [" + x + ", 1.0], \"k\": 3, \"num_candidates\": 10 } }");
        return search;
    }

    /**
     * Whether a document of the index of the sample holds a query vector that starts with {@code x}.
     */
    private static boolean isSampled(float x) throws IOException {
        Request refresh = new Request("POST", "/.query_sampling/_refresh");
        refresh.setOptions(systemIndexAccess());
        try {
            client().performRequest(refresh);
        } catch (ResponseException e) {
            return false; // the index does not exist before the first write
        }
        Request search = new Request("GET", "/.query_sampling/_search");
        search.setOptions(systemIndexAccess());
        ObjectPath result = ObjectPath.createFromResponse(client().performRequest(search));
        List<?> hits = result.evaluate("hits.hits");
        for (int i = 0; i < hits.size(); i++) {
            double first = ((Number) result.evaluate("hits.hits." + i + "._source.query.query_vector.0")).doubleValue();
            if (Math.abs(first - x) < 1e-6) {
                return true;
            }
        }
        return false;
    }

    /**
     * The index of the sample is a system index: reading it directly is allowed, but gets a deprecation warning.
     */
    private static RequestOptions systemIndexAccess() {
        return RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE).build();
    }

    private static long nodeStat(String name) throws IOException {
        return nodeValue(ObjectPath.createFromResponse(client().performRequest(new Request("GET", "/_query_sampling/stats"))), name);
    }

    private static long nodeValue(ObjectPath response, String name) throws IOException {
        Map<?, ?> nodes = response.evaluate("nodes");
        assertThat("the test cluster has one node", nodes.size(), equalTo(1));
        String nodeId = (String) nodes.keySet().iterator().next();
        return ((Number) response.evaluate("nodes." + nodeId + "." + name)).longValue();
    }
}
