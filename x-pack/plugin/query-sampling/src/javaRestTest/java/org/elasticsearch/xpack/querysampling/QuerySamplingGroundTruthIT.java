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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
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
        .setting("xpack.query_sampling.weights_refresh_interval", "1s")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testGroundTruthOfASampledSearchIsComputed() throws Exception {
        setUpIndexAndSampling();

        // the node keeps what it sampled between tests, so counts are relative
        long buffered = nodeStat("buffered");
        long withGroundTruth = nodeStat("with_ground_truth");

        // a new query is picked with a probability below one, so search for several different ones
        searchDistinctQueries();
        assertBusy(() -> assertThat(nodeStat("buffered"), greaterThan(buffered)));
        assertThat(nodeStat("with_ground_truth"), equalTo(withGroundTruth));

        ObjectPath result = ObjectPath.createFromResponse(client().performRequest(new Request("POST", "/_query_sampling/ground_truth")));
        // the other test of this class may have left a sampled query without ground truth on the node
        long computed = nodeValue(result, "computed");
        assertThat(computed, greaterThanOrEqualTo(1L));
        assertThat(nodeValue(result, "failed"), equalTo(0L));
        assertThat(nodeStat("with_ground_truth"), equalTo(withGroundTruth + computed));

        // nothing stays pending: a call does at most 100 queries, so ask until one has nothing left to do
        long last = computed;
        for (int i = 0; i < 100 && last > 0; i++) {
            result = ObjectPath.createFromResponse(client().performRequest(new Request("POST", "/_query_sampling/ground_truth")));
            last = nodeValue(result, "computed");
        }
        assertThat(last, equalTo(0L));
    }

    public void testSampledQueriesAreWrittenToTheIndex() throws Exception {
        setUpIndexAndSampling();
        List<Float> sent = searchDistinctQueries();

        // the queries are written once the flush interval has passed
        assertBusy(() -> assertNotNull("a document has the vector of one of the searches", sampledVector(sent)));
    }

    public void testWeightsOfStoredQueriesAreRefreshed() throws Exception {
        setUpIndexAndSampling();
        List<Float> sent = searchDistinctQueries();
        Float[] sampled = new Float[1];
        assertBusy(() -> {
            sampled[0] = sampledVector(sent);
            assertNotNull("a document has the vector of one of the searches", sampled[0]);
        });

        // the document was written when the query was picked, the arrivals that follow only reach it as an update
        float x = sampled[0];
        double before = storedMultiplicity(x);
        int arrivals = 5;
        for (int i = 0; i < arrivals; i++) {
            client().performRequest(knnSearch(x));
        }
        assertBusy(() -> assertThat(storedMultiplicity(x), greaterThanOrEqualTo(before + arrivals)));
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

    /**
     * Searches with many different vectors, so that it is practically certain that some of them are picked: a
     * new query is picked with a probability of about 0.69, and one that repeats is less and less likely to be.
     */
    private static List<Float> searchDistinctQueries() throws IOException {
        List<Float> sent = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            float x = randomFloat();
            sent.add(x);
            client().performRequest(knnSearch(x));
        }
        return sent;
    }

    private static Request knnSearch(float x) {
        Request search = new Request("POST", "/vectors/_search");
        search.setJsonEntity("{ \"knn\": { \"field\": \"vec\", \"query_vector\": [" + x + ", 1.0], \"k\": 3, \"num_candidates\": 10 } }");
        return search;
    }

    /**
     * The first of the vectors that has a document in the index of the sample, or {@code null}.
     */
    private static Float sampledVector(List<Float> vectors) throws IOException {
        for (float x : vectors) {
            if (storedMultiplicity(x) >= 0) {
                return x;
            }
        }
        return null;
    }

    /**
     * The estimated multiplicity stored with the sampled query whose vector starts with {@code x}, or -1 if it
     * was not stored.
     */
    private static double storedMultiplicity(float x) throws IOException {
        Request refresh = new Request("POST", "/.query_sampling/_refresh");
        refresh.setOptions(systemIndexAccess());
        try {
            client().performRequest(refresh);
        } catch (ResponseException e) {
            return -1; // the index does not exist before the first write
        }
        Request search = new Request("GET", "/.query_sampling/_search?size=10000");
        search.setOptions(systemIndexAccess());
        ObjectPath result = ObjectPath.createFromResponse(client().performRequest(search));
        List<?> hits = result.evaluate("hits.hits");
        for (int i = 0; i < hits.size(); i++) {
            double first = ((Number) result.evaluate("hits.hits." + i + "._source.query.query_vector.0")).doubleValue();
            if (Math.abs(first - x) < 1e-6) {
                return ((Number) result.evaluate("hits.hits." + i + "._source.weighted_multiplicity")).doubleValue();
            }
        }
        return -1;
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
