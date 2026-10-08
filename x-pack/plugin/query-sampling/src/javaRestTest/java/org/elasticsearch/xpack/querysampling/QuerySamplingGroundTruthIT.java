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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Runs the whole path against a real cluster: a kNN search is captured and sampled, the sampled query is written to
 * the index of the sample, its weights are kept up to date and its ground truth is computed through the REST API.
 * A sampled query reaches the index on another thread, which a YAML test cannot wait for, hence this class and its
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

    public void testGroundTruthOfStoredQueriesIsComputed() throws Exception {
        setUpIndexAndSampling();
        float x = sampleOneQuery();
        assertThat(storedValue(x, "has_ground_truth"), equalTo(false));

        // the index keeps what was sampled between tests and a call does at most 100 queries, so ask until nothing is
        // left; what a call stored is only found as done by the next one once the index was refreshed
        long computed = 0;
        long last;
        int calls = 0;
        do {
            refreshSampleIndex();
            ObjectPath result = ObjectPath.createFromResponse(
                client().performRequest(new Request("POST", "/_query_sampling/ground_truth"))
            );
            last = ((Number) result.evaluate("computed")).longValue();
            computed += last;
        } while (last > 0 && ++calls < 100);

        assertThat(computed, greaterThanOrEqualTo(1L));
        assertThat(last, equalTo(0L));
        assertBusy(() -> assertThat(storedValue(x, "has_ground_truth"), equalTo(true)));
        assertThat(((List<?>) storedValue(x, "ground_truth.neighbors")).size(), equalTo(3));
    }

    public void testSampledQueriesAreWrittenToTheIndex() throws Exception {
        setUpIndexAndSampling();
        sampleOneQuery();
    }

    public void testWeightsOfStoredQueriesAreRefreshed() throws Exception {
        setUpIndexAndSampling();
        float x = sampleOneQuery();

        // the document was written when the query was picked, the arrivals that follow only reach it as an update
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
     * Waits until one of them is in the index of the sample.
     *
     * @return the first component of the vector of such a query, which tells it from the others
     */
    private float sampleOneQuery() throws Exception {
        List<Float> sent = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            float x = randomFloat();
            sent.add(x);
            client().performRequest(knnSearch(x));
        }
        Float[] sampled = new Float[1];
        // queries are written once the flush interval has passed
        assertBusy(() -> {
            for (float x : sent) {
                if (storedValue(x, "weighted_multiplicity") != null) {
                    sampled[0] = x;
                    return;
                }
            }
            fail("none of the searched queries is in the index of the sample");
        });
        return sampled[0];
    }

    private static Request knnSearch(float x) {
        Request search = new Request("POST", "/vectors/_search");
        search.setJsonEntity("{ \"knn\": { \"field\": \"vec\", \"query_vector\": [" + x + ", 1.0], \"k\": 3, \"num_candidates\": 10 } }");
        return search;
    }

    private static double storedMultiplicity(float x) throws IOException {
        return ((Number) storedValue(x, "weighted_multiplicity")).doubleValue();
    }

    /**
     * A field of the document of the index of the sample whose query vector starts with {@code x}, or {@code null}
     * if there is no such document.
     */
    private static Object storedValue(float x, String path) throws IOException {
        if (refreshSampleIndex() == false) {
            return null;
        }
        Request search = new Request("GET", "/.query_sampling/_search?size=10000");
        search.setOptions(systemIndexAccess());
        ObjectPath result = ObjectPath.createFromResponse(client().performRequest(search));
        List<?> hits = result.evaluate("hits.hits");
        for (int i = 0; i < hits.size(); i++) {
            double first = ((Number) result.evaluate("hits.hits." + i + "._source.query.query_vector.0")).doubleValue();
            if (Math.abs(first - x) < 1e-6) {
                return result.evaluate("hits.hits." + i + "._source." + path);
            }
        }
        return null;
    }

    /**
     * @return whether the index of the sample exists, which it does not before the first write
     */
    private static boolean refreshSampleIndex() throws IOException {
        Request refresh = new Request("POST", "/.query_sampling/_refresh");
        refresh.setOptions(systemIndexAccess());
        try {
            client().performRequest(refresh);
            return true;
        } catch (ResponseException e) {
            return false;
        }
    }

    /**
     * The index of the sample is a system index: reading it directly is allowed, but gets a deprecation warning.
     */
    private static RequestOptions systemIndexAccess() {
        return RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE).build();
    }
}
