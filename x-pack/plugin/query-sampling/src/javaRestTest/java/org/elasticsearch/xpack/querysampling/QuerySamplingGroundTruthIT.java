/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.ClassRule;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Runs the whole path against a real cluster: a kNN search is captured and sampled, the sampled query is written to
 * the index of the sample, its weights are kept up to date and its ground truth is computed through the REST API.
 * A sampled query reaches the index on another thread, which a YAML test cannot wait for, hence this class and its
 * polling.
 */
public class QuerySamplingGroundTruthIT extends QuerySamplingRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("x-pack-query-sampling")
        .module("reindex")
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

        // the queries that have ground truth now are what the recall is estimated from
        refreshSampleIndex();
        ObjectPath estimate = ObjectPath.createFromResponse(
            client().performRequest(new Request("GET", "/_query_sampling/recall?include_samples=true"))
        );
        assertThat(((Number) estimate.evaluate("records_with_ground_truth")).intValue(), greaterThanOrEqualTo(1));
        // the index is so small that the approximate search finds every true neighbour
        assertThat(((Number) estimate.evaluate("traffic_weighted_recall")).doubleValue(), greaterThan(0.99));
        assertThat(((Number) estimate.evaluate("unique_query_recall")).doubleValue(), greaterThan(0.99));
        assertThat(((List<?>) estimate.evaluate("samples")).size(), greaterThanOrEqualTo(1));
    }

    public void testSampledQueriesAreWrittenToTheIndex() throws Exception {
        setUpIndexAndSampling();
        sampleOneQuery();
    }

    public void testHeadThresholdCanBeChangedWhileRunning() throws Exception {
        setUpIndexAndSampling();
        // a head threshold of one makes every captured query a head query, which is always picked, so none is left to chance
        Request settings = new Request("PUT", "/_cluster/settings");
        settings.setJsonEntity("""
            { "persistent": { "xpack.query_sampling.head_threshold": 1 } }
            """);
        client().performRequest(settings);

        List<Float> sent = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            float x = randomFloat();
            sent.add(x);
            client().performRequest(knnSearch(x));
        }

        assertBusy(() -> {
            for (float x : sent) {
                assertNotNull("the query was picked", storedValue(x, "weighted_multiplicity"));
            }
        });
    }

    public void testFloorCapturesQuietTrafficWhateverTheCaptureRate() throws Exception {
        setUpIndexAndSampling();
        // a capture rate of zero captures nothing: what is captured is only because of the floor. Head threshold one
        // picks every captured query, so that the outcome does not depend on a coin flip
        Request settings = new Request("PUT", "/_cluster/settings");
        settings.setJsonEntity("""
            { "persistent": {
                "xpack.query_sampling.capture_rate": 0.0,
                "xpack.query_sampling.min_captures_per_hour": 1000000,
                "xpack.query_sampling.head_threshold": 1
            } }
            """);
        client().performRequest(settings);

        // the traffic is observed once a second, until then the rate is the configured one
        List<Float> sent = new ArrayList<>();
        assertBusy(() -> {
            float x = randomFloat();
            sent.add(x);
            client().performRequest(knnSearch(x));
            boolean any = false;
            for (float y : sent) {
                any |= storedValue(y, "weighted_multiplicity") != null;
            }
            assertTrue("one of the searches was captured", any);
        });
    }

    public void testGroundTruthIsComputedByItselfWithinTheBudget() throws Exception {
        setUpIndexAndSampling();
        Request settings = new Request("PUT", "/_cluster/settings");
        settings.setJsonEntity("""
            { "persistent": { "xpack.query_sampling.sampling_cost_ratio": 1.0 } }
            """);
        client().performRequest(settings);

        // the searches earn the credit that the exact searches are paid with, and the worker looks for work every
        // few seconds, so nobody asks for the ground truth here
        float x = sampleOneQuery();

        assertBusy(() -> assertThat(storedValue(x, "has_ground_truth"), equalTo(true)), 60, TimeUnit.SECONDS);
        assertThat(((List<?>) storedValue(x, "ground_truth.neighbors")).size(), equalTo(3));
    }

    public void testSettingsOutOfRangeAreRejected() throws Exception {
        Request settings = new Request("PUT", "/_cluster/settings");
        settings.setJsonEntity("""
            { "persistent": { "xpack.query_sampling.head_threshold": 0 } }
            """);
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(settings));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
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
}
