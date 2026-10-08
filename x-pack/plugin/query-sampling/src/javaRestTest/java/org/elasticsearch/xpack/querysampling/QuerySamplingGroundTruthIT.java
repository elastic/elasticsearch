/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling;

import org.elasticsearch.client.Request;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.ClassRule;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
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
}
