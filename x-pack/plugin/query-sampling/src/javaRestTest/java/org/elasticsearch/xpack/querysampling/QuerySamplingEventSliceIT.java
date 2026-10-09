/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * The arrivals that are kept as events, against a real cluster: every search of a query is a document of its own in
 * the index of the sample, whose ground truth is computed like any other and which makes an estimate of the recall of
 * the traffic.
 */
public class QuerySamplingEventSliceIT extends QuerySamplingRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("x-pack-query-sampling")
        .module("reindex")
        .feature(FeatureFlag.QUERY_SAMPLING)
        .setting("xpack.security.enabled", "false")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testEveryArrivalIsADocumentAndGivesAnEstimateOfTheTraffic() throws Exception {
        setUpIndexAndSampling();
        Request settings = new Request("PUT", "/_cluster/settings");
        settings.setJsonEntity("""
            { "persistent": { "xpack.query_sampling.event_slice_rate": 1.0 } }
            """);
        client().performRequest(settings);

        // the same query, so that what tells the events apart is not the query
        float x = randomFloat();
        int searches = 4;
        for (int i = 0; i < searches; i++) {
            client().performRequest(knnSearch(x));
        }
        assertBusy(() -> assertThat(eventsOf(x), greaterThanOrEqualTo(searches)));

        // the index keeps what was sampled between tests, so ask until nothing is left to compute
        long last;
        int calls = 0;
        do {
            refreshSampleIndex();
            last = ((Number) ObjectPath.createFromResponse(client().performRequest(new Request("POST", "/_query_sampling/ground_truth")))
                .evaluate("computed")).longValue();
        } while (last > 0 && ++calls < 100);
        assertThat(last, equalTo(0L));

        assertBusy(() -> {
            refreshSampleIndex();
            ObjectPath estimate = ObjectPath.createFromResponse(client().performRequest(new Request("GET", "/_query_sampling/recall")));
            assertThat(((Number) estimate.evaluate("event_slice.records_with_ground_truth")).intValue(), greaterThanOrEqualTo(searches));
            // the index is so small that the approximate search finds every true neighbour
            assertThat(((Number) estimate.evaluate("event_slice.recall")).doubleValue(), greaterThan(0.99));
            assertThat(((Number) estimate.evaluate("event_slice.effective_size")).doubleValue(), greaterThan(0.0));
        });
    }

    /**
     * The documents of the index of the sample that are events of the query whose vector starts with {@code x}.
     */
    private static int eventsOf(float x) throws IOException {
        if (refreshSampleIndex() == false) {
            return 0;
        }
        Request search = new Request("GET", "/.query_sampling/_search?size=10000");
        search.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE).build());
        ObjectPath result = ObjectPath.createFromResponse(client().performRequest(search));
        List<?> hits = result.evaluate("hits.hits");
        int events = 0;
        for (int i = 0; i < hits.size(); i++) {
            double first = ((Number) result.evaluate("hits.hits." + i + "._source.query.query_vector.0")).doubleValue();
            if (Math.abs(first - x) < 1e-6 && result.evaluate("hits.hits." + i + "._source.event_id") != null) {
                events++;
            }
        }
        return events;
    }
}
