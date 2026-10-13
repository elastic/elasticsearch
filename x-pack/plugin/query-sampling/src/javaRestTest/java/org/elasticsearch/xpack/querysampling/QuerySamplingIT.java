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
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;

public class QuerySamplingIT extends QuerySamplingRestTestCase {

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
        assertThat(((Number) storedValue(x, "ground_truth.data_state.documents")).longValue(), equalTo(5L));
        assertThat(((Number) storedValue(x, "ground_truth.data_state.seq_no_sum")).doubleValue(), equalTo(10.0));

        refreshSampleIndex();
        ObjectPath estimate = ObjectPath.createFromResponse(
            client().performRequest(new Request("GET", "/_query_sampling/recall?include_samples=true"))
        );
        assertThat(((Number) estimate.evaluate("records_with_ground_truth")).intValue(), greaterThanOrEqualTo(1));
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
        Request settings = new Request("PUT", "/_cluster/settings");
        settings.setJsonEntity("""
            { "persistent": {
                "xpack.query_sampling.capture_rate": 0.0,
                "xpack.query_sampling.min_captures_per_hour": 1000000,
                "xpack.query_sampling.head_threshold": 1
            } }
            """);
        client().performRequest(settings);

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

        double before = storedMultiplicity(x);
        int arrivals = 5;
        for (int i = 0; i < arrivals; i++) {
            client().performRequest(knnSearch(x));
        }
        assertBusy(() -> assertThat(storedMultiplicity(x), greaterThanOrEqualTo(before + arrivals)));
    }

    public void testEveryArrivalIsADocumentAndGivesAnEstimateOfTheTraffic() throws Exception {
        setUpIndexAndSampling();
        Request settings = new Request("PUT", "/_cluster/settings");
        settings.setJsonEntity("""
            { "persistent": { "xpack.query_sampling.event_slice_rate": 1.0 } }
            """);
        client().performRequest(settings);

        float x = randomFloat();
        int searches = 4;
        for (int i = 0; i < searches; i++) {
            client().performRequest(knnSearch(x));
        }
        assertBusy(() -> assertThat(eventsOf(x), greaterThanOrEqualTo(searches)));

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
            assertThat(((Number) estimate.evaluate("event_slice.recall")).doubleValue(), greaterThan(0.99));
            assertThat(((Number) estimate.evaluate("event_slice.effective_size")).doubleValue(), greaterThan(0.0));
        });
    }

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

    private static Request filteredKnnSearch(float x, String filter) {
        Request search = new Request("POST", "/vectors/_search");
        search.setJsonEntity(
            "{ \"knn\": { \"field\": \"vec\", \"query_vector\": ["
                + x
                + ", 1.0], \"k\": 3, \"num_candidates\": 10, \"filter\": "
                + filter
                + " } }"
        );
        return search;
    }

    public void testSelectivityOfQueriesWithFiltersIsCountedAndTellsTheRecallApart() throws Exception {
        setUpIndexAndSampling();
        Request settings = new Request("PUT", "/_cluster/settings");
        settings.setJsonEntity("""
            { "persistent": { "xpack.query_sampling.estimate_selectivity": true, "xpack.query_sampling.head_threshold": 1 } }
            """);
        client().performRequest(settings);

        float all = randomFloat();
        float none = randomFloat();
        float unfiltered = randomFloat();
        client().performRequest(filteredKnnSearch(all, "{ \"match_all\": {} }"));
        client().performRequest(filteredKnnSearch(none, "{ \"exists\": { \"field\": \"nothing\" } }"));
        client().performRequest(knnSearch(unfiltered));

        assertBusy(() -> {
            assertThat(storedValue(all, "selectivity"), equalTo("high"));
            assertThat(storedValue(none, "selectivity"), equalTo("low"));
            assertThat(storedValue(unfiltered, "selectivity"), equalTo("unfiltered"));
        });

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
            List<?> selectivities = estimate.evaluate("by_selectivity");
            List<String> names = new ArrayList<>();
            for (int i = 0; i < selectivities.size(); i++) {
                names.add(estimate.evaluate("by_selectivity." + i + ".selectivity"));
            }
            assertThat(names, hasItem("high"));
            assertThat(names, hasItem("unfiltered"));
            assertThat(((Number) estimate.evaluate("by_selectivity.0.records_with_ground_truth")).intValue(), greaterThanOrEqualTo(1));
        });
    }
}
