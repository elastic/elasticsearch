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

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;

/**
 * How much of the vectors the filters of a query leave, counted against a real cluster: it is stored with the sampled
 * query, and is what the recall is told apart by.
 */
public class QuerySamplingSelectivityIT extends QuerySamplingRestTestCase {

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
        // every captured query is picked, so that what is stored does not depend on a coin flip
        Request settings = new Request("PUT", "/_cluster/settings");
        settings.setJsonEntity("""
            { "persistent": { "xpack.query_sampling.estimate_selectivity": true, "xpack.query_sampling.head_threshold": 1 } }
            """);
        client().performRequest(settings);

        float all = randomFloat();
        float none = randomFloat();
        float unfiltered = randomFloat();
        client().performRequest(filteredKnnSearch(all, "{ \"match_all\": {} }")); // every vector passes
        client().performRequest(filteredKnnSearch(none, "{ \"exists\": { \"field\": \"nothing\" } }")); // no vector has this field
        client().performRequest(knnSearch(unfiltered));

        assertBusy(() -> {
            assertThat(storedValue(all, "selectivity"), equalTo("high"));
            assertThat(storedValue(none, "selectivity"), equalTo("low"));
            assertThat(storedValue(unfiltered, "selectivity"), equalTo("unfiltered"));
        });

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
