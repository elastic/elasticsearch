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

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class QuerySamplingGoldenScheduleIT extends QuerySamplingRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("x-pack-query-sampling")
        .module("reindex")
        .feature(FeatureFlag.QUERY_SAMPLING)
        .setting("xpack.security.enabled", "false")
        .setting("xpack.query_sampling.golden_promotion_interval", "1s")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    private static void computeAllGroundTruth() throws IOException {
        long last;
        int calls = 0;
        do {
            refreshSampleIndex();
            last = ((Number) ObjectPath.createFromResponse(client().performRequest(new Request("POST", "/_query_sampling/ground_truth")))
                .evaluate("computed")).longValue();
        } while (last > 0 && ++calls < 100);
    }

    private static ObjectPath completedVersions() throws IOException {
        Request refresh = new Request("POST", "/.query_golden/_refresh");
        refresh.setOptions(systemIndexAccess());
        try {
            client().performRequest(refresh);
        } catch (ResponseException e) {
            return null;
        }
        Request search = new Request("GET", "/.query_golden/_search?size=100&sort=dataset_version:asc");
        search.setOptions(systemIndexAccess());
        search.setJsonEntity("""
            { "query": { "bool": { "filter": [ { "term": { "kind": "version" } }, { "term": { "completed": true } } ] } } }
            """);
        return ObjectPath.createFromResponse(client().performRequest(search));
    }

    public void testTheGoldenDatasetGrowsByItself() throws Exception {
        setUpIndexAndSampling();
        sampleOneQuery();
        computeAllGroundTruth();

        assertBusy(() -> {
            ObjectPath versions = completedVersions();
            assertNotNull("a version was made", versions);
            assertThat(((Number) versions.evaluate("hits.total.value")).intValue(), greaterThanOrEqualTo(1));
        });
        ObjectPath first = completedVersions();
        int firstRecords = ((Number) first.evaluate("hits.hits.0._source.records")).intValue();
        assertThat(firstRecords, greaterThanOrEqualTo(1));
        assertNull("the first is not made from another", first.evaluate("hits.hits.0._source.source_version"));
        long firstVersion = ((Number) first.evaluate("hits.hits.0._source.dataset_version")).longValue();

        sampleOneQuery();
        computeAllGroundTruth();
        assertBusy(() -> {
            ObjectPath versions = completedVersions();
            List<?> hits = versions.evaluate("hits.hits");
            assertThat(hits.size(), greaterThan(1));
            Object latestSource = versions.evaluate("hits.hits." + (hits.size() - 1) + "._source.source_version");
            assertNotNull("the latest is made from the one before", latestSource);
            int latestRecords = ((Number) versions.evaluate("hits.hits." + (hits.size() - 1) + "._source.records")).intValue();
            assertThat("it has what was there and what is new", latestRecords, greaterThan(firstRecords));
        });
        assertThat(((Number) completedVersions().evaluate("hits.hits.0._source.records")).intValue(), equalTo(firstRecords));
        assertThat(((Number) completedVersions().evaluate("hits.hits.0._source.dataset_version")).longValue(), equalTo(firstVersion));
    }
}
