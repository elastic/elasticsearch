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
import java.util.Locale;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Promotes stored sampled queries to the golden dataset against a real cluster: a version is made, it is complete and has
 * the queries with their ground truth and the state of the data it was computed on, and the next promotion makes another
 * version and leaves this one as it was.
 */
public class QuerySamplingGoldenIT extends QuerySamplingRestTestCase {

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

    private ObjectPath searchGolden(String kind, long version) throws IOException {
        Request refresh = new Request("POST", "/.query_golden/_refresh");
        refresh.setOptions(systemIndexAccess());
        client().performRequest(refresh);
        Request search = new Request("GET", "/.query_golden/_search?size=1000");
        search.setOptions(systemIndexAccess());
        search.setJsonEntity(String.format(Locale.ROOT, """
            { "query": { "bool": { "filter": [ { "term": { "kind": "%s" } }, { "term": { "dataset_version": %d } } ] } } }
            """, kind, version));
        return ObjectPath.createFromResponse(client().performRequest(search));
    }

    public void testStoredQueriesArePromotedToVersionsThatStayAsTheyWere() throws Exception {
        setUpIndexAndSampling();
        float x = sampleOneQuery();
        // the index keeps what was sampled between tests, so ask until nothing is left to compute
        long last;
        int calls = 0;
        do {
            refreshSampleIndex();
            last = ((Number) ObjectPath.createFromResponse(client().performRequest(new Request("POST", "/_query_sampling/ground_truth")))
                .evaluate("computed")).longValue();
        } while (last > 0 && ++calls < 100);
        assertBusy(() -> assertThat(storedValue(x, "has_ground_truth"), equalTo(true)));
        refreshSampleIndex();

        ObjectPath first = ObjectPath.createFromResponse(client().performRequest(new Request("POST", "/_query_sampling/golden/promote")));

        assertThat(((Number) first.evaluate("version")).longValue(), equalTo(1L));
        int promoted = ((Number) first.evaluate("promoted")).intValue();
        assertThat(promoted, greaterThanOrEqualTo(1));
        assertThat(((Number) first.evaluate("failed")).intValue(), equalTo(0));
        ObjectPath manifest = searchGolden("version", 1);
        assertThat(((Number) manifest.evaluate("hits.total.value")).intValue(), equalTo(1));
        assertThat(manifest.evaluate("hits.hits.0._source.completed"), equalTo(true));
        assertThat(((Number) manifest.evaluate("hits.hits.0._source.records")).intValue(), equalTo(promoted));
        ObjectPath records = searchGolden("record", 1);
        assertThat(((Number) records.evaluate("hits.total.value")).intValue(), equalTo(promoted));
        // a copy of what was stored, with the state of the data its ground truth is of: the five documents of the index
        assertThat(((Number) records.evaluate("hits.hits.0._source.ground_truth.data_state.documents")).longValue(), equalTo(5L));
        assertNotNull(records.evaluate("hits.hits.0._source.query.query_vector"));

        ObjectPath second = ObjectPath.createFromResponse(client().performRequest(new Request("POST", "/_query_sampling/golden/promote")));

        assertThat(((Number) second.evaluate("version")).longValue(), equalTo(2L));
        assertThat(
            "the first version is as it was",
            ((Number) searchGolden("record", 1).evaluate("hits.total.value")).intValue(),
            equalTo(promoted)
        );
        assertThat(((Number) searchGolden("record", 2).evaluate("hits.total.value")).intValue(), equalTo(promoted));
    }

    public void testBoundsOfTheRequestAreChecked() {
        Request request = new Request("POST", "/_query_sampling/golden/promote");
        request.addParameter("max", "0");

        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));

        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
    }
}
