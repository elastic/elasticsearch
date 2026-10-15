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

import java.io.IOException;
import java.util.Locale;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.not;

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

    public void testTheDatasetIsBroughtUpToDateByVersionsThatStayAsTheyWere() throws Exception {
        setUpIndexAndSampling();
        float x = sampleOneQuery();
        long last;
        int calls = 0;
        do {
            refreshSampleIndex();
            last = ((Number) ObjectPath.createFromResponse(client().performRequest(new Request("POST", "/_query_sampling/ground_truth")))
                .evaluate("computed")).longValue();
        } while (last > 0 && ++calls < 100);
        assertBusy(() -> assertThat(storedValue(x, "has_ground_truth"), equalTo(true)));
        refreshSampleIndex();

        ObjectPath first = update();
        assertThat(((Number) first.evaluate("version")).longValue(), equalTo(1L));
        int added = ((Number) first.evaluate("added")).intValue();
        assertThat(added, greaterThanOrEqualTo(1));
        assertNull("it is not made from another", first.evaluate("source_version"));
        assertThat(((Number) first.evaluate("failed")).intValue(), equalTo(0));
        ObjectPath manifest = searchGolden("version", 1);
        assertThat(((Number) manifest.evaluate("hits.total.value")).intValue(), equalTo(1));
        assertThat(manifest.evaluate("hits.hits.0._source.completed"), equalTo(true));
        assertThat(((Number) manifest.evaluate("hits.hits.0._source.records")).intValue(), equalTo(added));
        ObjectPath records = searchGolden("record", 1);
        assertThat(((Number) records.evaluate("hits.total.value")).intValue(), equalTo(added));
        assertThat(((Number) records.evaluate("hits.hits.0._source.ground_truth.data_state.documents")).longValue(), equalTo(5L));
        assertNotNull(records.evaluate("hits.hits.0._source.query.query_vector"));

        ObjectPath nothing = update();
        assertNull(nothing.evaluate("version"));
        assertThat(((Number) nothing.evaluate("copied")).intValue(), equalTo(added));

        ObjectPath fresh = check("?version=1");
        assertThat(((Number) fresh.evaluate("version")).longValue(), equalTo(1L));
        assertThat(((Number) fresh.evaluate("checked")).intValue(), equalTo(added));
        assertThat(((Number) fresh.evaluate("fresh")).intValue(), equalTo(added));
        assertThat(((Number) fresh.evaluate("stale")).intValue(), equalTo(0));

        Request change = new Request("PUT", "/vectors/_doc/0");
        change.setJsonEntity("{\"vec\": [9, 9]}");
        client().performRequest(change);
        client().performRequest(new Request("POST", "/vectors/_refresh"));
        ObjectPath stale = check("");
        assertThat("the latest version, when none is asked for", ((Number) stale.evaluate("version")).longValue(), equalTo(1L));
        assertThat(((Number) stale.evaluate("stale")).intValue(), equalTo(added));
        assertThat(((Number) stale.evaluate("fresh")).intValue(), equalTo(0));

        ObjectPath updated = update();
        assertThat(((Number) updated.evaluate("source_version")).longValue(), equalTo(1L));
        assertThat(((Number) updated.evaluate("version")).longValue(), equalTo(2L));
        assertThat(((Number) updated.evaluate("recomputed")).intValue(), equalTo(added));
        assertThat(((Number) updated.evaluate("copied")).intValue(), equalTo(0));
        assertThat(((Number) updated.evaluate("failed")).intValue(), equalTo(0));
        assertThat(((Number) searchGolden("version", 2).evaluate("hits.hits.0._source.source_version")).longValue(), equalTo(1L));
        long digestOfVersion2 = ((Number) searchGolden("record", 2).evaluate("hits.hits.0._source.ground_truth.data_state.digest"))
            .longValue();
        long digestOfVersion1 = ((Number) searchGolden("record", 1).evaluate("hits.hits.0._source.ground_truth.data_state.digest"))
            .longValue();
        assertThat("the state is of the data as it was", digestOfVersion2, not(equalTo(digestOfVersion1)));
        ObjectPath freshAgain = check("");
        assertThat(((Number) freshAgain.evaluate("version")).longValue(), equalTo(2L));
        assertThat(((Number) freshAgain.evaluate("fresh")).intValue(), equalTo(added));
    }

    private static ObjectPath update() throws IOException {
        return ObjectPath.createFromResponse(client().performRequest(new Request("POST", "/_query_sampling/golden/update")));
    }

    private static ObjectPath check(String parameters) throws IOException {
        return ObjectPath.createFromResponse(client().performRequest(new Request("GET", "/_query_sampling/golden/check" + parameters)));
    }
}
