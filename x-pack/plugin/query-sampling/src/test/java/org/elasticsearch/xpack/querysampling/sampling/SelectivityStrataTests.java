/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.sampling;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.querysampling.QuerySamplingSettings;
import org.elasticsearch.xpack.querysampling.capture.CapturedQuery;
import org.elasticsearch.xpack.querysampling.dedup.MultiplicityTracker;
import org.elasticsearch.xpack.querysampling.dedup.QueryFingerprint;
import org.elasticsearch.xpack.querysampling.dedup.Selectivity;
import org.elasticsearch.xpack.querysampling.dedup.TrackedQuery;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.equalTo;

public class SelectivityStrataTests extends ESTestCase {

    private static final long VECTORS = 1000;

    private final AtomicLong now = new AtomicLong();
    private final MultiplicityTracker tracker = new MultiplicityTracker(1000);
    private long passing = 100;

    private SelectivityStrata strata() {
        SelectivityStrata strata = new SelectivityStrata(this::answer, now::get, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        enableEstimate(strata);
        return strata;
    }

    private static void enableEstimate(SelectivityStrata strata) {
        strata.watch(
            new ClusterSettings(
                Settings.builder().put(QuerySamplingSettings.ESTIMATE_SELECTIVITY.getKey(), true).build(),
                Set.of(QuerySamplingSettings.ESTIMATE_SELECTIVITY)
            )
        );
    }

    private void answer(SearchRequest request, ActionListener<SearchResponse> listener) {
        boolean all = request.source().query() instanceof ExistsQueryBuilder;
        SearchHits hits = SearchHits.empty(new TotalHits(all ? VECTORS : passing, TotalHits.Relation.EQUAL_TO), 1f);
        SearchResponse response = SearchResponseUtils.successfulResponse(hits);
        hits.decRef();
        try {
            listener.onResponse(response);
        } finally {
            response.decRef();
        }
    }

    private static CapturedQuery filtered(long id) {
        return new CapturedQuery(
            new String[] { "idx" },
            "vec",
            new float[] { id },
            10,
            100,
            null,
            null,
            List.of(QueryBuilders.termQuery("category", id)),
            null
        );
    }

    private TrackedQuery tracked(long id) {
        return tracker.record(new QueryFingerprint(id, id));
    }

    public void testTheShareOfTheVectorsThatPassTheFiltersTellsTheSelectivity() {
        SelectivityStrata strata = strata();
        long[][] cases = {
            { 600, Selectivity.HIGH.ordinal() },
            { 500, Selectivity.HIGH.ordinal() },
            { 499, Selectivity.MEDIUM.ordinal() },
            { 50, Selectivity.MEDIUM.ordinal() },
            { 49, Selectivity.LOW.ordinal() },
            { 0, Selectivity.LOW.ordinal() } };
        long id = 1;
        for (long[] oneCase : cases) {
            passing = oneCase[0];
            TrackedQuery tracked = tracked(id);

            strata.assign(tracked, filtered(id++));

            assertThat("passing " + oneCase[0], tracked.selectivity(), equalTo(Selectivity.values()[(int) oneCase[1]]));
        }
        assertThat(strata.counted(), equalTo((long) cases.length));
    }
}
