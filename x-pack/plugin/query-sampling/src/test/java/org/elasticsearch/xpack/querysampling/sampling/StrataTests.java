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
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.querysampling.QuerySamplingSettings;
import org.elasticsearch.xpack.querysampling.capture.CapturedQuery;
import org.elasticsearch.xpack.querysampling.dedup.Hardness;
import org.elasticsearch.xpack.querysampling.dedup.MultiplicityTracker;
import org.elasticsearch.xpack.querysampling.dedup.QueryFingerprint;
import org.elasticsearch.xpack.querysampling.dedup.Selectivity;
import org.elasticsearch.xpack.querysampling.dedup.Stratum;
import org.elasticsearch.xpack.querysampling.dedup.TrackedQuery;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

public class StrataTests extends ESTestCase {

    private static final long VECTORS = 1000;

    private final AtomicLong now = new AtomicLong();
    private final MultiplicityTracker tracker = new MultiplicityTracker(1000);
    private long passing = 100;

    private static SpatialStrata balancing(int clusters, int warmup, double balance) {
        SpatialStrata strata = new SpatialStrata(clusters, warmup, () -> new Random(7L));
        strata.watch(
            new ClusterSettings(
                Settings.builder().put(QuerySamplingSettings.SPATIAL_BALANCE.getKey(), balance).build(),
                Set.of(QuerySamplingSettings.SPATIAL_BALANCE)
            )
        );
        return strata;
    }

    private static AtomicReference<Stratum> assign(SpatialStrata strata, String field, float... vector) {
        AtomicReference<Stratum> told = new AtomicReference<>();
        strata.assign(field, vector, told::set);
        return told;
    }

    private static Stratum[] denseAndSparse(SpatialStrata strata, int dense) {
        AtomicReference<Stratum> first = assign(strata, "vector", 0, 0);
        AtomicReference<Stratum> sparse = assign(strata, "vector", 50, 50);
        AtomicReference<Stratum> last = first;
        for (int i = 1; i < dense; i++) {
            last = assign(strata, "vector", 0, 0.1f);
        }
        return new Stratum[] { last.get(), sparse.get() };
    }

    public void testGroupsThatAreFarApartAreInClustersOfTheirOwn() {
        int[] sizes = { 100, 50, 40, 10 };
        SpatialStrata strata = balancing(4, 200, 0.5);
        Random random = new Random(randomLong());
        List<List<AtomicReference<Stratum>>> told = new ArrayList<>();
        List<Integer> arrivals = new ArrayList<>();
        for (int group = 0; group < sizes.length; group++) {
            told.add(new ArrayList<>());
            for (int i = 0; i < sizes[group]; i++) {
                arrivals.add(group);
            }
        }
        Collections.shuffle(arrivals, random);
        for (int group : arrivals) {
            told.get(group).add(assign(strata, "vector", 100f * group + random.nextFloat(), random.nextFloat(), random.nextFloat()));
        }

        long[] counts = strata.counts("vector/3");
        Arrays.sort(counts);
        assertThat(counts, equalTo(new long[] { 10, 40, 50, 100 }));
        for (int group = 0; group < sizes.length; group++) {
            Set<Stratum> clusters = new HashSet<>();
            told.get(group).forEach(reference -> clusters.add(reference.get()));
            assertThat("group " + group + " is in one cluster", clusters.size(), equalTo(1));
        }
    }

    public void testSparseClustersAreFavouredAndDenseOnesAreNot() {
        SpatialStrata strata = balancing(2, 2, 1.0);
        Stratum[] found = denseAndSparse(strata, 9);

        assertThat(strata.factor(found[0]), closeTo(5.0 / 9, 1e-9));
        assertThat(strata.factor(found[1]), closeTo(5.0, 1e-9));
    }

    private static HardnessStrata tilting(double tilt) {
        HardnessStrata strata = new HardnessStrata();
        strata.watch(
            new ClusterSettings(
                Settings.builder().put(QuerySamplingSettings.HARDNESS_TILT.getKey(), tilt).build(),
                Set.of(QuerySamplingSettings.HARDNESS_TILT)
            )
        );
        return strata;
    }

    public void testHardQueriesAreFavouredAndEasyOnesAreNot() {
        HardnessStrata strata = tilting(1.0);

        assertThat(strata.factor(Hardness.HARD), greaterThan(1.0));
        assertThat(strata.factor(Hardness.EASY), lessThan(1.0));
        assertThat(strata.factor(Hardness.HARD) / strata.factor(Hardness.MEDIUM), closeTo(Math.E, 1e-9));
        assertThat(strata.factor(Hardness.MEDIUM) / strata.factor(Hardness.EASY), closeTo(Math.E, 1e-9));
    }

    private SelectivityStrata strata() {
        SelectivityStrata strata = new SelectivityStrata(this::answer, now::get);
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
