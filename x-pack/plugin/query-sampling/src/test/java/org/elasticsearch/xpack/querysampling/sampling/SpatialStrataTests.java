/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.sampling;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.querysampling.QuerySamplingSettings;
import org.elasticsearch.xpack.querysampling.dedup.Stratum;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class SpatialStrataTests extends ESTestCase {

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

    public void testSparseClustersAreFavouredAndDenseOnesAreNot() {
        SpatialStrata strata = balancing(2, 2, 1.0);
        Stratum[] found = denseAndSparse(strata, 9);

        assertThat(strata.factor(found[0]), closeTo(5.0 / 9, 1e-9));
        assertThat(strata.factor(found[1]), closeTo(5.0, 1e-9));
    }

    public void testNoClusterIsGivenBeforeTheWarmupIsOverAndThenEveryWaitingQueryGetsOne() {
        SpatialStrata strata = balancing(2, 4, 1.0);
        float[][] vectors = { { 0, 0 }, { 0, 1 }, { 100, 100 }, { 100, 101 } };
        List<AtomicReference<Stratum>> told = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            told.add(assign(strata, "vec", vectors[i]));
            assertNull("still waiting for the fourth query", told.get(i).get());
        }

        told.add(assign(strata, "vec", vectors[3]));

        for (AtomicReference<Stratum> one : told) {
            assertNotNull("the fit is done, so the queries that waited are told their cluster", one.get());
        }
    }

    public void testQueriesThatAreNearEachOtherShareAClusterAndFarOnesDoNot() {
        SpatialStrata strata = balancing(2, 4, 1.0);
        AtomicReference<Stratum> nearA = assign(strata, "vec", 0, 0);
        AtomicReference<Stratum> nearB = assign(strata, "vec", 0, 1);
        AtomicReference<Stratum> farA = assign(strata, "vec", 100, 100);
        AtomicReference<Stratum> farB = assign(strata, "vec", 100, 101); // the fourth query ends the warmup

        assertThat("near queries share a cluster", nearA.get().cluster(), equalTo(nearB.get().cluster()));
        assertThat("so do far ones", farA.get().cluster(), equalTo(farB.get().cluster()));
        assertThat("the two groups are in different clusters", nearA.get().cluster(), not(equalTo(farA.get().cluster())));

        assertThat(
            "a later query joins the cluster that it is nearest to",
            assign(strata, "vec", 1, 0).get().cluster(),
            equalTo(nearA.get().cluster())
        );
        assertThat(assign(strata, "vec", 99, 100).get().cluster(), equalTo(farA.get().cluster()));
    }

    public void testNothingIsAssignedWithoutClustersOrWithAnEmptyVector() {
        SpatialStrata none = new SpatialStrata(0, 0, () -> new Random(7L));
        assertNull(assign(none, "vec", 1, 1).get());
        assertNull(assign(none, "vec", 2, 2).get());

        SpatialStrata strata = balancing(1, 1, 1.0);
        assertNull(assign(strata, "vec").get());
    }

    public void testVectorsThatAreNotFiniteAreLeftOutAndDoNotCountForTheWarmup() {
        SpatialStrata strata = balancing(1, 2, 1.0);
        AtomicReference<Stratum> first = assign(strata, "vec", 1, 1);

        assertNull(assign(strata, "vec", Float.NaN, 0).get());
        assertNull(assign(strata, "vec", Float.POSITIVE_INFINITY, 0).get());
        assertNull("they did not count: one query still waits for another", first.get());

        AtomicReference<Stratum> second = assign(strata, "vec", 2, 2);
        assertNotNull(first.get());
        assertNotNull(second.get());
    }

    public void testEveryFieldAndNumberOfDimensionsHasASpaceOfItsOwn() {
        SpatialStrata strata = balancing(1, 1, 1.0);

        assertThat(assign(strata, "title", 1, 1).get().space(), equalTo("title/2"));
        assertThat(assign(strata, "title", 1, 1, 1).get().space(), equalTo("title/3"));
        assertThat(assign(strata, "body", 1, 1).get().space(), equalTo("body/2"));
    }

    public void testBeyondTheLimitTheSpaceUsedLeastRecentlyIsDropped() {
        SpatialStrata strata = balancing(1, 2, 1.0); // a space waits for two queries before it tells their cluster
        for (int i = 0; i < SpatialStrata.MAX_SPACES; i++) {
            AtomicReference<Stratum> first = assign(strata, "field" + i, 1);
            AtomicReference<Stratum> second = assign(strata, "field" + i, 2);
            assertNotNull(first.get());
            assertNotNull(second.get());
        }
        assertNotNull("field0 is used, so field1 is the one used least recently", assign(strata, "field0", 3).get());

        AtomicReference<Stratum> beyond = assign(strata, "beyond", 1);
        assertNull("a new space starts with its warmup", beyond.get());
        assign(strata, "beyond", 2);
        assertNotNull("it is grouped, as there is room for it now", beyond.get());

        assertNull("field1 was dropped, so it starts over", assign(strata, "field1", 4).get());
        assertNotNull("field0 was used lately and is still there", assign(strata, "field0", 5).get());
        assertThat("a dropped space has no cluster any more, so its factor is 1", strata.factor(new Stratum("field2/1", 0)), equalTo(1.0));
    }

    public void testTheFactorIsOneWithoutBalanceAClusterOrAKnownSpace() {
        SpatialStrata off = balancing(2, 2, 0.0);
        Stratum[] found = denseAndSparse(off, 9);
        assertThat("no balance asked for", off.factor(found[1]), equalTo(1.0));

        SpatialStrata on = balancing(2, 2, 1.0);
        denseAndSparse(on, 9);
        assertThat(on.factor(null), equalTo(1.0));
        assertThat("a space that this node has not met", on.factor(new Stratum("unknown/2", 0)), equalTo(1.0));
    }

    public void testABarelyKnownClusterIsFavouredOnlyUpToTheCap() {
        SpatialStrata strata = balancing(2, 2, 1.0);

        Stratum[] found = denseAndSparse(strata, 1000); // one sparse query against 999 dense ones: 500 times the average

        assertThat(strata.factor(found[1]), equalTo(SpatialStrata.MAX_FACTOR));
    }
}
