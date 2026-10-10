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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

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

    /**
     * What a query is told about its cluster, which can be nothing yet. Read it after the clusters are fitted.
     */
    private static AtomicReference<Stratum> assign(SpatialStrata strata, String field, float... vector) {
        AtomicReference<Stratum> told = new AtomicReference<>();
        strata.assign(field, vector, told::set);
        return told;
    }

    /**
     * Queries of two groups that are far from each other, of {@code dense} and one queries. The first query of each is
     * the first two of the warm-up of 2, so the clusters are fitted when the second is in.
     *
     * @return what the last query of the large group was told, and what the other was
     */
    private static Stratum[] denseAndSparse(SpatialStrata strata, int dense) {
        AtomicReference<Stratum> first = assign(strata, "vector", 0, 0);
        AtomicReference<Stratum> sparse = assign(strata, "vector", 50, 50);
        AtomicReference<Stratum> last = first;
        for (int i = 1; i < dense; i++) {
            last = assign(strata, "vector", 0, 0.1f);
        }
        return new Stratum[] { last.get(), sparse.get() };
    }

    public void testWithoutClustersNothingIsAssignedAndNoRandomnessIsNeeded() {
        SpatialStrata strata = new SpatialStrata(0, 200, () -> { throw new AssertionError("nothing to fit"); });

        assertThat(assign(strata, "vector", 1, 2).get(), nullValue());
        assertThat(strata.factor(null), equalTo(1.0));
    }

    public void testQueriesWaitForTheClustersAndAreToldAllAtOnce() {
        SpatialStrata strata = balancing(2, 4, 0.5);

        AtomicReference<Stratum> first = assign(strata, "vector", 0, 0);
        AtomicReference<Stratum> second = assign(strata, "vector", 100, 100);
        AtomicReference<Stratum> third = assign(strata, "vector", 0, 1);
        assertThat("not told yet", first.get(), nullValue());
        assertThat(second.get(), nullValue());
        assertThat(strata.counts("vector/2").length, equalTo(0));

        AtomicReference<Stratum> fourth = assign(strata, "vector", 101, 100);

        assertThat(first.get(), equalTo(third.get()));
        assertThat(second.get(), equalTo(fourth.get()));
        assertThat(first.get(), not(equalTo(second.get())));
        assertThat(strata.counts("vector/2"), equalTo(new long[] { 2, 2 }));
    }

    /**
     * The queries of a space can be very much alike, as embeddings are. The clusters have to tell the groups that there
     * are apart even then, whichever queries come first, and not have all the queries nearest to one of them.
     */
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
            // groups are 100 apart on one axis, and the queries of a group are within 1 of each other on the others
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

    public void testNewQueriesJoinTheNearestClusterAndMoveItsCentroid() {
        SpatialStrata strata = balancing(2, 2, 1.0);
        AtomicReference<Stratum> a = assign(strata, "vector", 0, 0);
        AtomicReference<Stratum> b = assign(strata, "vector", 100, 0);

        // joins the first cluster, whose centroid then moves to (4, 0)
        assertThat(assign(strata, "vector", 8, 0).get(), equalTo(a.get()));

        assertThat("47 from (4, 0), where it is 51 from (0, 0)", assign(strata, "vector", 51, 0).get(), equalTo(a.get()));
        assertThat(assign(strata, "vector", 99, 1).get(), equalTo(b.get()));
        assertThat(Arrays.stream(strata.counts("vector/2")).sum(), equalTo(5L));
    }

    public void testNoBalanceMeansNoEffect() {
        SpatialStrata strata = balancing(2, 2, 0.0);
        Stratum[] found = denseAndSparse(strata, 9);

        assertThat(strata.factor(found[0]), equalTo(1.0));
        assertThat(strata.factor(found[1]), equalTo(1.0));
    }

    public void testSparseClustersAreFavouredAndDenseOnesAreNot() {
        SpatialStrata strata = balancing(2, 2, 1.0);
        Stratum[] found = denseAndSparse(strata, 9);

        // 10 queries over 2 clusters is an average of 5
        assertThat(strata.factor(found[0]), closeTo(5.0 / 9, 1e-9));
        assertThat(strata.factor(found[1]), closeTo(5.0, 1e-9));
    }

    public void testBalanceOfOneHalfIsAMiddleWay() {
        SpatialStrata strata = balancing(2, 2, 0.5);

        assertThat(strata.factor(denseAndSparse(strata, 9)[1]), closeTo(Math.sqrt(5.0), 1e-9));
    }

    public void testFactorIsBounded() {
        SpatialStrata strata = balancing(2, 2, 1.0);

        assertThat(strata.factor(denseAndSparse(strata, 999)[1]), equalTo(SpatialStrata.MAX_FACTOR));
    }

    public void testBalanceFollowsTheSettingWhenItChanges() {
        SpatialStrata strata = new SpatialStrata(2, 2, () -> new Random(7L));
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, Set.of(QuerySamplingSettings.SPATIAL_BALANCE));
        strata.watch(clusterSettings);
        Stratum sparse = denseAndSparse(strata, 9)[1];
        assertThat(strata.factor(sparse), equalTo(1.0));

        clusterSettings.applySettings(Settings.builder().put(QuerySamplingSettings.SPATIAL_BALANCE.getKey(), 1.0).build());

        assertThat(strata.factor(sparse), greaterThan(1.0));
    }

    public void testFieldsAndDimensionsAreSeparateSpaces() {
        SpatialStrata strata = balancing(1, 1, 1.0);

        Stratum a = assign(strata, "a", 1, 2).get();
        Stratum b = assign(strata, "b", 1, 2).get();
        Stratum longer = assign(strata, "a", 1, 2, 3).get();

        assertThat(a.space(), equalTo("a/2"));
        assertThat(b.space(), equalTo("b/2"));
        assertThat(longer.space(), equalTo("a/3"));
        assertThat(strata.counts("a/2"), equalTo(new long[] { 1 }));
    }

    public void testVectorsThatAreNotFiniteAreLeftOut() {
        SpatialStrata strata = balancing(2, 2, 1.0);
        assign(strata, "vector", 1, 1);

        assertThat(assign(strata, "vector", Float.NaN, 1).get(), nullValue());
        assertThat(assign(strata, "vector", 1, Float.POSITIVE_INFINITY).get(), nullValue());
        assertThat("they do not count towards the clusters either", strata.counts("vector/2").length, equalTo(0));
        assertNotNull(assign(strata, "vector", 3, 3).get());
        assertThat(strata.counts("vector/2"), equalTo(new long[] { 1, 1 }));
    }

    public void testQueriesThatAreAllTheSameAreStillTold() {
        SpatialStrata strata = balancing(2, 4, 1.0);
        List<AtomicReference<Stratum>> told = new ArrayList<>();

        for (int i = 0; i < 4; i++) {
            told.add(assign(strata, "vector", 1, 1));
        }

        told.forEach(reference -> assertNotNull(reference.get()));
        assertThat(Arrays.stream(strata.counts("vector/2")).sum(), equalTo(4L));
    }

    public void testStopsGivingNewSpacesAtALimit() {
        SpatialStrata strata = balancing(1, 1, 1.0);
        for (int i = 0; i < SpatialStrata.MAX_SPACES; i++) {
            assertNotNull(assign(strata, "field" + i, 1).get());
        }

        assertThat(assign(strata, "one-too-many", 1).get(), nullValue());
        assertNotNull("a space that is known keeps working", assign(strata, "field0", 1).get());
        assertThat(strata.factor(new Stratum("unknown/1", 0)), lessThan(Double.MAX_VALUE));
    }
}
