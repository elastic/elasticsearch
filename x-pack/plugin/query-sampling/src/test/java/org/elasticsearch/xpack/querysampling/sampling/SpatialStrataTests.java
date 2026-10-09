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

import java.util.Set;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.nullValue;

public class SpatialStrataTests extends ESTestCase {

    private static SpatialStrata balancing(int clusters, double balance) {
        SpatialStrata strata = new SpatialStrata(clusters);
        strata.watch(
            new ClusterSettings(
                Settings.builder().put(QuerySamplingSettings.SPATIAL_BALANCE.getKey(), balance).build(),
                Set.of(QuerySamplingSettings.SPATIAL_BALANCE)
            )
        );
        return strata;
    }

    /**
     * Two clusters, the first made of {@code dense} queries and the second of one.
     */
    private static Stratum[] denseAndSparse(SpatialStrata strata, int dense) {
        Stratum first = strata.assign("vector", new float[] { 0, 0 }); // the first queries of a space are its centroids
        Stratum sparse = strata.assign("vector", new float[] { 50, 50 });
        Stratum last = first;
        for (int i = 1; i < dense; i++) {
            last = strata.assign("vector", new float[] { 0, 0.1f });
        }
        return new Stratum[] { last, sparse };
    }

    public void testWithoutClustersNothingIsAssigned() {
        SpatialStrata strata = balancing(0, 1.0);

        assertThat(strata.assign("vector", new float[] { 1, 2 }), nullValue());
        assertThat(strata.factor(null), equalTo(1.0));
    }

    public void testTheFirstQueriesAreTheCentroidsAndTheOthersJoinTheNearest() {
        SpatialStrata strata = balancing(2, 0.5);

        Stratum a = strata.assign("vector", new float[] { 0, 0 });
        Stratum b = strata.assign("vector", new float[] { 10, 10 });
        Stratum nearA = strata.assign("vector", new float[] { 1, 0 });
        Stratum nearB = strata.assign("vector", new float[] { 9, 11 });

        assertThat(a.cluster(), equalTo(0));
        assertThat(b.cluster(), equalTo(1));
        assertThat(nearA.cluster(), equalTo(0));
        assertThat(nearB.cluster(), equalTo(1));
        assertThat(strata.counts(a.space()), equalTo(new long[] { 2, 2 }));
    }

    public void testCentroidsMoveTowardsTheQueriesThatJoinThem() {
        SpatialStrata strata = balancing(2, 1.0);
        strata.assign("vector", new float[] { 0, 0 });
        strata.assign("vector", new float[] { 100, 0 });
        // joins the first cluster, which then has its centroid at (4, 0)
        strata.assign("vector", new float[] { 8, 0 });

        assertThat("47 from (4, 0), where it is 51 from (0, 0)", strata.assign("vector", new float[] { 51, 0 }).cluster(), equalTo(0));
    }

    public void testNoBalanceMeansNoEffect() {
        SpatialStrata strata = balancing(2, 0.0);
        Stratum[] found = denseAndSparse(strata, 9);

        assertThat(strata.factor(found[0]), equalTo(1.0));
        assertThat(strata.factor(found[1]), equalTo(1.0));
    }

    public void testSparseClustersAreFavouredAndDenseOnesAreNot() {
        SpatialStrata strata = balancing(2, 1.0);
        Stratum[] found = denseAndSparse(strata, 9);

        // 10 queries over 2 clusters is an average of 5
        assertThat(strata.factor(found[0]), closeTo(5.0 / 9, 1e-9));
        assertThat(strata.factor(found[1]), closeTo(5.0, 1e-9));
    }

    public void testBalanceOfOneHalfIsAMiddleWay() {
        SpatialStrata strata = balancing(2, 0.5);
        Stratum sparse = denseAndSparse(strata, 9)[1];

        assertThat(strata.factor(sparse), closeTo(Math.sqrt(5.0), 1e-9));
    }

    public void testFactorIsBounded() {
        SpatialStrata strata = balancing(2, 1.0);
        Stratum sparse = denseAndSparse(strata, 999)[1];

        assertThat(strata.factor(sparse), equalTo(SpatialStrata.MAX_FACTOR));
    }

    public void testBalanceFollowsTheSettingWhenItChanges() {
        SpatialStrata strata = new SpatialStrata(2);
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, Set.of(QuerySamplingSettings.SPATIAL_BALANCE));
        strata.watch(clusterSettings);
        Stratum sparse = denseAndSparse(strata, 9)[1];
        assertThat(strata.factor(sparse), equalTo(1.0));

        clusterSettings.applySettings(Settings.builder().put(QuerySamplingSettings.SPATIAL_BALANCE.getKey(), 1.0).build());

        assertThat(strata.factor(sparse), greaterThan(1.0));
    }

    public void testFieldsAndDimensionsAreSeparateSpaces() {
        SpatialStrata strata = balancing(1, 1.0);

        Stratum a = strata.assign("a", new float[] { 1, 2 });
        Stratum b = strata.assign("b", new float[] { 1, 2 });
        Stratum longer = strata.assign("a", new float[] { 1, 2, 3 });

        assertThat(a.space(), equalTo("a/2"));
        assertThat(b.space(), equalTo("b/2"));
        assertThat(longer.space(), equalTo("a/3"));
        assertThat(strata.counts("a/2"), equalTo(new long[] { 1 }));
    }

    public void testVectorsThatAreNotFiniteAreLeftOut() {
        SpatialStrata strata = balancing(2, 1.0);
        strata.assign("vector", new float[] { 1, 1 });

        assertThat(strata.assign("vector", new float[] { Float.NaN, 1 }), nullValue());
        assertThat(strata.assign("vector", new float[] { 1, Float.POSITIVE_INFINITY }), nullValue());
        assertThat(strata.counts("vector/2"), equalTo(new long[] { 1 }));
    }

    public void testStopsGivingNewSpacesAtALimit() {
        SpatialStrata strata = balancing(1, 1.0);
        for (int i = 0; i < SpatialStrata.MAX_SPACES; i++) {
            assertNotNull(strata.assign("field" + i, new float[] { 1 }));
        }

        assertThat(strata.assign("one-too-many", new float[] { 1 }), nullValue());
        assertNotNull("a space that is known keeps working", strata.assign("field0", new float[] { 1 }));
        assertThat(strata.factor(new Stratum("unknown/1", 0)), lessThan(Double.MAX_VALUE));
    }
}
