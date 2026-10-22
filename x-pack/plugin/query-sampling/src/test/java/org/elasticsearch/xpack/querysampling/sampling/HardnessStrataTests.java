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
import org.elasticsearch.xpack.querysampling.capture.CapturedSearch;
import org.elasticsearch.xpack.querysampling.dedup.Hardness;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

public class HardnessStrataTests extends ESTestCase {

    /** The best hit scores 1, and the other two {@code other}, so the contrast is {@code (1 + 2 * other) / 3}. */
    private static List<CapturedSearch.Hit> hits(float other) {
        return List.of(
            new CapturedSearch.Hit("idx", "a", 1f),
            new CapturedSearch.Hit("idx", "b", other),
            new CapturedSearch.Hit("idx", "c", other)
        );
    }

    public void testThereIsNoContrastForTooFewHitsOrAZeroScore() {
        assertThat(HardnessStrata.contrast(hits(0.5f).subList(0, 2)), equalTo(Double.NaN));
        assertThat(HardnessStrata.contrast(hits(0f)), equalTo(Double.NaN));
        assertThat(HardnessStrata.contrast(hits(0.5f)), closeTo(2.0 / 3, 1e-6));
    }

    public void testTheFirstQueriesOfASpaceAreMediumAndLaterOnesAreSortedAgainstThem() {
        HardnessStrata strata = new HardnessStrata();
        List<Hardness> first = new ArrayList<>();
        for (int i = 0; i < HardnessStrata.MIN_QUERIES; i++) {
            first.add(strata.assign("vec", 8, hits(0.4f + 0.2f * i / HardnessStrata.MIN_QUERIES)));
        }

        assertThat(first, equalTo(Collections.nCopies(HardnessStrata.MIN_QUERIES, Hardness.MEDIUM)));
        assertThat("scores all as good as the first", strata.assign("vec", 8, hits(1f)), equalTo(Hardness.HARD));
        assertThat("scores far below the first", strata.assign("vec", 8, hits(0.01f)), equalTo(Hardness.EASY));
        assertThat("another space starts again", strata.assign("other", 8, hits(1f)), equalTo(Hardness.MEDIUM));
    }

    public void testTheFactorIsOneWithoutATiltAndAveragesOneWithIt() {
        HardnessStrata strata = new HardnessStrata();
        for (Hardness hardness : Hardness.values()) {
            assertThat(strata.factor(hardness), equalTo(1.0));
        }

        strata.watch(
            new ClusterSettings(
                Settings.builder().put(QuerySamplingSettings.HARDNESS_TILT.getKey(), 1.0).build(),
                Set.of(QuerySamplingSettings.HARDNESS_TILT)
            )
        );

        assertThat(strata.factor(Hardness.EASY), lessThan(strata.factor(Hardness.MEDIUM)));
        assertThat(strata.factor(Hardness.HARD), greaterThan(strata.factor(Hardness.MEDIUM)));
        double average = (strata.factor(Hardness.EASY) + strata.factor(Hardness.MEDIUM) + strata.factor(Hardness.HARD)) / 3;
        assertThat(average, closeTo(1.0, 1e-9));
    }
}
