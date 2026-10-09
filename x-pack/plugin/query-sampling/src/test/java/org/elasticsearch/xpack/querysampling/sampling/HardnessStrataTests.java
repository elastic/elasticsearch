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

import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.nullValue;

public class HardnessStrataTests extends ESTestCase {

    private static List<CapturedSearch.Hit> hits(float... scores) {
        return IntStream.range(0, scores.length).mapToObj(i -> new CapturedSearch.Hit("idx", "doc" + i, scores[i])).toList();
    }

    /**
     * Hits of a first one of 1 and others of the same score, which have a contrast of {@code (1 + 2x) / 3}.
     */
    private static List<CapturedSearch.Hit> hitsWithContrast(double contrast) {
        float others = (float) ((3 * contrast - 1) / 2);
        return hits(1f, others, others);
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

    /**
     * Gives the strata queries of contrasts that are spread evenly from 0.4 to 0.9, which tells it what is usual.
     */
    private static void learn(HardnessStrata strata, String field, int queries) {
        for (int i = 0; i < queries; i++) {
            strata.assign(field, 2, hitsWithContrast(0.4 + 0.5 * i / queries));
        }
    }

    public void testContrastIsOneWhenAllHitsAreAsGoodAsTheFirstAndSmallerWhenTheyAreNot() {
        assertThat(HardnessStrata.contrast(hits(1f, 1f, 1f)), closeTo(1.0, 1e-9));
        assertThat(HardnessStrata.contrast(hits(1f, 0.5f, 0.25f)), closeTo(1.75 / 3, 1e-9));
        assertThat("it is the best that counts, not the first", HardnessStrata.contrast(hits(0.5f, 1f, 0.25f)), closeTo(1.75 / 3, 1e-9));
    }

    public void testThereIsNoContrastWithoutAReasonableAnswer() {
        assertTrue(Double.isNaN(HardnessStrata.contrast(List.of())));
        assertTrue("too few hits", Double.isNaN(HardnessStrata.contrast(hits(1f, 0.5f))));
        assertTrue("a score of zero", Double.isNaN(HardnessStrata.contrast(hits(1f, 0.5f, 0f))));
        assertTrue("a negative score", Double.isNaN(HardnessStrata.contrast(hits(1f, 0.5f, -1f))));
        assertTrue("not a number", Double.isNaN(HardnessStrata.contrast(hits(1f, Float.NaN, 0.5f))));
    }

    public void testQueriesAreMediumUntilThereAreEnoughToTellThemApart() {
        HardnessStrata strata = tilting(1.0);

        for (int i = 0; i < HardnessStrata.MIN_QUERIES; i++) {
            assertThat(strata.assign("vec", 2, hitsWithContrast(0.4 + 0.5 * i / HardnessStrata.MIN_QUERIES)), equalTo(Hardness.MEDIUM));
        }
    }

    public void testQueriesAreToldApartByHowFarTheyAreFromTheOthers() {
        HardnessStrata strata = tilting(1.0);
        learn(strata, "vec", 100);

        assertThat(strata.assign("vec", 2, hitsWithContrast(0.95)), equalTo(Hardness.HARD));
        assertThat(strata.assign("vec", 2, hitsWithContrast(0.35)), equalTo(Hardness.EASY));
        assertThat(strata.assign("vec", 2, hitsWithContrast(0.65)), equalTo(Hardness.MEDIUM));
    }

    /**
     * The thirds are those of a normal distribution, so that is what the queries of this test are made to have. They
     * come in a random order: queries that get steadily harder would all be harder than the average of those before.
     */
    public void testAboutAThirdOfTheQueriesAreInEachBucket() {
        HardnessStrata strata = tilting(1.0);
        Random random = new Random(randomLong());
        int queries = 600;
        int[] counts = new int[Hardness.values().length];
        for (int i = 0; i < 500 + queries; i++) {
            double contrast = Math.min(1.0, Math.max(0.34, 0.65 + 0.1 * random.nextGaussian()));
            Hardness hardness = strata.assign("vec", 2, hitsWithContrast(contrast));
            if (i >= 500) {
                counts[hardness.ordinal()]++;
            }
        }

        for (int count : counts) {
            assertThat((double) count / queries, closeTo(1.0 / 3, 0.08));
        }
    }

    public void testQueriesThatAreAllTheSameAreMedium() {
        HardnessStrata strata = tilting(1.0);
        for (int i = 0; i < 100; i++) {
            assertThat(strata.assign("vec", 2, hitsWithContrast(0.7)), equalTo(Hardness.MEDIUM));
        }
    }

    public void testFieldsAndDimensionsAreSeparateSpaces() {
        HardnessStrata strata = tilting(1.0);
        learn(strata, "a", 100);

        assertThat("it knows nothing of this field", strata.assign("b", 2, hitsWithContrast(0.95)), equalTo(Hardness.MEDIUM));
        assertThat("nor of these dimensions", strata.assign("a", 3, hitsWithContrast(0.95)), equalTo(Hardness.MEDIUM));
        assertThat(strata.assign("a", 2, hitsWithContrast(0.95)), equalTo(Hardness.HARD));
    }

    public void testQueriesWithoutAContrastAreLeftOut() {
        HardnessStrata strata = tilting(1.0);

        assertThat(strata.assign("vec", 2, hits(1f, 0.5f)), nullValue());
        assertThat(strata.factor(null), equalTo(1.0));
    }

    public void testNoTiltMeansNoEffect() {
        HardnessStrata strata = tilting(0.0);

        for (Hardness hardness : Hardness.values()) {
            assertThat(strata.factor(hardness), equalTo(1.0));
        }
    }

    public void testHardQueriesAreFavouredAndEasyOnesAreNot() {
        HardnessStrata strata = tilting(1.0);

        assertThat(strata.factor(Hardness.HARD), greaterThan(1.0));
        assertThat(strata.factor(Hardness.EASY), lessThan(1.0));
        assertThat(strata.factor(Hardness.HARD) / strata.factor(Hardness.MEDIUM), closeTo(Math.E, 1e-9));
        assertThat(strata.factor(Hardness.MEDIUM) / strata.factor(Hardness.EASY), closeTo(Math.E, 1e-9));
    }

    public void testAThirdOfEachGivesAnAverageOfOne() {
        for (double tilt : new double[] { 0.1, 1.0, 3.0, 10.0 }) {
            HardnessStrata strata = tilting(tilt);
            double sum = 0;
            for (Hardness hardness : Hardness.values()) {
                sum += strata.factor(hardness);
            }
            assertThat("tilt " + tilt, sum / 3, closeTo(1.0, 1e-9));
        }
    }

    public void testTiltFollowsTheSettingWhenItChanges() {
        HardnessStrata strata = new HardnessStrata();
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, Set.of(QuerySamplingSettings.HARDNESS_TILT));
        strata.watch(clusterSettings);
        assertThat(strata.factor(Hardness.HARD), equalTo(1.0));

        clusterSettings.applySettings(Settings.builder().put(QuerySamplingSettings.HARDNESS_TILT.getKey(), 2.0).build());

        assertThat(strata.factor(Hardness.HARD), greaterThan(1.0));
    }

    public void testStopsGivingNewSpacesAtALimit() {
        HardnessStrata strata = tilting(1.0);
        for (int i = 0; i < HardnessStrata.MAX_SPACES; i++) {
            assertThat(strata.assign("field" + i, 2, hitsWithContrast(0.7)), equalTo(Hardness.MEDIUM));
        }

        assertThat(strata.assign("one-too-many", 2, hitsWithContrast(0.7)), nullValue());
        assertThat("a space that is known keeps working", strata.assign("field0", 2, hitsWithContrast(0.7)), equalTo(Hardness.MEDIUM));
    }
}
