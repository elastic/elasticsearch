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

import java.util.Random;
import java.util.Set;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class EventSliceTests extends ESTestCase {

    private static EventSlice slice(double rate, Random random) {
        EventSlice slice = new EventSlice(() -> random);
        slice.watch(
            new ClusterSettings(
                Settings.builder().put(QuerySamplingSettings.EVENT_SLICE_RATE.getKey(), rate).build(),
                Set.of(QuerySamplingSettings.EVENT_SLICE_RATE)
            )
        );
        return slice;
    }

    public void testNothingIsKeptWithoutARateAndNoRandomnessIsNeeded() {
        EventSlice slice = new EventSlice(() -> { throw new AssertionError("nothing to draw"); });

        assertThat(slice.draw(), equalTo(0.0));
    }

    public void testEverythingIsKeptAtARateOfOne() {
        EventSlice slice = slice(1.0, new Random(randomLong()));

        for (int i = 0; i < 100; i++) {
            assertThat(slice.draw(), equalTo(1.0));
        }
    }

    public void testAKeptSearchCarriesTheProbabilityItHadOfBeingKept() {
        EventSlice slice = slice(0.25, new Random(randomLong()) {
            @Override
            public double nextDouble() {
                return 0.2; // under the rate
            }
        });

        assertThat(slice.draw(), equalTo(0.25));
    }

    public void testASearchThatIsNotDrawnIsNotKept() {
        EventSlice slice = slice(0.25, new Random(randomLong()) {
            @Override
            public double nextDouble() {
                return 0.3;
            }
        });

        assertThat(slice.draw(), equalTo(0.0));
    }

    public void testAboutTheRateOfSearchesIsKept() {
        EventSlice slice = slice(0.1, new Random(randomLong()));
        int trials = 20_000;
        int kept = 0;
        for (int i = 0; i < trials; i++) {
            kept += slice.draw() > 0 ? 1 : 0;
        }

        assertThat((double) kept / trials, closeTo(0.1, 5 * Math.sqrt(0.1 * 0.9 / trials)));
    }

    public void testRateFollowsTheSettingWhenItChanges() {
        EventSlice slice = new EventSlice(() -> new Random(0L) {
            @Override
            public double nextDouble() {
                return 0.0;
            }
        });
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, Set.of(QuerySamplingSettings.EVENT_SLICE_RATE));
        slice.watch(clusterSettings);
        assertThat(slice.draw(), equalTo(0.0));

        clusterSettings.applySettings(Settings.builder().put(QuerySamplingSettings.EVENT_SLICE_RATE.getKey(), 0.5).build());

        assertThat(slice.draw(), equalTo(0.5));
    }

    public void testEveryEventHasItsOwnId() {
        EventSlice slice = slice(1.0, new Random(randomLong()));

        assertThat(slice.newId(), not(equalTo(slice.newId())));
    }
}
