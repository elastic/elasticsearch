/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.dedup;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class MultiplicityTrackerTests extends ESTestCase {

    public void testCountsRepeatsOfTheSameQuery() {
        MultiplicityTracker tracker = new MultiplicityTracker(10);
        QueryFingerprint a = new QueryFingerprint(1, 1);
        QueryFingerprint b = new QueryFingerprint(2, 2);

        assertThat(tracker.record(a), equalTo(1L));
        assertThat(tracker.record(a), equalTo(2L));
        assertThat(tracker.record(b), equalTo(1L));
        assertThat(tracker.record(a), equalTo(3L));

        assertThat(tracker.distinct(), equalTo(2));
        assertThat(tracker.untracked(), equalTo(0L));
    }

    public void testStopsTrackingNewQueriesWhenFull() {
        MultiplicityTracker tracker = new MultiplicityTracker(2);
        QueryFingerprint known = new QueryFingerprint(1, 1);
        assertThat(tracker.record(known), equalTo(1L));
        assertThat(tracker.record(new QueryFingerprint(2, 2)), equalTo(1L));

        assertThat(tracker.record(new QueryFingerprint(3, 3)), equalTo(0L));
        assertThat(tracker.record(new QueryFingerprint(4, 4)), equalTo(0L));
        assertThat("queries that are already known keep being counted", tracker.record(known), equalTo(2L));

        assertThat(tracker.distinct(), equalTo(2));
        assertThat(tracker.untracked(), equalTo(2L));
    }
}
