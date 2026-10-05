/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.dedup;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class MultiplicityTrackerTests extends ESTestCase {

    public void testCountsRepeatsOfTheSameQuery() {
        MultiplicityTracker tracker = new MultiplicityTracker(10);
        QueryFingerprint a = new QueryFingerprint(1, 1);
        QueryFingerprint b = new QueryFingerprint(2, 2);

        TrackedQuery first = tracker.record(a);
        assertThat(first.multiplicity(), equalTo(1L));
        assertThat(tracker.record(a), sameInstance(first));
        assertThat(first.multiplicity(), equalTo(2L));
        assertThat(tracker.record(b).multiplicity(), equalTo(1L));
        assertThat(tracker.record(a).multiplicity(), equalTo(3L));

        assertThat(tracker.distinct(), equalTo(2));
        assertThat(tracker.untracked(), equalTo(0L));
    }

    public void testCapturedArrivalsCountAsManyAsTheyStandFor() {
        MultiplicityTracker tracker = new MultiplicityTracker(10);
        QueryFingerprint a = new QueryFingerprint(1, 1);

        tracker.record(a, 0.1);
        tracker.record(a, 0.1);
        TrackedQuery query = tracker.record(a, 0.5);

        assertThat(query.multiplicity(), equalTo(3L));
        assertThat(query.weightedMultiplicity(), closeTo(10 + 10 + 2, 1e-9));
    }

    public void testStopsTrackingNewQueriesWhenFull() {
        MultiplicityTracker tracker = new MultiplicityTracker(2);
        QueryFingerprint known = new QueryFingerprint(1, 1);
        tracker.record(known);
        tracker.record(new QueryFingerprint(2, 2));

        assertThat(tracker.record(new QueryFingerprint(3, 3)), nullValue());
        assertThat(tracker.record(new QueryFingerprint(4, 4)), nullValue());
        assertThat("queries that are already known keep being counted", tracker.record(known).multiplicity(), equalTo(2L));

        assertThat(tracker.distinct(), equalTo(2));
        assertThat(tracker.untracked(), equalTo(2L));
    }

    public void testForgetsQueriesNotSeenForAWindow() {
        AtomicLong now = new AtomicLong();
        MultiplicityTracker tracker = new MultiplicityTracker(10, TimeValue.timeValueMinutes(1), now::get);
        QueryFingerprint gone = new QueryFingerprint(1, 1);
        tracker.record(gone);

        now.addAndGet(TimeValue.timeValueMinutes(1).nanos());
        assertThat("still known one window after its arrival", tracker.record(new QueryFingerprint(2, 2)).multiplicity(), equalTo(1L));
        assertThat(tracker.distinct(), equalTo(2));

        now.addAndGet(TimeValue.timeValueMinutes(1).nanos());
        assertThat(tracker.record(new QueryFingerprint(3, 3)).multiplicity(), equalTo(1L));
        assertThat("the first query is gone", tracker.distinct(), equalTo(2));
        assertThat("and starts from scratch if it comes back", tracker.record(gone).multiplicity(), equalTo(1L));
    }

    public void testQueriesKeepTheirCountAsLongAsTheyKeepArriving() {
        AtomicLong now = new AtomicLong();
        MultiplicityTracker tracker = new MultiplicityTracker(10, TimeValue.timeValueMinutes(1), now::get);
        QueryFingerprint frequent = new QueryFingerprint(1, 1);

        int windows = between(3, 6);
        for (int i = 0; i < windows; i++) {
            assertThat(tracker.record(frequent).multiplicity(), equalTo((long) i + 1));
            now.addAndGet(TimeValue.timeValueSeconds(61).nanos());
        }
        assertThat(tracker.distinct(), equalTo(1));
    }

    public void testRotationMakesRoomWhenFull() {
        AtomicLong now = new AtomicLong();
        MultiplicityTracker tracker = new MultiplicityTracker(2, TimeValue.timeValueMinutes(1), now::get);
        tracker.record(new QueryFingerprint(1, 1));
        tracker.record(new QueryFingerprint(2, 2));
        assertThat(tracker.record(new QueryFingerprint(3, 3)), nullValue());

        now.addAndGet(TimeValue.timeValueMinutes(2).nanos());

        assertThat(tracker.record(new QueryFingerprint(3, 3)).multiplicity(), equalTo(1L));
        assertThat(tracker.distinct(), equalTo(1));
    }
}
