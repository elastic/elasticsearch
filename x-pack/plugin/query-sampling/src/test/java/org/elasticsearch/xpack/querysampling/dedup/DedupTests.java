/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.dedup;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.querysampling.capture.CapturedQuery;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class DedupTests extends ESTestCase {

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

    public void testAQueryIsForgottenBetweenOneAndTwoWindowsAfterItsLastArrival() {
        AtomicLong nanos = new AtomicLong();
        TimeValue window = TimeValue.timeValueMinutes(1);
        MultiplicityTracker tracker = new MultiplicityTracker(10, window, nanos::get);
        QueryFingerprint a = new QueryFingerprint(1, 1);
        QueryFingerprint b = new QueryFingerprint(2, 2);
        QueryFingerprint c = new QueryFingerprint(3, 3);

        TrackedQuery firstA = tracker.record(a);
        nanos.addAndGet(window.nanos());
        TrackedQuery firstB = tracker.record(b);
        assertTrue("a window later it is still known", tracker.isTracking(a, firstA));
        assertThat(tracker.distinct(), equalTo(2));

        nanos.addAndGet(window.nanos());
        assertThat("b arrived in the window before", tracker.record(b), sameInstance(firstB));
        assertFalse("a was last seen two windows ago", tracker.isTracking(a, firstA));
        assertThat(tracker.distinct(), equalTo(1));
        TrackedQuery secondA = tracker.record(a);
        assertThat("its next arrival starts a new count", secondA, not(sameInstance(firstA)));
        assertThat(secondA.multiplicity(), equalTo(1L));

        nanos.addAndGet(5 * window.nanos());
        tracker.record(c);
        assertThat("after a long quiet spell nothing else is known", tracker.distinct(), equalTo(1));
        assertFalse(tracker.isTracking(b, firstB));
    }

    public void testParametersThatDecideTheQualityAreInTheIdentityAndTheLabelIsNot() {
        float[] vector = randomVector(16);
        CapturedQuery base = new CapturedQuery(new String[] { "a" }, "vec", vector, 10, 100, null, null, List.of(), "x");

        assertThat("only a label", QueryFingerprint.of(base), equalTo(QueryFingerprint.of(withLabel(base, "y"))));
        for (CapturedQuery other : List.of(
            new CapturedQuery(new String[] { "a" }, "vec", vector, 50, 100, null, null, List.of(), "x"),
            new CapturedQuery(new String[] { "a" }, "vec", vector, 10, 500, null, null, List.of(), "x"),
            new CapturedQuery(new String[] { "a" }, "vec", vector, 10, 100, 0.1f, null, List.of(), "x"),
            new CapturedQuery(new String[] { "a" }, "vec", vector, 10, 100, null, 3f, List.of(), "x")
        )) {
            assertThat(QueryFingerprint.of(other), not(equalTo(QueryFingerprint.of(base))));
        }
    }

    private static CapturedQuery withLabel(CapturedQuery query, String label) {
        return new CapturedQuery(
            query.indices(),
            query.field(),
            query.queryVector(),
            query.k(),
            query.numCandidates(),
            query.visitPercentage(),
            query.oversample(),
            query.filters(),
            label
        );
    }

    private static float[] randomVector(int dims) {
        float[] vector = new float[dims];
        for (int i = 0; i < dims; i++) {
            vector[i] = randomFloat();
        }
        return vector;
    }
}
