/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.dedup;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.querysampling.capture.CapturedQuery;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
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

    public void testCapturedArrivalsCountAsManyAsTheyStandFor() {
        MultiplicityTracker tracker = new MultiplicityTracker(10);
        QueryFingerprint a = new QueryFingerprint(1, 1);

        tracker.record(a, 0.1);
        tracker.record(a, 0.1);
        TrackedQuery query = tracker.record(a, 0.5);

        assertThat(query.multiplicity(), equalTo(3L));
        assertThat(query.weightedMultiplicity(), closeTo(10 + 10 + 2, 1e-9));
    }

    public void testSeenProbabilityOfAQueryCapturedRarely() {
        MultiplicityTracker tracker = new MultiplicityTracker(10);
        QueryFingerprint a = new QueryFingerprint(1, 1);

        assertThat(tracker.record(a, 0.01).seenProbability(), closeTo(1 - Math.pow(0.99, 100), 1e-9));
        assertThat(tracker.record(a, 0.01).seenProbability(), closeTo(1 - Math.pow(0.99, 200), 1e-9));
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

    public void testNoCollisionsAmongManyQueries() {
        Set<QueryFingerprint> seen = new HashSet<>();
        int queries = 50_000;
        for (int i = 0; i < queries; i++) {
            seen.add(QueryFingerprint.of(query("vec", randomVector(8), List.of())));
        }
        assertThat(seen.size(), equalTo(queries));
    }

    private static CapturedQuery query(String field, float[] vector, List<QueryBuilder> filters) {
        return new CapturedQuery(new String[] { "idx" }, field, vector, 10, 100, null, null, filters, null);
    }

    private static float[] randomVector(int dims) {
        float[] vector = new float[dims];
        for (int i = 0; i < dims; i++) {
            vector[i] = randomFloat();
        }
        return vector;
    }
}
