/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.dedup;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.querysampling.capture.CapturedQuery;

import java.util.List;

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
