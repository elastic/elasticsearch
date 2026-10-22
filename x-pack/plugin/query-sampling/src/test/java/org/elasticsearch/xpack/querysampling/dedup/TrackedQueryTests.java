/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.dedup;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class TrackedQueryTests extends ESTestCase {

    private static final QueryFingerprint FINGERPRINT = new QueryFingerprint(1, 1);

    public void testAQueryThatIsAlwaysCapturedIsSeenWithCertainty() {
        TrackedQuery query = new MultiplicityTracker(10).record(FINGERPRINT, 1.0);

        assertThat(query.weights().seenProbability(), equalTo(1.0));
    }

    public void testEachArrivalCountsForTheInverseOfTheCaptureRateItHad() {
        MultiplicityTracker tracker = new MultiplicityTracker(10);
        tracker.record(FINGERPRINT, 0.5);

        TrackedQuery.Weights weights = tracker.record(FINGERPRINT, 0.25).weights();

        assertThat(weights.weightedMultiplicity(), closeTo(2 + 4, 1e-9));
        assertThat(weights.multiplicity(), equalTo(2L));
        assertThat(weights.captureRate(), closeTo(0.25, 1e-9));
    }

    public void testNoChanceOfBeingPickedIsRecordedAsPositiveZeroAndDrawsCompound() {
        TrackedQuery none = new TrackedQuery();
        none.recordDraw(0);
        assertThat("not -0.0", none.weights().inclusionProbability(), equalTo(0.0));

        TrackedQuery twice = new TrackedQuery();
        twice.recordDraw(0.5);
        twice.recordDraw(0.5);
        assertThat(twice.weights().inclusionProbability(), closeTo(0.75, 1e-9));
    }

    public void testAnEventCountsOnceAndIsKeptWithTheCaptureRateTimesTheSliceRate() {
        TrackedQuery.Weights weights = TrackedQuery.event(0.25, 0.5).weights();

        assertThat(weights.multiplicity(), equalTo(1L));
        assertThat(weights.weightedMultiplicity(), closeTo(4.0, 1e-9));
        assertThat(weights.inclusionProbability(), closeTo(0.125, 1e-9));
    }
}
