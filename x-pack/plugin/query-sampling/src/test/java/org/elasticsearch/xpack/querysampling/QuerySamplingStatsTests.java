/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import static org.hamcrest.Matchers.equalTo;

public class QuerySamplingStatsTests extends AbstractWireSerializingTestCase<QuerySamplingStats> {

    private static final int COUNTERS = 14;

    @Override
    protected Writeable.Reader<QuerySamplingStats> instanceReader() {
        return QuerySamplingStats::new;
    }

    @Override
    protected QuerySamplingStats createTestInstance() {
        long[] counters = new long[COUNTERS];
        for (int i = 0; i < COUNTERS; i++) {
            counters[i] = randomNonNegativeLong();
        }
        return stats(counters, randomDouble(), randomDouble(), randomDouble());
    }

    @Override
    protected QuerySamplingStats mutateInstance(QuerySamplingStats instance) {
        long[] counters = counters(instance);
        double rate = instance.effectiveCaptureRate();
        double scale = instance.effectiveAcceptanceScale();
        double credit = instance.groundTruthCreditMillis();
        int mutated = between(0, COUNTERS + 2);
        if (mutated < COUNTERS) {
            counters[mutated]++;
        } else if (mutated == COUNTERS) {
            rate += 0.5;
        } else if (mutated == COUNTERS + 1) {
            scale += 0.5;
        } else {
            credit += 0.5;
        }
        return stats(counters, rate, scale, credit);
    }

    private static long[] counters(QuerySamplingStats stats) {
        return new long[] {
            stats.knnSearches(),
            stats.captured(),
            stats.dropped(),
            stats.distinctQueries(),
            stats.untrackedArrivals(),
            stats.picked(),
            stats.written(),
            stats.writeFailures(),
            stats.writeDropped(),
            stats.weightsRefreshed(),
            stats.expired(),
            stats.groundTruthComputed(),
            stats.groundTruthFailed(),
            stats.starved() };
    }

    private static QuerySamplingStats stats(
        long[] counters,
        double effectiveCaptureRate,
        double effectiveAcceptanceScale,
        double groundTruthCreditMillis
    ) {
        assertThat(counters.length, equalTo(COUNTERS));
        return new QuerySamplingStats(
            counters[0],
            counters[1],
            counters[2],
            counters[3],
            counters[4],
            counters[5],
            counters[6],
            counters[7],
            counters[8],
            counters[9],
            counters[10],
            counters[11],
            counters[12],
            counters[13],
            effectiveCaptureRate,
            effectiveAcceptanceScale,
            groundTruthCreditMillis
        );
    }
}
