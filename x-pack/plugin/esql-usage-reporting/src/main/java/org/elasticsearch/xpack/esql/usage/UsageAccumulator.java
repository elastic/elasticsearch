/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.usage;

import org.elasticsearch.xpack.core.esql.QueryMetricsListener.Outcome;

import java.time.Instant;
import java.util.Map;

/**
 * The only place where the two threads of control of this plugin meet: query threads add to it through
 * {@link UsageListener}, and the {@link UsageReporter} thread drains it once per period.
 */
public interface UsageAccumulator {

    /**
     * Adds the counted parts of {@code metrics} to the sum of {@code outcome}. This is the hot path: it is called concurrently by
     * many query threads, so implementations may only do atomic additions, with no I/O, no locks and no allocation to speak of.
     */
    void record(Outcome outcome, Map<String, Long> metrics);

    /**
     * Returns the sums since the previous drain and resets them, so that a delta is never returned twice and an addend is never
     * lost: the reset is atomic per sum. The components of one query that is recorded while a drain runs may land in different
     * drains, which is harmless because the sums are additive. Called only by the reporter.
     */
    UsageSnapshot drain(Instant drainTime);
}
