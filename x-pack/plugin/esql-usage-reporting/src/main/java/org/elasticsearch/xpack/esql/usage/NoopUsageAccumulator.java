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
 * A {@link UsageAccumulator} that keeps nothing. It is what {@link EsqlUsageReportingPlugin#accumulator()} returns until the real
 * one is built, and for ever when the plugin is disabled or incomplete: queries are never affected, and nothing is reported.
 */
public final class NoopUsageAccumulator implements UsageAccumulator {

    public static final NoopUsageAccumulator INSTANCE = new NoopUsageAccumulator();

    private NoopUsageAccumulator() {}

    @Override
    public void record(Outcome outcome, Map<String, Long> metrics) {}

    @Override
    public UsageSnapshot drain(Instant drainTime) {
        return new UsageSnapshot(drainTime, Map.of());
    }
}
