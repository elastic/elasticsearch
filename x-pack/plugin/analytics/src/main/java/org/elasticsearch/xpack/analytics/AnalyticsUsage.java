/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics;

import org.elasticsearch.common.xcontent.ContextParser;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks usage of the Analytics aggregations.
 */
public class AnalyticsUsage {
    private final AtomicLong boxplotUsage = new AtomicLong(0);
    private final AtomicLong cumulativeCardUsage = new AtomicLong(0);
    private final AtomicLong stringStatsUsage = new AtomicLong(0);
    private final AtomicLong topMetricsUsage = new AtomicLong(0);

    public <C, T> ContextParser<C, T> trackBoxplot(ContextParser<C, T> realParser) {
        return track(realParser, boxplotUsage);
    }

    public long getBoxplotUsage() {
        return boxplotUsage.get();
    }

    public <C, T> ContextParser<C, T> trackCumulativeCardinality(ContextParser<C, T> realParser) {
        return track(realParser, cumulativeCardUsage);
    }

    public long getCumulativeCardUsage() {
        return cumulativeCardUsage.get();
    }

    public <C, T> ContextParser<C, T> trackStringStats(ContextParser<C, T> realParser) {
        return track(realParser, stringStatsUsage);
    }

    public long getStringStatsUsage() {
        return stringStatsUsage.get();
    }

    public <C, T> ContextParser<C, T> trackTopMetrics(ContextParser<C, T> realParser) {
        return track(realParser, topMetricsUsage);
    }

    public long getTopMetricsUsage() {
        return topMetricsUsage.get();
    }

    /**
     * Track successful parsing.
     */
    private static <C, T> ContextParser<C, T> track(ContextParser<C, T> realParser, AtomicLong usage) {
        return (parser, context) -> {
            T value = realParser.parse(parser, context);
            // Intentionally doesn't count unless the parser returns cleanly.
            usage.incrementAndGet();
            return value;
        };
    }
}
