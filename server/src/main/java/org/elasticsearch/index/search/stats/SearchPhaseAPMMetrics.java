/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.search.stats;

import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.telemetry.metric.LongHistogram;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class SearchPhaseAPMMetrics {
    public static final String SYSTEM_THREAD_ATTRIBUTE_NAME = "system_thread";
    // Avoid allocating objects in the search path and multithreading clashes
    private static final ThreadLocal<Map<String, Object>> THREAD_LOCAL_ATTRS = ThreadLocal.withInitial(() -> new HashMap<>(1));

    protected static void recordPhaseLatency(LongHistogram histogramMetric, long tookInNanos) {
        Map<String, Object> attrs = SearchPhaseAPMMetrics.THREAD_LOCAL_ATTRS.get();
        boolean isSystem = ((EsExecutors.EsThread) Thread.currentThread()).isSystem();
        attrs.put(SYSTEM_THREAD_ATTRIBUTE_NAME, isSystem);
        histogramMetric.record(TimeUnit.NANOSECONDS.toMillis(tookInNanos), attrs);
    }
}
