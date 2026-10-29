/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.update;

import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.MeterRegistry;

/**
 * Counts how an update that qualifies for the in-place doc-values fast path is realized: in place, or as a reindex because the base is not
 * yet on every tracked copy (see {@link UpdateHelper}). The fallback is correct but costs a full reindex and is expected to be rare outside
 * recovery windows, so the ratio of the two counters is the signal to watch.
 */
public class DocValuesUpdateMetrics {

    public static final String IN_PLACE_TOTAL = "es.update.doc_values.in_place.total";
    public static final String REINDEX_FALLBACK_TOTAL = "es.update.doc_values.reindex_fallback.total";

    public static final DocValuesUpdateMetrics NOOP = new DocValuesUpdateMetrics(TelemetryProvider.NOOP.getMeterRegistry());

    private final LongCounter inPlaceTotal;
    private final LongCounter reindexFallbackTotal;

    public DocValuesUpdateMetrics(MeterRegistry meterRegistry) {
        inPlaceTotal = meterRegistry.registerLongCounter(IN_PLACE_TOTAL, "Updates that took the in-place doc-values fast path", "unit");
        reindexFallbackTotal = meterRegistry.registerLongCounter(
            REINDEX_FALLBACK_TOTAL,
            "In-place-eligible updates reindexed instead because the base was not yet on every tracked copy",
            "unit"
        );
    }

    public void onInPlace() {
        inPlaceTotal.increment();
    }

    public void onReindexFallback() {
        reindexFallbackTotal.increment();
    }
}
