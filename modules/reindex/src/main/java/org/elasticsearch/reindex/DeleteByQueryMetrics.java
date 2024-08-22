/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reindex;

import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

public class DeleteByQueryMetrics {
    public static final String DELETE_BY_QUERY_TIME_HISTOGRAM = "es.delete_by_query.duration.histogram";

    private final LongHistogram deleteByQueryTimeSecsHistogram;

    public DeleteByQueryMetrics(MeterRegistry meterRegistry) {
        this(
            meterRegistry.registerLongHistogram(DELETE_BY_QUERY_TIME_HISTOGRAM, "Time taken to execute Delete by Query request", "seconds")
        );
    }

    private DeleteByQueryMetrics(LongHistogram deleteByQueryTimeSecsHistogram) {
        this.deleteByQueryTimeSecsHistogram = deleteByQueryTimeSecsHistogram;
    }

    public long recordTookTime(long tookTime) {
        deleteByQueryTimeSecsHistogram.record(tookTime);
        return tookTime;
    }
}
