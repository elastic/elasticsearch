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

public class UpdateByQueryMetrics {
    public static final String UPDATE_BY_QUERY_TIME_HISTOGRAM = "es.update_by_query.duration.histogram";

    private final LongHistogram updateByQueryTimeSecsHistogram;

    public UpdateByQueryMetrics(MeterRegistry meterRegistry) {
        this(
            meterRegistry.registerLongHistogram(UPDATE_BY_QUERY_TIME_HISTOGRAM, "Time taken to execute Update by Query request", "seconds")
        );
    }

    private UpdateByQueryMetrics(LongHistogram updateByQueryTimeSecsHistogram) {
        this.updateByQueryTimeSecsHistogram = updateByQueryTimeSecsHistogram;
    }

    public long recordTookTime(long tookTime) {
        updateByQueryTimeSecsHistogram.record(tookTime);
        return tookTime;
    }
}
