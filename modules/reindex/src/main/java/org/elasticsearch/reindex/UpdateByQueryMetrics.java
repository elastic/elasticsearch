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
    public static final String TOOK_TIME_HISTOGRAM = "es.update_by_query.took_time.histogram";

    private final LongHistogram updateByQueryTimeSecsHistogram;

    public UpdateByQueryMetrics(MeterRegistry meterRegistry) {
        this(meterRegistry.registerLongHistogram(TOOK_TIME_HISTOGRAM, "Time taken by Update by Query request", "seconds"));
    }

    private UpdateByQueryMetrics(LongHistogram reindexTimeSecsHistogram) {
        this.updateByQueryTimeSecsHistogram = reindexTimeSecsHistogram;
    }

    public long recordTookTime(long tookTime) {
        updateByQueryTimeSecsHistogram.record(tookTime);
        return tookTime;
    }
}
