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

public class ReindexMetrics {

    public static final String REINDEX_TIME_HISTOGRAM = "es.reindex.duration.histogram";

    private final LongHistogram reindexTimeSecsHistogram;

    public ReindexMetrics(MeterRegistry meterRegistry) {
        this(meterRegistry.registerLongHistogram(REINDEX_TIME_HISTOGRAM, "Time to reindex by search", "millis"));
    }

    private ReindexMetrics(LongHistogram reindexTimeSecsHistogram) {
        this.reindexTimeSecsHistogram = reindexTimeSecsHistogram;
    }

    public long recordTookTime(long tookTime) {
        reindexTimeSecsHistogram.record(tookTime);
        return tookTime;
    }
}
