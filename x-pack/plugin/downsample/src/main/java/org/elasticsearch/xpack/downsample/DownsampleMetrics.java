/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

public class DownsampleMetrics {
    public static final String SUCCESS = "es.tsdb.downsample.success.total";
    public static final String FAILURE = "es.tsdb.downsample.failure.total";

    private final LongCounter downsampleSuccess;
    private final LongCounter downsampleFailure;

    public DownsampleMetrics(MeterRegistry meterRegistry) {
        this(
            meterRegistry.registerLongCounter(
                SUCCESS,
                "Number of successful downsample operations",
                "count"
            ),
            meterRegistry.registerLongCounter(
                FAILURE,
                "Number of failed downsample operations",
                "count"
            )
        );
    }

    public DownsampleMetrics(LongCounter downsampleSuccess, LongCounter downsampleFailure) {
        this.downsampleSuccess = downsampleSuccess;
        this.downsampleFailure = downsampleFailure;
    }

    public LongCounter getDownsampleSuccess() {
        return downsampleSuccess;
    }

    public LongCounter getDownsampleFailure() {
        return downsampleFailure;
    }
}
