/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.shard.ShardMetrics;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.telemetry.metric.MetricAttributes;

import java.util.Locale;
import java.util.Map;

/**
 * Counts failures thrown by Lucene codec formats. Recorded from two hooks that together see every codec call: {@link MetricingCodec} on
 * the write side and {@link org.elasticsearch.common.lucene.index.ElasticsearchLeafReader} on the read side.
 */
public class CodecMetrics {

    public static final String CODEC_FAILURE_TOTAL = "es.codec.failure.total";

    public static final String INDEX_MODE_ATTRIBUTE = "es_index_mode";
    public static final String FORMAT_ATTRIBUTE = "es_codec_format";
    public static final String OPERATION_ATTRIBUTE = "es_codec_operation";

    public static final CodecMetrics NOOP = new CodecMetrics(TelemetryProvider.NOOP.getMeterRegistry());

    /** The codec format the failure came from. */
    public enum Format {
        POSTINGS,
        DOC_VALUES,
        STORED_FIELDS,
        KNN_VECTORS,
        POINTS,
        NORMS
    }

    public enum Operation {
        OPEN,
        READ,
        WRITE,
        MERGE
    }

    private final LongCounter failures;

    public CodecMetrics(MeterRegistry meterRegistry) {
        failures = meterRegistry.registerLongCounter(CODEC_FAILURE_TOTAL, "Number of failures thrown by Lucene codec formats", "unit");
    }

    public void onFailure(IndexMode indexMode, Format format, Operation operation, Throwable t) {
        failures.incrementBy(
            1,
            Map.of(
                INDEX_MODE_ATTRIBUTE,
                indexMode.getName(),
                MetricAttributes.ERROR_TYPE,
                ShardMetrics.errorType(t),
                FORMAT_ATTRIBUTE,
                format.name().toLowerCase(Locale.ROOT),
                OPERATION_ATTRIBUTE,
                operation.name().toLowerCase(Locale.ROOT)
            )
        );
    }
}
