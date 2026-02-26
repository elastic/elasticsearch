/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class ReindexMetrics {

    public static final String REINDEX_TIME_HISTOGRAM = "es.reindex.duration.histogram";
    public static final String REINDEX_COMPLETION_COUNTER = "es.reindex.completion.total";

    // refers to https://opentelemetry.io/docs/specs/semconv/registry/attributes/error/#error-type
    public static final String ATTRIBUTE_NAME_ERROR_TYPE = "error_type";

    public static final String ATTRIBUTE_NAME_SOURCE = "reindex_source";
    public static final String ATTRIBUTE_VALUE_SOURCE_LOCAL = "local";
    public static final String ATTRIBUTE_VALUE_SOURCE_REMOTE = "remote";

    public static final String ATTRIBUTE_NAME_SLICING_MODE = "slicing_mode";

    private final LongHistogram reindexTimeSecsHistogram;
    private final LongCounter reindexCompletionCounter;

    public enum SlicingMode {
        /** No slicing. */
        NONE,
        /** Using {@code source.slice} in the request body. */
        MANUAL,
        /** Using {@code ?slices=N} for some integer N. */
        AUTO_FIXED,
        /** Using {@code ?slices=auto}. */
        AUTO_AUTO
    }

    public ReindexMetrics(MeterRegistry meterRegistry) {
        this.reindexTimeSecsHistogram = meterRegistry.registerLongHistogram(REINDEX_TIME_HISTOGRAM, "Time to reindex by search", "seconds");
        this.reindexCompletionCounter = meterRegistry.registerLongCounter(
            REINDEX_COMPLETION_COUNTER,
            "Number of completed reindex operations",
            "unit"
        );
    }

    public long recordTookTime(long tookTime, boolean remote, SlicingMode slicingMode) {
        Map<String, Object> attributes = getAttributes(remote, slicingMode);

        reindexTimeSecsHistogram.record(tookTime, attributes);
        return tookTime;
    }

    public void recordSuccess(boolean remote, SlicingMode slicingMode) {
        Map<String, Object> attributes = getAttributes(remote, slicingMode);
        // attribute ATTRIBUTE_ERROR_TYPE being absent indicates success
        assert attributes.get(ATTRIBUTE_NAME_ERROR_TYPE) == null : "error.type attribute must not be present for successes";

        reindexCompletionCounter.incrementBy(1, attributes);
    }

    public void recordFailure(boolean remote, Throwable e, SlicingMode slicingMode) {
        Map<String, Object> attributes = getAttributes(remote, slicingMode);
        // best effort to extract useful error type if possible
        String errorType;
        if (e instanceof ElasticsearchStatusException ese) {
            errorType = ese.status().name();
        } else {
            errorType = e.getClass().getTypeName();
        }
        attributes.put(ATTRIBUTE_NAME_ERROR_TYPE, errorType);

        // attribute ATTRIBUTE_ERROR_TYPE being present indicates failure
        // https://opentelemetry.io/docs/specs/semconv/general/recording-errors/#recording-errors-on-metrics
        assert attributes.get(ATTRIBUTE_NAME_ERROR_TYPE) != null : "error.type attribute must be present for failures";

        reindexCompletionCounter.incrementBy(1, attributes);
    }

    private static Map<String, Object> getAttributes(boolean remote, SlicingMode slicingMode) {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(ATTRIBUTE_NAME_SOURCE, remote ? ATTRIBUTE_VALUE_SOURCE_REMOTE : ATTRIBUTE_VALUE_SOURCE_LOCAL);
        attributes.put(ATTRIBUTE_NAME_SLICING_MODE, slicingMode.name().toLowerCase(Locale.ROOT));

        return attributes;
    }
}
