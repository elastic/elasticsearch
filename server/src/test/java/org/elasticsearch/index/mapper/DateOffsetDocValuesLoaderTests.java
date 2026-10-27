/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.time.DateUtils;

/**
 * Verifies that {@code date} and {@code date_nanos} fields record array offsets natively under
 * {@code synthetic_source_keep: arrays}, and that synthetic source rebuilds the array in its original order,
 * including duplicates and {@code null} entries, from doc values plus the {@code .offsets} field.
 * <p>
 * Values are produced by the default formatter of the chosen resolution, so that the formatted synthetic source
 * is byte-for-byte equal to the input.
 */
public class DateOffsetDocValuesLoaderTests extends OffsetDocValuesLoaderTestCase {

    private final DateFieldMapper.Resolution resolution = randomFrom(DateFieldMapper.Resolution.values());

    public void testOffsetArray() throws Exception {
        verifyOffsets(
            "{\"field\":[\"2024-05-01T00:00:00.000Z\",\"2024-01-01T00:00:00.000Z\",\"2024-03-01T00:00:00.000Z\","
                + "\"2024-05-01T00:00:00.000Z\",\"2024-01-01T00:00:00.000Z\"]}"
        );
        verifyOffsets(
            "{\"field\":[\"2024-05-01T00:00:00.000Z\",null,\"2024-01-01T00:00:00.000Z\",\"2024-03-01T00:00:00.000Z\",null,null,"
                + "\"2024-05-01T00:00:00.000Z\",\"2024-01-01T00:00:00.000Z\"]}"
        );
        verifyOffsets("{\"field\":[\"2024-01-01T00:00:00.000Z\",\"2024-01-01T00:00:00.000Z\",\"2024-01-01T00:00:00.000Z\"]}");
    }

    public void testOffsetNestedArray() throws Exception {
        verifyOffsets(
            "{\"field\":[[\"2024-05-01T00:00:00.000Z\",[\"2024-01-01T00:00:00.000Z\",[\"2024-03-01T00:00:00.000Z\"]]],"
                + "[\"2024-05-01T00:00:00.000Z\",\"2024-01-01T00:00:00.000Z\"]]}",
            "{\"field\":[\"2024-05-01T00:00:00.000Z\",\"2024-01-01T00:00:00.000Z\",\"2024-03-01T00:00:00.000Z\","
                + "\"2024-05-01T00:00:00.000Z\",\"2024-01-01T00:00:00.000Z\"]}"
        );
        verifyOffsets(
            "{\"field\":[\"2024-05-01T00:00:00.000Z\",[null,[[\"2024-01-01T00:00:00.000Z\",\"2024-01-01T00:00:00.000Z\"],[null,null]],"
                + "[\"2024-03-01T00:00:00.000Z\",\"2024-01-01T00:00:00.000Z\"]]]}",
            "{\"field\":[\"2024-05-01T00:00:00.000Z\",null,\"2024-01-01T00:00:00.000Z\",\"2024-01-01T00:00:00.000Z\",null,null,"
                + "\"2024-03-01T00:00:00.000Z\",\"2024-01-01T00:00:00.000Z\"]}"
        );
    }

    @Override
    protected String getFieldTypeName() {
        return resolution.type();
    }

    @Override
    protected Object randomValue() {
        return switch (resolution) {
            case MILLISECONDS -> DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(
                randomLongBetween(0, DateUtils.MAX_MILLIS_BEFORE_9999)
            );
            case NANOSECONDS -> DateFieldMapper.DEFAULT_DATE_TIME_NANOS_FORMATTER.formatNanos(
                randomLongBetween(0, DateUtils.MAX_NANOSECOND)
            );
        };
    }
}
