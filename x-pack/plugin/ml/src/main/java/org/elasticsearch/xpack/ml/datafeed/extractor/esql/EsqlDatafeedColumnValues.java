/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor.esql;

import org.elasticsearch.xpack.core.ml.job.messages.Messages;

import java.time.Instant;

/**
 * Shared conversions between ES|QL column output types and values used by extraction and validation.
 */
final class EsqlDatafeedColumnValues {

    private EsqlDatafeedColumnValues() {}

    static boolean isDateColumnType(String outputType) {
        return "date".equals(outputType) || "date_nanos".equals(outputType);
    }

    static Long toEpochMillisOrNull(Object value, boolean isDate) {
        if (value == null) {
            return null;
        }
        return toEpochMillis(value, isDate);
    }

    static long toEpochMillis(Object value, boolean isDate) {
        if (isDate) {
            if (value instanceof String isoDate) {
                return Instant.parse(isoDate).toEpochMilli();
            }
            throw new IllegalArgumentException(Messages.getMessage(Messages.DATAFEED_ESQL_EXPECTED_DATE_VALUE));
        }
        if (value instanceof Number number) {
            return number.longValue();
        }
        throw new IllegalArgumentException(Messages.getMessage(Messages.DATAFEED_ESQL_EXPECTED_NUMERIC_TIMESTAMP));
    }
}
