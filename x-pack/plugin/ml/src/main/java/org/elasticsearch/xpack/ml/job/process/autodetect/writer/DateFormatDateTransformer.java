/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.writer;

import org.elasticsearch.xpack.core.ml.utils.time.DateTimeFormatterTimestampConverter;
import org.elasticsearch.xpack.core.ml.utils.time.TimestampConverter;

import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.Locale;

/**
 * A transformer that attempts to parse a String timestamp as a data according to a time format.
 * It converts that to a long that represents the equivalent milliseconds since the epoch.
 */
public class DateFormatDateTransformer implements DateTransformer {

    private final String timeFormat;
    private final TimestampConverter dateToEpochConverter;

    public DateFormatDateTransformer(String timeFormat) {
        this.timeFormat = timeFormat;
        dateToEpochConverter = DateTimeFormatterTimestampConverter.ofPattern(timeFormat, ZoneOffset.UTC);
    }

    @Override
    public long transform(String timestamp) throws CannotParseTimestampException {
        try {
            return dateToEpochConverter.toEpochMillis(timestamp);
        } catch (DateTimeParseException e) {
            String message = String.format(Locale.ROOT, "Cannot parse date '%s' with format string '%s'", timestamp, timeFormat);
            throw new CannotParseTimestampException(message, e);
        }
    }
}
