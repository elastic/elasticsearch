/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.util.DateUtils;

import java.io.IOException;
import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;
import java.util.Objects;

public class DateTimeParseProcessor extends BinaryDateTimeProcessor {

    public static final String NAME = "dtparse";

    public DateTimeParseProcessor(Processor source1, Processor source2, ZoneId zoneId) {
        super(source1, source2, zoneId);
    }

    public DateTimeParseProcessor(StreamInput in) throws IOException {
        super(in);
    }

    /**
     * Used in Painless scripting
     */
    public static Object process(Object timestampStr, Object pattern, ZoneId zoneId) {
        if (timestampStr == null || pattern == null) {
            return null;
        }
        if (timestampStr instanceof String == false) {
            throw new SqlIllegalArgumentException("A string is required; received [{}]", timestampStr);
        }
        if (pattern instanceof String == false) {
            throw new SqlIllegalArgumentException("A string is required; received [{}]", pattern);
        }

        if (((String) timestampStr).isEmpty() || ((String) pattern).isEmpty()) {
            return null;
        }

        try {
            TemporalAccessor ta = DateTimeFormatter.ofPattern((String) pattern, Locale.ROOT)
                .parseBest((String) timestampStr, ZonedDateTime::from, LocalDateTime::from);
            if (ta instanceof LocalDateTime) {
                return DateUtils.atTimeZone((LocalDateTime) ta, zoneId);
            } else {
                return ((ZonedDateTime) ta).withZoneSameInstant(zoneId);
            }
        } catch (IllegalArgumentException | DateTimeException e) {
            String msg = e.getMessage();
            if (msg.contains("Unable to convert parsed text using any of the specified queries")) {
                msg = "Unable to convert parsed text into [datetime]";
            }
            throw new SqlIllegalArgumentException(
                "Invalid date/time string [{}] or pattern [{}] is received; {}",
                timestampStr,
                pattern,
                msg
            );
        }
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected Object doProcess(Object timestamp, Object pattern) {
        return process(timestamp, pattern, zoneId());
    }

    @Override
    public int hashCode() {
        return Objects.hash(left(), right());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        DateTimeParseProcessor other = (DateTimeParseProcessor) obj;
        return Objects.equals(left(), other.left()) && Objects.equals(right(), other.right());
    }
}
