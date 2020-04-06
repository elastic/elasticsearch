/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;

import java.io.IOException;
import java.time.DateTimeException;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;

import static org.elasticsearch.xpack.sql.util.DateUtils.asTimeAtZone;

public class ToCharProcessor extends BinaryDateTimeProcessor {

    public static final String NAME = "dtochar";

    public ToCharProcessor(Processor source1, Processor source2, ZoneId zoneId) {
        super(source1, source2, zoneId);
    }

    public ToCharProcessor(StreamInput in) throws IOException {
        super(in);
    }

    /**
     * Used in Painless scripting
     */
    public static Object process(Object timestamp, Object pattern, ZoneId zoneId) {
        if (timestamp == null || pattern == null) {
            return null;
        }
        if (pattern instanceof String == false) {
            throw new SqlIllegalArgumentException("A string is required; received [{}]", pattern);
        }
        if (((String) pattern).isEmpty()) {
            return null;
        }

        if (timestamp instanceof ZonedDateTime == false && timestamp instanceof OffsetTime == false) {
            throw new SqlIllegalArgumentException("A date/datetime/time is required; received [{}]", timestamp);
        }

        TemporalAccessor ta;
        if (timestamp instanceof ZonedDateTime) {
            ta = ((ZonedDateTime) timestamp).withZoneSameInstant(zoneId);
        } else {
            ta = asTimeAtZone((OffsetTime) timestamp, zoneId);
        }
        try {
            return DateTimeFormatter.ofPattern((String) pattern, Locale.ROOT).format(ta);
        } catch (IllegalArgumentException | DateTimeException e) {
            throw new SqlIllegalArgumentException("Invalid date/time pattern is received [{}]; {}", pattern, e.getMessage());
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
}
