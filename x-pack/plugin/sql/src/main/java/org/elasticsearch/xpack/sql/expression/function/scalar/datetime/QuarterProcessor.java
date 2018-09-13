/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Objects;
import java.util.TimeZone;

public class QuarterProcessor extends BaseDateTimeProcessor {
    
    public QuarterProcessor(TimeZone timeZone) {
        super(timeZone);
    }
    
    public QuarterProcessor(StreamInput in) throws IOException {
        super(in);
    }
    
    public static final String NAME = "q";

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Object doProcess(long millis) {
        return quarter(millis, timeZone().getID());
    }
    
    public static Integer quarter(long millis, String tzId) {
        ZonedDateTime time = ZonedDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneId.of(tzId));
        return Integer.valueOf(time.format(DateTimeFormatter.ofPattern(Quarter.QUARTER_FORMAT, Locale.ROOT)));
    }

    @Override
    public int hashCode() {
        return Objects.hash(timeZone());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        DateTimeProcessor other = (DateTimeProcessor) obj;
        return Objects.equals(timeZone(), other.timeZone());
    }
}
