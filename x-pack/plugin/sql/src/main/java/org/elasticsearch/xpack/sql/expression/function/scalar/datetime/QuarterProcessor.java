/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Objects;

public class QuarterProcessor extends BaseDateTimeProcessor {

    public static final String NAME = "q";
    private static final DateTimeFormatter QUARTER_FORMAT = DateTimeFormatter.ofPattern("q", Locale.ROOT);

    
    public QuarterProcessor(ZoneId zoneId) {
        super(zoneId);
    }
    
    public QuarterProcessor(StreamInput in) throws IOException {
        super(in);
    }
    
    @Override
    public void writeTo(StreamOutput out) throws IOException {}

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Object doProcess(ZonedDateTime zdt) {
        return quarter(zdt);
    }
    
    public static Integer quarter(ZonedDateTime dateTime, String tzId) {
        return quarter(dateTime.withZoneSameInstant(ZoneId.of(tzId)));
    }

    static Integer quarter(ZonedDateTime zdt) {
        return Integer.valueOf(zdt.format(QUARTER_FORMAT));
    }

    @Override
    public int hashCode() {
        return Objects.hash(zoneId());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        DateTimeProcessor other = (DateTimeProcessor) obj;
        return Objects.equals(zoneId(), other.zoneId());
    }
}
