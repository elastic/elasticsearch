/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Objects;
import java.util.TimeZone;
import java.util.function.BiFunction;

public class NamedDateTimeProcessor extends BaseDateTimeProcessor {
    
    public enum NameExtractor {
        // for the moment we'll use no specific Locale, but we might consider introducing a Locale parameter, just like the timeZone one
        DAY_NAME((Long millis, String tzId) -> {
            ZonedDateTime time = ZonedDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneId.of(tzId));
            return time.format(DateTimeFormatter.ofPattern(DayName.DAY_NAME_FORMAT, Locale.ROOT));
        }),
        MONTH_NAME((Long millis, String tzId) -> {
            ZonedDateTime time = ZonedDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneId.of(tzId));
            return time.format(DateTimeFormatter.ofPattern(MonthName.MONTH_NAME_FORMAT, Locale.ROOT));
        });

        private final BiFunction<Long,String,String> apply;
        
        NameExtractor(BiFunction<Long,String,String> apply) {
            this.apply = apply;
        }

        public final String extract(Long millis, String tzId) {
            return apply.apply(millis, tzId);
        }
    }
    
    public static final String NAME = "ndt";
    
    private final NameExtractor extractor;

    public NamedDateTimeProcessor(NameExtractor extractor, TimeZone timeZone) {
        super(timeZone);
        this.extractor = extractor;
    }

    public NamedDateTimeProcessor(StreamInput in) throws IOException {
        super(in);
        extractor = in.readEnum(NameExtractor.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeEnum(extractor);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    NameExtractor extractor() {
        return extractor;
    }

    @Override
    public Object doProcess(long millis) {
        return extractor.extract(millis, timeZone().getID());
    }

    @Override
    public int hashCode() {
        return Objects.hash(extractor, timeZone());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        NamedDateTimeProcessor other = (NamedDateTimeProcessor) obj;
        return Objects.equals(extractor, other.extractor)
                && Objects.equals(timeZone(), other.timeZone());
    }

    @Override
    public String toString() {
        return extractor.toString();
    }
}
