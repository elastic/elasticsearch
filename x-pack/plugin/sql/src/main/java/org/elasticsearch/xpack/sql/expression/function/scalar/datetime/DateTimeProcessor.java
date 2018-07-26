/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.runtime.Processor;
import org.joda.time.DateTime;
import org.joda.time.DateTimeFieldType;
import org.joda.time.DateTimeZone;
import org.joda.time.ReadableDateTime;
import org.joda.time.ReadableInstant;

import java.io.IOException;
import java.util.Objects;
import java.util.TimeZone;

public class DateTimeProcessor implements Processor {
    
    public enum DateTimeExtractor {
        DAY_OF_MONTH(DateTimeFieldType.dayOfMonth()),
        DAY_OF_WEEK(DateTimeFieldType.dayOfWeek()),
        DAY_OF_YEAR(DateTimeFieldType.dayOfYear()),
        HOUR_OF_DAY(DateTimeFieldType.hourOfDay()),
        MINUTE_OF_DAY(DateTimeFieldType.minuteOfDay()),
        MINUTE_OF_HOUR(DateTimeFieldType.minuteOfHour()),
        MONTH_OF_YEAR(DateTimeFieldType.monthOfYear()),
        SECOND_OF_MINUTE(DateTimeFieldType.secondOfMinute()),
        WEEK_OF_YEAR(DateTimeFieldType.weekOfWeekyear()),
        YEAR(DateTimeFieldType.year());

        private final DateTimeFieldType field;

        DateTimeExtractor(DateTimeFieldType field) {
            this.field = field;
        }

        public int extract(ReadableDateTime dt) {
            return dt.get(field);
        }
    }
    
    public static final String NAME = "dt";

    private final DateTimeExtractor extractor;
    private final TimeZone timeZone;

    public DateTimeProcessor(DateTimeExtractor extractor, TimeZone timeZone) {
        this.extractor = extractor;
        this.timeZone = timeZone;
    }

    public DateTimeProcessor(StreamInput in) throws IOException {
        extractor = in.readEnum(DateTimeExtractor.class);
        timeZone = TimeZone.getTimeZone(in.readString());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(extractor);
        out.writeString(timeZone.getID());
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    DateTimeExtractor extractor() {
        return extractor;
    }

    @Override
    public Object process(Object l) {
        if (l == null) {
            return null;
        }

        ReadableDateTime dt;
        if (l instanceof String) {
            // 6.4+
            final long millis = Long.parseLong(l.toString());
            dt = new DateTime(millis, DateTimeZone.forTimeZone(timeZone));
        } else if (l instanceof ReadableInstant) {
            // 6.3-
            dt = (ReadableDateTime) l;
            if (!TimeZone.getTimeZone("UTC").equals(timeZone)) {
                dt = dt.toDateTime().withZone(DateTimeZone.forTimeZone(timeZone));
            }
        } else {
            throw new SqlIllegalArgumentException("A string or a date is required; received {}", l);
        }

        return extractor.extract(dt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(extractor, timeZone);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        DateTimeProcessor other = (DateTimeProcessor) obj;
        return Objects.equals(extractor, other.extractor)
                && Objects.equals(timeZone, other.timeZone);
    }

    @Override
    public String toString() {
        return extractor.toString();
    }
}
