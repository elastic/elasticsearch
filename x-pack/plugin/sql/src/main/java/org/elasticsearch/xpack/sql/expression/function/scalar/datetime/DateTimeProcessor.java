/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.joda.time.DateTime;
import org.joda.time.DateTimeFieldType;
import org.joda.time.DateTimeZone;
import org.joda.time.ReadableDateTime;

import java.io.IOException;
import java.util.Objects;
import java.util.TimeZone;

public class DateTimeProcessor extends BaseDateTimeProcessor {
    
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

    public DateTimeProcessor(DateTimeExtractor extractor, TimeZone timeZone) {
        super(timeZone);
        this.extractor = extractor;
    }

    public DateTimeProcessor(StreamInput in) throws IOException {
        super(in);
        extractor = in.readEnum(DateTimeExtractor.class);
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

    DateTimeExtractor extractor() {
        return extractor;
    }

    @Override
    public Object doProcess(long millis) {
        ReadableDateTime dt = new DateTime(millis, DateTimeZone.forTimeZone(timeZone()));

        return extractor.extract(dt);
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
        DateTimeProcessor other = (DateTimeProcessor) obj;
        return Objects.equals(extractor, other.extractor)
                && Objects.equals(timeZone(), other.timeZone());
    }

    @Override
    public String toString() {
        return extractor.toString();
    }
}
