/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.LocaleUtils;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.function.FunctionContext;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.runtime.Processor;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.runtime.VarArgsProcessor;
import org.joda.time.DateTime;
import org.joda.time.DateTimeFieldType;
import org.joda.time.DateTimeZone;
import org.joda.time.ReadableDateTime;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.TimeZone;

public class DateTimeProcessor extends VarArgsProcessor {

    private final static TimeZone UTC = TimeZone.getTimeZone("UTC");

    public enum DateTimeExtractor {
        DAY_OF_MONTH(DateTimeFieldType.dayOfMonth()),
        DAY_OF_WEEK(DateTimeFieldType.dayOfWeek()),
        DAY_OF_YEAR(DateTimeFieldType.dayOfYear()),
        HOUR_OF_DAY(DateTimeFieldType.hourOfDay()),
        MINUTE_OF_DAY(DateTimeFieldType.minuteOfDay()),
        MINUTE_OF_HOUR(DateTimeFieldType.minuteOfHour()),
        MONTH_OF_YEAR(DateTimeFieldType.monthOfYear()),
        SECOND_OF_MINUTE(DateTimeFieldType.secondOfMinute()){
            // no overload involving timezone
            int maxArgumentCount() {
                return 1;
            }
        },
        WEEK_OF_YEAR(DateTimeFieldType.weekOfWeekyear()),
        YEAR(DateTimeFieldType.year()),
        DAYNAME(DateTimeFieldType.dayOfWeek()) {

            int maxArgumentCount() {
                return 3;
            }

            public Object extract(ReadableDateTime dateTime, Locale locale) {
                final Instant instant = Instant.ofEpochMilli(dateTime.getMillis());
                final ZoneId zoneId = ZoneId.of(dateTime.getZone().getID(), ZoneId.SHORT_IDS);
                final ZonedDateTime zdt = ZonedDateTime.ofInstant(instant, zoneId);

                return zdt.format(DateTimeFormatter.ofPattern(DayName.FORMAT, locale)); // long form name of week.
            }
        };

        final DateTimeFieldType field;

        DateTimeExtractor(DateTimeFieldType field) {
            this.field = field;
        }

        public void checkArgumentCount(final List<? extends Object> arguments) {
            final int count = arguments.size();
            final int max = this.maxArgumentCount();
            if(count < 1 || count > max) {
                throw new IllegalArgumentException("Expected between 1 and " + max + " parameters but got " +
                    count + "=" + arguments);
            }
        }

        int maxArgumentCount() {
            return 2;
        }

        public Object extract(ReadableDateTime dt, Locale locale) {
            return dt.get(field);
        }
    }

    public static final String NAME = "dt";

    private final DateTimeExtractor extractor;
    private final FunctionContext context;

    public DateTimeProcessor(List<Processor> arguments, DateTimeExtractor extractor, FunctionContext context) {
        super(arguments);
        extractor.checkArgumentCount(arguments);
        this.extractor = extractor;
        this.context = context;
    }

    public DateTimeProcessor(StreamInput in) throws IOException {
        super(in);
        extractor = in.readEnum(DateTimeExtractor.class);
        context = FunctionContext.read(in);
    }

    @Override
    protected void writeToAdditional(StreamOutput out) throws IOException {
        out.writeEnum(extractor);
        this.context.writeTo(out);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected Object doProcess(List<Object> parameters) {
        ReadableDateTime dateTime = null;

        FunctionContext context = this.context;
        TimeZone timeZone = context.timeZone();
        Object locale = context.locale();

        final int count = parameters.size();
        for(int i = 0; i < count; i++) {
            final Object p = parameters.get(i);
            switch(i) {
                case 0:
                    if (!(p instanceof ReadableDateTime)) {
                        throw new SqlIllegalArgumentException("First parameter date/time is required; received {}", p);
                    }
                    dateTime = (ReadableDateTime)p;
                    break;
                case 1:
                    if (!(p instanceof String)) {
                        throw new SqlIllegalArgumentException("2nd parameter date/time is required; received {}", p);
                    }
                    timeZone = TimeZone.getTimeZone((String)p);
                    break;
                case 2:
                    if (!(p instanceof String)) {
                        throw new SqlIllegalArgumentException("3rd parameter string is required; received {}", p);
                    }
                    locale = LocaleUtils.parse((String)p);
                    break;
            }
        }

        if (!UTC.equals(timeZone)) {
            dateTime = dateTime.toDateTime().withZone(DateTimeZone.forTimeZone(timeZone));
        }

        return extractor().extract((DateTime)dateTime, (Locale) locale);
    }

    DateTimeExtractor extractor() {
        return extractor;
    }

    @Override
    protected int hashCode(List<Processor> processors) {
        return Objects.hash(processors, this.extractor, this.context);
    }

    @Override
    protected boolean equals0(VarArgsProcessor other) {
        return equals1((DateTimeProcessor)other);
    }

    private boolean equals1(DateTimeProcessor other) {
        return this.extractor.equals(other.extractor) &&
            this.context.equals(other.context);
    }

    @Override
    public String toString() {
        return extractor.toString();
    }
}
