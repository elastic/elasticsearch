/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.IsoFields;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalField;
import java.util.Objects;

public class DateTimeProcessor extends BaseDateTimeProcessor {

    public enum DateTimeExtractor {
        DAY_OF_MONTH(ChronoField.DAY_OF_MONTH),
        ISO_DAY_OF_WEEK(ChronoField.DAY_OF_WEEK),
        DAY_OF_YEAR(ChronoField.DAY_OF_YEAR),
        HOUR_OF_DAY(ChronoField.HOUR_OF_DAY),
        MINUTE_OF_DAY(ChronoField.MINUTE_OF_DAY),
        MINUTE_OF_HOUR(ChronoField.MINUTE_OF_HOUR),
        MONTH_OF_YEAR(ChronoField.MONTH_OF_YEAR),
        SECOND_OF_MINUTE(ChronoField.SECOND_OF_MINUTE),
        ISO_WEEK_OF_YEAR(IsoFields.WEEK_OF_WEEK_BASED_YEAR),
        YEAR(ChronoField.YEAR);

        private final TemporalField field;

        DateTimeExtractor(TemporalField field) {
            this.field = field;
        }

        public int extract(Temporal time) {
            return time.get(field);
        }
    }

    public static final String NAME = "dt";
    private final DateTimeExtractor extractor;

    public DateTimeProcessor(DateTimeExtractor extractor, ZoneId zoneId) {
        super(zoneId);
        this.extractor = extractor;
    }

    public DateTimeProcessor(StreamInput in) throws IOException {
        super(in);
        extractor = in.readEnum(DateTimeExtractor.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
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
    public Object doProcess(ZonedDateTime dateTime) {
        return extractor.extract(dateTime);
    }

    public static Integer doProcess(ZonedDateTime dateTime, String tzId, String extractorName) {
        ZonedDateTime zdt = dateTime.withZoneSameInstant(ZoneId.of(tzId));
        return doProcess(zdt, extractorName);
    }

    protected static Integer doProcess(Temporal dateTime, String extractorName) {
        return DateTimeExtractor.valueOf(extractorName).extract(dateTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(extractor, zoneId());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        DateTimeProcessor other = (DateTimeProcessor) obj;
        return Objects.equals(extractor, other.extractor) && Objects.equals(zoneId(), other.zoneId());
    }

    @Override
    public String toString() {
        return extractor.toString();
    }
}
