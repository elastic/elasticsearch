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
import java.time.temporal.ChronoField;
import java.time.temporal.WeekFields;
import java.util.Objects;
import java.util.function.Function;

public class NonIsoDateTimeProcessor extends BaseDateTimeProcessor {
    
    public enum NonIsoDateTimeExtractor {
        DAY_OF_WEEK(zdt -> {
            // by ISO 8601 standard, Monday is the first day of the week and has the value 1
            // non-ISO 8601 standard considers Sunday as the first day of the week and value 1
            int dayOfWeek = zdt.get(ChronoField.DAY_OF_WEEK) + 1;
            return dayOfWeek == 8 ? 1 : dayOfWeek;
        }),
        WEEK_OF_YEAR(zdt -> {
            return zdt.get(WeekFields.SUNDAY_START.weekOfYear());
        });

        private final Function<ZonedDateTime, Integer> apply;

        NonIsoDateTimeExtractor(Function<ZonedDateTime, Integer> apply) {
            this.apply = apply;
        }

        public final Integer extract(ZonedDateTime dateTime) {
            return apply.apply(dateTime);
        }

        public final Integer extract(ZonedDateTime millis, String tzId) {
            return apply.apply(millis.withZoneSameInstant(ZoneId.of(tzId)));
        }
    }
    
    public static final String NAME = "nidt";

    private final NonIsoDateTimeExtractor extractor;

    public NonIsoDateTimeProcessor(NonIsoDateTimeExtractor extractor, ZoneId zoneId) {
        super(zoneId);
        this.extractor = extractor;
    }

    public NonIsoDateTimeProcessor(StreamInput in) throws IOException {
        super(in);
        extractor = in.readEnum(NonIsoDateTimeExtractor.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(extractor);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    NonIsoDateTimeExtractor extractor() {
        return extractor;
    }

    @Override
    public Object doProcess(ZonedDateTime dateTime) {
        return extractor.extract(dateTime);
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
        NonIsoDateTimeProcessor other = (NonIsoDateTimeProcessor) obj;
        return Objects.equals(extractor, other.extractor)
                && Objects.equals(zoneId(), other.zoneId());
    }

    @Override
    public String toString() {
        return extractor.toString();
    }
}
