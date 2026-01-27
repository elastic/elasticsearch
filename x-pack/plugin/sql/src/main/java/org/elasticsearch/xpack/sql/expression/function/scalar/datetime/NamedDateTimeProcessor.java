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
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Objects;
import java.util.function.Function;

public class NamedDateTimeProcessor extends BaseDateTimeProcessor {

    public enum NameExtractor {
        // for the moment we'll use no specific Locale, but we might consider introducing a Locale parameter, just like the timeZone one
        DAY_NAME(time -> time.format(DAY_NAME_FORMATTER)),
        MONTH_NAME(time -> time.format(MONTH_NAME_FORMATTER));

        private final Function<ZonedDateTime, String> apply;

        NameExtractor(Function<ZonedDateTime, String> apply) {
            this.apply = apply;
        }

        public final String extract(ZonedDateTime dateTime) {
            return apply.apply(dateTime);
        }

        public final String extract(ZonedDateTime millis, String tzId) {
            return apply.apply(millis.withZoneSameInstant(ZoneId.of(tzId)));
        }
    }

    public static final String NAME = "ndt";
    private static final DateTimeFormatter DAY_NAME_FORMATTER = DateTimeFormatter.ofPattern("EEEE", Locale.ENGLISH);
    private static final DateTimeFormatter MONTH_NAME_FORMATTER = DateTimeFormatter.ofPattern("MMMM", Locale.ENGLISH);

    private final NameExtractor extractor;

    public NamedDateTimeProcessor(NameExtractor extractor, ZoneId zoneId) {
        super(zoneId);
        this.extractor = extractor;
    }

    public NamedDateTimeProcessor(StreamInput in) throws IOException {
        super(in);
        extractor = in.readEnum(NameExtractor.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
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
        NamedDateTimeProcessor other = (NamedDateTimeProcessor) obj;
        return Objects.equals(extractor, other.extractor) && Objects.equals(zoneId(), other.zoneId());
    }

    @Override
    public String toString() {
        return extractor.toString();
    }
}
