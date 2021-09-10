/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.xcontent;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.script.JodaCompatibleZonedDateTime;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.joda.time.MutableDateTime;
import org.joda.time.ReadableInstant;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.joda.time.tz.CachedDateTimeZone;
import org.joda.time.tz.FixedDateTimeZone;

import java.time.DayOfWeek;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.MonthDay;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.Year;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * SPI extensions for Elasticsearch-specific classes (like the Lucene or Joda
 * dependency classes) that need to be encoded by {@link XContentBuilder} in a
 * specific way.
 */
public class XContentElasticsearchExtension implements XContentBuilderExtension {

    public static final DateTimeFormatter DEFAULT_DATE_PRINTER = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC);
    public static final DateFormatter DEFAULT_FORMATTER = DateFormatter.forPattern("strict_date_optional_time_nanos");
    public static final DateFormatter LOCAL_TIME_FORMATTER = DateFormatter.forPattern("HH:mm:ss.SSS");
    public static final DateFormatter OFFSET_TIME_FORMATTER = DateFormatter.forPattern("HH:mm:ss.SSSZZZZZ");

    @Override
    public Map<Class<?>, XContentBuilder.Writer> getXContentWriters() {
        Map<Class<?>, XContentBuilder.Writer> writers = new HashMap<>();

        // Fully-qualified here to reduce ambiguity around our (ES') Version class
        writers.put(org.apache.lucene.util.Version.class, (b, v) -> b.value(Objects.toString(v)));
        writers.put(DateTimeZone.class, (b, v) -> b.value(Objects.toString(v)));
        writers.put(CachedDateTimeZone.class, (b, v) -> b.value(Objects.toString(v)));
        writers.put(FixedDateTimeZone.class, (b, v) -> b.value(Objects.toString(v)));
        writers.put(MutableDateTime.class, XContentBuilder::timeValue);
        writers.put(DateTime.class, XContentBuilder::timeValue);
        writers.put(TimeValue.class, (b, v) -> b.value(v.toString()));
        writers.put(ZonedDateTime.class, XContentBuilder::timeValue);
        writers.put(OffsetDateTime.class, XContentBuilder::timeValue);
        writers.put(OffsetTime.class, XContentBuilder::timeValue);
        writers.put(java.time.Instant.class, XContentBuilder::timeValue);
        writers.put(LocalDateTime.class, XContentBuilder::timeValue);
        writers.put(LocalDate.class, XContentBuilder::timeValue);
        writers.put(LocalTime.class, XContentBuilder::timeValue);
        writers.put(DayOfWeek.class, (b, v) -> b.value(v.toString()));
        writers.put(Month.class, (b, v) -> b.value(v.toString()));
        writers.put(MonthDay.class, (b, v) -> b.value(v.toString()));
        writers.put(Year.class, (b, v) -> b.value(v.toString()));
        writers.put(Duration.class, (b, v) -> b.value(v.toString()));
        writers.put(Period.class, (b, v) -> b.value(v.toString()));
        writers.put(JodaCompatibleZonedDateTime.class, XContentBuilder::timeValue);

        writers.put(BytesReference.class, (b, v) -> {
            if (v == null) {
                b.nullValue();
            } else {
                BytesRef bytes = ((BytesReference) v).toBytesRef();
                b.value(bytes.bytes, bytes.offset, bytes.length);
            }
        });

        writers.put(BytesRef.class, (b, v) -> {
            if (v == null) {
                b.nullValue();
            } else {
                BytesRef bytes = (BytesRef) v;
                b.value(bytes.bytes, bytes.offset, bytes.length);
            }
        });
        return writers;
    }

    @Override
    public Map<Class<?>, XContentBuilder.HumanReadableTransformer> getXContentHumanReadableTransformers() {
        Map<Class<?>, XContentBuilder.HumanReadableTransformer> transformers = new HashMap<>();
        transformers.put(TimeValue.class, v -> ((TimeValue) v).millis());
        transformers.put(ByteSizeValue.class, v -> ((ByteSizeValue) v).getBytes());
        return transformers;
    }

    @Override
    public Map<Class<?>, Function<Object, Object>> getDateTransformers() {
        Map<Class<?>, Function<Object, Object>> transformers = new HashMap<>();
        transformers.put(Date.class, d -> DEFAULT_DATE_PRINTER.print(((Date) d).getTime()));
        transformers.put(DateTime.class, d -> DEFAULT_DATE_PRINTER.print((DateTime) d));
        transformers.put(MutableDateTime.class, d -> DEFAULT_DATE_PRINTER.print((MutableDateTime) d));
        transformers.put(ReadableInstant.class, d -> DEFAULT_DATE_PRINTER.print((ReadableInstant) d));
        transformers.put(Long.class, d -> DEFAULT_DATE_PRINTER.print((long) d));
        transformers.put(Calendar.class, d -> DEFAULT_DATE_PRINTER.print(((Calendar) d).getTimeInMillis()));
        transformers.put(GregorianCalendar.class, d -> DEFAULT_DATE_PRINTER.print(((Calendar) d).getTimeInMillis()));
        transformers.put(Instant.class, d -> DEFAULT_DATE_PRINTER.print((Instant) d));
        transformers.put(ZonedDateTime.class, d -> DEFAULT_FORMATTER.format((ZonedDateTime) d));
        transformers.put(OffsetDateTime.class, d -> DEFAULT_FORMATTER.format((OffsetDateTime) d));
        transformers.put(OffsetTime.class, d -> OFFSET_TIME_FORMATTER.format((OffsetTime) d));
        transformers.put(LocalDateTime.class, d -> DEFAULT_FORMATTER.format((LocalDateTime) d));
        transformers.put(java.time.Instant.class,
            d -> DEFAULT_FORMATTER.format(ZonedDateTime.ofInstant((java.time.Instant) d, ZoneOffset.UTC)));
        transformers.put(LocalDate.class, d -> ((LocalDate) d).toString());
        transformers.put(LocalTime.class, d -> LOCAL_TIME_FORMATTER.format((LocalTime) d));
        transformers.put(JodaCompatibleZonedDateTime.class,
            d -> DEFAULT_FORMATTER.format(((JodaCompatibleZonedDateTime) d).getZonedDateTime()));
        return transformers;
    }
}
