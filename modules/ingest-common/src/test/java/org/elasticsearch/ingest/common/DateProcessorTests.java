/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.core.Strings;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.ingest.TestTemplateService;
import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.test.ESTestCase;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DateProcessorTests extends ESTestCase {

    private TemplateScript.Factory templatize(Locale locale) {
        return new TestTemplateService.MockTemplateScript.Factory(locale.getLanguage());
    }

    private TemplateScript.Factory templatize(ZoneId timezone) {
        return new TestTemplateService.MockTemplateScript.Factory(timezone.getId());
    }

    public void testJavaPattern() {
        DateProcessor dateProcessor = new DateProcessor(
            randomAlphaOfLength(10),
            null,
            templatize(ZoneId.of("Europe/Amsterdam")),
            templatize(Locale.ENGLISH),
            "date_as_string",
            List.of("yyyy dd MM HH:mm:ss"),
            "date_as_date"
        );
        Map<String, Object> document = new HashMap<>();
        document.put("date_as_string", "2010 12 06 11:05:15");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        dateProcessor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("date_as_date", String.class), equalTo("2010-06-12T11:05:15.000+02:00"));
    }

    public void testJavaPatternMultipleFormats() {
        List<String> matchFormats = new ArrayList<>();
        matchFormats.add("yyyy dd MM");
        matchFormats.add("dd/MM/yyyy");
        matchFormats.add("dd-MM-yyyy");
        DateProcessor dateProcessor = new DateProcessor(
            randomAlphaOfLength(10),
            null,
            templatize(ZoneId.of("Europe/Amsterdam")),
            templatize(Locale.ENGLISH),
            "date_as_string",
            matchFormats,
            "date_as_date"
        );

        Map<String, Object> document = new HashMap<>();
        document.put("date_as_string", "2010 12 06");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        dateProcessor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("date_as_date", String.class), equalTo("2010-06-12T00:00:00.000+02:00"));

        document = new HashMap<>();
        document.put("date_as_string", "12/06/2010");
        ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        dateProcessor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("date_as_date", String.class), equalTo("2010-06-12T00:00:00.000+02:00"));

        document = new HashMap<>();
        document.put("date_as_string", "12-06-2010");
        ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        dateProcessor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("date_as_date", String.class), equalTo("2010-06-12T00:00:00.000+02:00"));

        document = new HashMap<>();
        document.put("date_as_string", "2010");
        ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        try {
            dateProcessor.execute(ingestDocument);
            fail("processor should have failed due to not supported date format");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("unable to parse date [2010]"));
        }
    }

    public void testShortCircuitAdditionalPatternsAfterFirstMatchingPattern() {
        List<String> matchFormats = new ArrayList<>();
        matchFormats.add("invalid");
        matchFormats.add("uuuu-dd-MM");
        matchFormats.add("uuuu-MM-dd");
        DateProcessor dateProcessor = new DateProcessor(
            randomAlphaOfLength(10),
            null,
            templatize(ZoneId.of("Europe/Amsterdam")),
            templatize(Locale.ENGLISH),
            "date_as_string",
            matchFormats,
            "date_as_date"
        );

        Map<String, Object> document = new HashMap<>();
        document.put("date_as_string", "2010-03-04");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        dateProcessor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("date_as_date", String.class), equalTo("2010-04-03T00:00:00.000+02:00"));
    }

    public void testJavaPatternNoTimezone() {
        DateProcessor dateProcessor = new DateProcessor(
            randomAlphaOfLength(10),
            null,
            null,
            null,
            "date_as_string",
            List.of("yyyy dd MM HH:mm:ss XXX"),
            "date_as_date"
        );

        Map<String, Object> document = new HashMap<>();
        document.put("date_as_string", "2010 12 06 00:00:00 -02:00");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        dateProcessor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("date_as_date", String.class), equalTo("2010-06-12T02:00:00.000Z"));
    }

    public void testInvalidJavaPattern() {
        try {
            DateProcessor processor = new DateProcessor(
                randomAlphaOfLength(10),
                null,
                templatize(ZoneOffset.UTC),
                templatize(randomLocale(random())),
                "date_as_string",
                List.of("invalid pattern"),
                "date_as_date"
            );
            Map<String, Object> document = new HashMap<>();
            document.put("date_as_string", "2010");
            processor.execute(RandomDocumentPicks.randomIngestDocument(random(), document));
            fail("date processor execution should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("unable to parse date [2010]"));
            assertThat(e.getCause().getMessage(), equalTo("Invalid format: [invalid pattern]: Unknown pattern letter: i"));
        }
    }

    public void testJavaPatternLocale() {
        assumeFalse("Can't run in a FIPS JVM, Joda parse date error", inFipsJvm());
        DateProcessor dateProcessor = new DateProcessor(
            randomAlphaOfLength(10),
            null,
            templatize(ZoneId.of("Europe/Amsterdam")),
            templatize(Locale.ITALIAN),
            "date_as_string",
            List.of("yyyy dd MMMM"),
            "date_as_date"
        );
        Map<String, Object> document = new HashMap<>();
        document.put("date_as_string", "2010 12 giugno");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        dateProcessor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("date_as_date", String.class), equalTo("2010-06-12T00:00:00.000+02:00"));
    }

    public void testJavaPatternEnglishLocale() {
        // Since testJavaPatternLocale is muted in FIPS mode, test that we can correctly parse dates in english
        DateProcessor dateProcessor = new DateProcessor(
            randomAlphaOfLength(10),
            null,
            templatize(ZoneId.of("Europe/Amsterdam")),
            templatize(Locale.ENGLISH),
            "date_as_string",
            List.of("yyyy dd MMMM"),
            "date_as_date"
        );
        Map<String, Object> document = new HashMap<>();
        document.put("date_as_string", "2010 12 June");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        dateProcessor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("date_as_date", String.class), equalTo("2010-06-12T00:00:00.000+02:00"));
    }

    public void testJavaPatternDefaultYear() {
        String format = randomFrom("dd/MM", "8dd/MM");
        DateProcessor dateProcessor = new DateProcessor(
            randomAlphaOfLength(10),
            null,
            templatize(ZoneId.of("Europe/Amsterdam")),
            templatize(Locale.ENGLISH),
            "date_as_string",
            List.of(format),
            "date_as_date"
        );
        Map<String, Object> document = new HashMap<>();
        document.put("date_as_string", "12/06");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        dateProcessor.execute(ingestDocument);
        assertThat(
            ingestDocument.getFieldValue("date_as_date", String.class),
            equalTo(ZonedDateTime.now().getYear() + "-06-12T00:00:00.000+02:00")
        );
    }

    public void testTAI64N() {
        DateProcessor dateProcessor = new DateProcessor(
            randomAlphaOfLength(10),
            null,
            templatize(ZoneOffset.ofHours(2)),
            templatize(randomLocale(random())),
            "date_as_string",
            List.of("TAI64N"),
            "date_as_date"
        );
        Map<String, Object> document = new HashMap<>();
        String dateAsString = (randomBoolean() ? "@" : "") + "4000000050d506482dbdf024";
        document.put("date_as_string", dateAsString);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        dateProcessor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("date_as_date", String.class), equalTo("2012-12-22T03:00:46.767+02:00"));
    }

    public void testUnixMs() {
        DateProcessor dateProcessor = new DateProcessor(
            randomAlphaOfLength(10),
            null,
            templatize(ZoneOffset.UTC),
            templatize(randomLocale(random())),
            "date_as_string",
            List.of("UNIX_MS"),
            "date_as_date"
        );
        Map<String, Object> document = new HashMap<>();
        document.put("date_as_string", "1000500");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        dateProcessor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("date_as_date", String.class), equalTo("1970-01-01T00:16:40.500Z"));

        document = new HashMap<>();
        document.put("date_as_string", 1000500L);
        ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        dateProcessor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("date_as_date", String.class), equalTo("1970-01-01T00:16:40.500Z"));
    }

    public void testUnix() {
        DateProcessor dateProcessor = new DateProcessor(
            randomAlphaOfLength(10),
            null,
            templatize(ZoneOffset.UTC),
            templatize(randomLocale(random())),
            "date_as_string",
            List.of("UNIX"),
            "date_as_date"
        );
        Map<String, Object> document = new HashMap<>();
        document.put("date_as_string", "1000.5");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        dateProcessor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("date_as_date", String.class), equalTo("1970-01-01T00:16:40.500Z"));
    }

    public void testEpochMillis() {
        DateProcessor dateProcessor = new DateProcessor(
            randomAlphaOfLength(10),
            null,
            templatize(ZoneOffset.UTC),
            templatize(randomLocale(random())),
            "date_as_string",
            List.of("epoch_millis"),
            "date_as_date"
        );
        Map<String, Object> document = new HashMap<>();
        document.put("date_as_string", "1683701716065");
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        dateProcessor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("date_as_date", String.class), equalTo("2023-05-10T06:55:16.065Z"));

        document = new HashMap<>();
        document.put("date_as_string", 1683701716065L);
        ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        dateProcessor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("date_as_date", String.class), equalTo("2023-05-10T06:55:16.065Z"));
    }

    public void testInvalidTimezone() {
        DateProcessor processor = new DateProcessor(
            randomAlphaOfLength(10),
            null,
            new TestTemplateService.MockTemplateScript.Factory("invalid_timezone"),
            templatize(randomLocale(random())),
            "date_as_string",
            List.of("yyyy"),
            "date_as_date"
        );
        Map<String, Object> document = new HashMap<>();
        document.put("date_as_string", "2010");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> processor.execute(RandomDocumentPicks.randomIngestDocument(random(), document))
        );
        assertThat(e.getMessage(), equalTo("unable to parse date [2010]"));
        assertThat(e.getCause().getMessage(), equalTo("Unknown time-zone ID: invalid_timezone"));
    }

    public void testInvalidLocale() {
        DateProcessor processor = new DateProcessor(
            randomAlphaOfLength(10),
            null,
            templatize(ZoneOffset.UTC),
            new TestTemplateService.MockTemplateScript.Factory("invalid_locale"),
            "date_as_string",
            List.of("yyyy"),
            "date_as_date"
        );
        Map<String, Object> document = new HashMap<>();
        document.put("date_as_string", "2010");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> processor.execute(RandomDocumentPicks.randomIngestDocument(random(), document))
        );
        assertThat(e.getMessage(), equalTo("unable to parse date [2010]"));
        assertThat(e.getCause().getMessage(), equalTo("Unknown language: invalid"));
    }

    public void testOutputFormat() {
        long nanosAfterEpoch = randomLongBetween(1, 999999);
        DateProcessor processor = new DateProcessor(
            randomAlphaOfLength(10),
            null,
            null,
            null,
            "date_as_string",
            List.of("iso8601"),
            "date_as_date",
            "HH:mm:ss.SSSSSSSSS"
        );
        Map<String, Object> document = new HashMap<>();
        document.put("date_as_string", Instant.EPOCH.plusNanos(nanosAfterEpoch).toString());
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        processor.execute(ingestDocument);
        // output format is time only with nanosecond precision
        String expectedDate = "00:00:00." + Strings.format("%09d", nanosAfterEpoch);
        assertThat(ingestDocument.getFieldValue("date_as_date", String.class), equalTo(expectedDate));
    }

    @SuppressWarnings("unchecked")
    public void testCacheIsEvictedAfterReachMaxCapacity() {
        Supplier<Function<String, ZonedDateTime>> supplier1 = mock(Supplier.class);
        Supplier<Function<String, ZonedDateTime>> supplier2 = mock(Supplier.class);
        Function<String, ZonedDateTime> zonedDateTimeFunction1 = str -> ZonedDateTime.now();
        Function<String, ZonedDateTime> zonedDateTimeFunction2 = str -> ZonedDateTime.now();
        var cache = new DateProcessor.Cache(1);
        var key1 = new DateProcessor.Cache.Key("format-1", ZoneId.systemDefault().toString(), Locale.ROOT.toString());
        var key2 = new DateProcessor.Cache.Key("format-2", ZoneId.systemDefault().toString(), Locale.ROOT.toString());

        when(supplier1.get()).thenReturn(zonedDateTimeFunction1);
        when(supplier2.get()).thenReturn(zonedDateTimeFunction2);

        assertEquals(cache.getOrCompute(key1, supplier1), zonedDateTimeFunction1); // 1 call to supplier1
        assertEquals(cache.getOrCompute(key2, supplier2), zonedDateTimeFunction2); // 1 call to supplier2
        assertEquals(cache.getOrCompute(key1, supplier1), zonedDateTimeFunction1); // 1 more call to supplier1
        assertEquals(cache.getOrCompute(key1, supplier1), zonedDateTimeFunction1); // should use cached value
        assertEquals(cache.getOrCompute(key2, supplier2), zonedDateTimeFunction2); // 1 more call to supplier2
        assertEquals(cache.getOrCompute(key2, supplier2), zonedDateTimeFunction2); // should use cached value
        assertEquals(cache.getOrCompute(key2, supplier2), zonedDateTimeFunction2); // should use cached value
        assertEquals(cache.getOrCompute(key2, supplier2), zonedDateTimeFunction2); // should use cached value
        assertEquals(cache.getOrCompute(key1, supplier1), zonedDateTimeFunction1); // 1 more to call to supplier1

        verify(supplier1, times(3)).get();
        verify(supplier2, times(2)).get();
    }
}
