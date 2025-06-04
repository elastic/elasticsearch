/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.ingest.TestTemplateService;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class DateProcessorFactoryTests extends ESTestCase {

    private DateProcessor.Factory factory;

    @Before
    public void init() {
        factory = new DateProcessor.Factory(TestTemplateService.instance());
    }

    public void testBuildDefaults() throws Exception {
        Map<String, Object> config = new HashMap<>();
        String sourceField = randomAlphaOfLengthBetween(1, 10);
        config.put("field", sourceField);
        config.put("formats", List.of("dd/MM/yyyyy"));
        String processorTag = randomAlphaOfLength(10);
        DateProcessor processor = factory.create(null, processorTag, null, config, null);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), equalTo(sourceField));
        assertThat(processor.getTargetField(), equalTo(DateProcessor.DEFAULT_TARGET_FIELD));
        assertThat(processor.getFormats(), equalTo(List.of("dd/MM/yyyyy")));
        assertThat(processor.getTimezone(null), equalTo(ZoneOffset.UTC));
        assertThat(processor.getLocale(null), equalTo(Locale.ENGLISH));
    }

    public void testMatchFieldIsMandatory() throws Exception {
        Map<String, Object> config = new HashMap<>();
        String targetField = randomAlphaOfLengthBetween(1, 10);
        config.put("target_field", targetField);
        config.put("formats", List.of("dd/MM/yyyyy"));

        try {
            factory.create(null, null, null, config, null);
            fail("processor creation should have failed");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), containsString("[field] required property is missing"));
        }
    }

    public void testMatchFormatsIsMandatory() throws Exception {
        Map<String, Object> config = new HashMap<>();
        String sourceField = randomAlphaOfLengthBetween(1, 10);
        String targetField = randomAlphaOfLengthBetween(1, 10);
        config.put("field", sourceField);
        config.put("target_field", targetField);

        try {
            factory.create(null, null, null, config, null);
            fail("processor creation should have failed");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), containsString("[formats] required property is missing"));
        }
    }

    public void testParseLocale() throws Exception {
        Map<String, Object> config = new HashMap<>();
        String sourceField = randomAlphaOfLengthBetween(1, 10);
        config.put("field", sourceField);
        config.put("formats", List.of("dd/MM/yyyyy"));
        Locale locale = randomFrom(Locale.GERMANY, Locale.FRENCH, Locale.CANADA);
        config.put("locale", locale.toLanguageTag());

        DateProcessor processor = factory.create(null, null, null, config, null);
        IngestDocument document = RandomDocumentPicks.randomIngestDocument(random(), Map.of());
        assertThat(processor.getLocale(document), equalTo(locale));
    }

    public void testParseTimezone() throws Exception {
        Map<String, Object> config = new HashMap<>();
        String sourceField = randomAlphaOfLengthBetween(1, 10);
        config.put("field", sourceField);
        config.put("formats", List.of("dd/MM/yyyyy"));

        ZoneId timezone = randomZone();
        config.put("timezone", timezone.getId());
        DateProcessor processor = factory.create(null, null, null, config, null);
        IngestDocument document = RandomDocumentPicks.randomIngestDocument(random(), Map.of());
        assertThat(processor.getTimezone(document), equalTo(timezone));
    }

    public void testParseMatchFormats() throws Exception {
        Map<String, Object> config = new HashMap<>();
        String sourceField = randomAlphaOfLengthBetween(1, 10);
        config.put("field", sourceField);
        config.put("formats", List.of("dd/MM/yyyy", "dd-MM-yyyy"));

        DateProcessor processor = factory.create(null, null, null, config, null);
        assertThat(processor.getFormats(), equalTo(List.of("dd/MM/yyyy", "dd-MM-yyyy")));
    }

    public void testParseMatchFormatsFailure() throws Exception {
        Map<String, Object> config = new HashMap<>();
        String sourceField = randomAlphaOfLengthBetween(1, 10);
        config.put("field", sourceField);
        config.put("formats", "dd/MM/yyyy");

        try {
            factory.create(null, null, null, config, null);
            fail("processor creation should have failed");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), containsString("[formats] property isn't a list, but of type [java.lang.String]"));
        }
    }

    public void testParseTargetField() throws Exception {
        Map<String, Object> config = new HashMap<>();
        String sourceField = randomAlphaOfLengthBetween(1, 10);
        String targetField = randomAlphaOfLengthBetween(1, 10);
        config.put("field", sourceField);
        config.put("target_field", targetField);
        config.put("formats", List.of("dd/MM/yyyy", "dd-MM-yyyy"));

        DateProcessor processor = factory.create(null, null, null, config, null);
        assertThat(processor.getTargetField(), equalTo(targetField));
    }

    public void testParseOutputFormat() throws Exception {
        final String outputFormat = "dd:MM:yyyy";
        Map<String, Object> config = new HashMap<>();
        String sourceField = randomAlphaOfLengthBetween(1, 10);
        String targetField = randomAlphaOfLengthBetween(1, 10);
        config.put("field", sourceField);
        config.put("target_field", targetField);
        config.put("formats", List.of("dd/MM/yyyy", "dd-MM-yyyy"));
        config.put("output_format", outputFormat);
        DateProcessor processor = factory.create(null, null, null, config, null);
        assertThat(processor.getOutputFormat(), equalTo(outputFormat));
    }

    public void testDefaultOutputFormat() throws Exception {
        Map<String, Object> config = new HashMap<>();
        String sourceField = randomAlphaOfLengthBetween(1, 10);
        String targetField = randomAlphaOfLengthBetween(1, 10);
        config.put("field", sourceField);
        config.put("target_field", targetField);
        config.put("formats", List.of("dd/MM/yyyy", "dd-MM-yyyy"));
        DateProcessor processor = factory.create(null, null, null, config, null);
        assertThat(processor.getOutputFormat(), equalTo(DateProcessor.DEFAULT_OUTPUT_FORMAT));
    }

    public void testInvalidOutputFormatRejected() throws Exception {
        final String outputFormat = "invalid_date_format";
        Map<String, Object> config = new HashMap<>();
        String sourceField = randomAlphaOfLengthBetween(1, 10);
        String targetField = randomAlphaOfLengthBetween(1, 10);
        config.put("field", sourceField);
        config.put("target_field", targetField);
        config.put("formats", List.of("dd/MM/yyyy", "dd-MM-yyyy"));
        config.put("output_format", outputFormat);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> factory.create(null, null, null, config, null));
        assertThat(e.getMessage(), containsString("invalid output format [" + outputFormat + "]"));
    }
}
