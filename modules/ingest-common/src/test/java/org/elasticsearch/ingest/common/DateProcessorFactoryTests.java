/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ingest.TestTemplateService;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
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
        config.put("formats", Collections.singletonList("dd/MM/yyyyy"));
        String processorTag = randomAlphaOfLength(10);
        DateProcessor processor = factory.create(null, processorTag, null, config);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), equalTo(sourceField));
        assertThat(processor.getTargetField(), equalTo(DateProcessor.DEFAULT_TARGET_FIELD));
        assertThat(processor.getFormats(), equalTo(Collections.singletonList("dd/MM/yyyyy")));
        assertNull(processor.getLocale());
        assertNull(processor.getTimezone());
    }

    public void testMatchFieldIsMandatory() throws Exception {
        Map<String, Object> config = new HashMap<>();
        String targetField = randomAlphaOfLengthBetween(1, 10);
        config.put("target_field", targetField);
        config.put("formats", Collections.singletonList("dd/MM/yyyyy"));

        try {
            factory.create(null, null, null, config);
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
            factory.create(null, null, null, config);
            fail("processor creation should have failed");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), containsString("[formats] required property is missing"));
        }
    }

    public void testParseLocale() throws Exception {
        Map<String, Object> config = new HashMap<>();
        String sourceField = randomAlphaOfLengthBetween(1, 10);
        config.put("field", sourceField);
        config.put("formats", Collections.singletonList("dd/MM/yyyyy"));
        Locale locale = randomFrom(Locale.GERMANY, Locale.FRENCH, Locale.ROOT);
        config.put("locale", locale.toLanguageTag());

        DateProcessor processor = factory.create(null, null, null, config);
        assertThat(processor.getLocale().newInstance(Collections.emptyMap()).execute(), equalTo(locale.toLanguageTag()));
    }

    public void testParseTimezone() throws Exception {
        Map<String, Object> config = new HashMap<>();
        String sourceField = randomAlphaOfLengthBetween(1, 10);
        config.put("field", sourceField);
        config.put("formats", Collections.singletonList("dd/MM/yyyyy"));

        ZoneId timezone = randomZone();
        config.put("timezone", timezone.getId());
        DateProcessor processor = factory.create(null, null, null, config);
        assertThat(processor.getTimezone().newInstance(Collections.emptyMap()).execute(), equalTo(timezone.getId()));
    }

    public void testParseMatchFormats() throws Exception {
        Map<String, Object> config = new HashMap<>();
        String sourceField = randomAlphaOfLengthBetween(1, 10);
        config.put("field", sourceField);
        config.put("formats", Arrays.asList("dd/MM/yyyy", "dd-MM-yyyy"));

        DateProcessor processor = factory.create(null, null, null, config);
        assertThat(processor.getFormats(), equalTo(Arrays.asList("dd/MM/yyyy", "dd-MM-yyyy")));
    }

    public void testParseMatchFormatsFailure() throws Exception {
        Map<String, Object> config = new HashMap<>();
        String sourceField = randomAlphaOfLengthBetween(1, 10);
        config.put("field", sourceField);
        config.put("formats", "dd/MM/yyyy");

        try {
            factory.create(null, null, null, config);
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
        config.put("formats", Arrays.asList("dd/MM/yyyy", "dd-MM-yyyy"));

        DateProcessor processor = factory.create(null, null, null, config);
        assertThat(processor.getTargetField(), equalTo(targetField));
    }

    public void testParseOutputFormat() throws Exception {
        final String outputFormat = "dd:MM:yyyy";
        Map<String, Object> config = new HashMap<>();
        String sourceField = randomAlphaOfLengthBetween(1, 10);
        String targetField = randomAlphaOfLengthBetween(1, 10);
        config.put("field", sourceField);
        config.put("target_field", targetField);
        config.put("formats", Arrays.asList("dd/MM/yyyy", "dd-MM-yyyy"));
        config.put("output_format", outputFormat);
        DateProcessor processor = factory.create(null, null, null, config);
        assertThat(processor.getOutputFormat(), equalTo(outputFormat));
    }

    public void testDefaultOutputFormat() throws Exception {
        Map<String, Object> config = new HashMap<>();
        String sourceField = randomAlphaOfLengthBetween(1, 10);
        String targetField = randomAlphaOfLengthBetween(1, 10);
        config.put("field", sourceField);
        config.put("target_field", targetField);
        config.put("formats", Arrays.asList("dd/MM/yyyy", "dd-MM-yyyy"));
        DateProcessor processor = factory.create(null, null, null, config);
        assertThat(processor.getOutputFormat(), equalTo(DateProcessor.DEFAULT_OUTPUT_FORMAT));
    }

    public void testInvalidOutputFormatRejected() throws Exception {
        final String outputFormat = "invalid_date_format";
        Map<String, Object> config = new HashMap<>();
        String sourceField = randomAlphaOfLengthBetween(1, 10);
        String targetField = randomAlphaOfLengthBetween(1, 10);
        config.put("field", sourceField);
        config.put("target_field", targetField);
        config.put("formats", Arrays.asList("dd/MM/yyyy", "dd-MM-yyyy"));
        config.put("output_format", outputFormat);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> factory.create(null, null, null, config));
        assertThat(e.getMessage(), containsString("invalid output format [" + outputFormat + "]"));
    }
}
