/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.test.ESTestCase;
import org.joda.time.DateTimeZone;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class DateProcessorFactoryTests extends ESTestCase {

    public void testBuildDefaults() throws Exception {
        DateProcessor.Factory factory = new DateProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        String sourceField = randomAlphaOfLengthBetween(1, 10);
        config.put("field", sourceField);
        config.put("formats", Collections.singletonList("dd/MM/yyyyy"));
        String processorTag = randomAlphaOfLength(10);
        DateProcessor processor = factory.create(null, processorTag, config);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), equalTo(sourceField));
        assertThat(processor.getTargetField(), equalTo(DateProcessor.DEFAULT_TARGET_FIELD));
        assertThat(processor.getFormats(), equalTo(Collections.singletonList("dd/MM/yyyyy")));
        assertThat(processor.getLocale(), equalTo(Locale.ROOT));
        assertThat(processor.getTimezone(), equalTo(DateTimeZone.UTC));
    }

    public void testMatchFieldIsMandatory() throws Exception {
        DateProcessor.Factory factory = new DateProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        String targetField = randomAlphaOfLengthBetween(1, 10);
        config.put("target_field", targetField);
        config.put("formats", Collections.singletonList("dd/MM/yyyyy"));

        try {
            factory.create(null, null, config);
            fail("processor creation should have failed");
        } catch(ElasticsearchParseException e) {
            assertThat(e.getMessage(), containsString("[field] required property is missing"));
        }
    }

    public void testMatchFormatsIsMandatory() throws Exception {
        DateProcessor.Factory factory = new DateProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        String sourceField = randomAlphaOfLengthBetween(1, 10);
        String targetField = randomAlphaOfLengthBetween(1, 10);
        config.put("field", sourceField);
        config.put("target_field", targetField);

        try {
            factory.create(null, null, config);
            fail("processor creation should have failed");
        } catch(ElasticsearchParseException e) {
            assertThat(e.getMessage(), containsString("[formats] required property is missing"));
        }
    }

    public void testParseLocale() throws Exception {
        DateProcessor.Factory factory = new DateProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        String sourceField = randomAlphaOfLengthBetween(1, 10);
        config.put("field", sourceField);
        config.put("formats", Collections.singletonList("dd/MM/yyyyy"));
        Locale locale = randomFrom(Locale.GERMANY, Locale.FRENCH, Locale.ROOT);
        config.put("locale", locale.toLanguageTag());

        DateProcessor processor = factory.create(null, null, config);
        assertThat(processor.getLocale().toLanguageTag(), equalTo(locale.toLanguageTag()));
    }

    public void testParseInvalidLocale() throws Exception {
        String[] locales = new String[] { "invalid_locale", "english", "xy", "xy-US" };
        for (String locale : locales) {
            DateProcessor.Factory factory = new DateProcessor.Factory();
            Map<String, Object> config = new HashMap<>();
            String sourceField = randomAlphaOfLengthBetween(1, 10);
            config.put("field", sourceField);
            config.put("formats", Collections.singletonList("dd/MM/yyyyy"));
            config.put("locale", locale);
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                    () -> factory.create(null, null, config));
            assertThat(e.getMessage(), equalTo("Unknown language: " + locale.split("[_-]")[0]));
        }

        locales = new String[] { "en-XY", "en-Canada" };
        for (String locale : locales) {
            DateProcessor.Factory factory = new DateProcessor.Factory();
            Map<String, Object> config = new HashMap<>();
            String sourceField = randomAlphaOfLengthBetween(1, 10);
            config.put("field", sourceField);
            config.put("formats", Collections.singletonList("dd/MM/yyyyy"));
            config.put("locale", locale);
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                    () -> factory.create(null, null, config));
            assertThat(e.getMessage(), equalTo("Unknown country: " + locale.split("[_-]")[1]));
        }
    }

    public void testParseTimezone() throws Exception {
        DateProcessor.Factory factory = new DateProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        String sourceField = randomAlphaOfLengthBetween(1, 10);
        config.put("field", sourceField);
        config.put("formats", Collections.singletonList("dd/MM/yyyyy"));

        DateTimeZone timezone = randomDateTimeZone();
        config.put("timezone", timezone.getID());
        DateProcessor processor = factory.create(null, null, config);
        assertThat(processor.getTimezone(), equalTo(timezone));
    }

    public void testParseInvalidTimezone() throws Exception {
        DateProcessor.Factory factory = new DateProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        String sourceField = randomAlphaOfLengthBetween(1, 10);
        config.put("field", sourceField);
        config.put("match_formats", Collections.singletonList("dd/MM/yyyyy"));
        config.put("timezone", "invalid_timezone");
        try {
            factory.create(null, null, config);
            fail("invalid timezone should fail");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("The datetime zone id 'invalid_timezone' is not recognised"));
        }
    }

    public void testParseMatchFormats() throws Exception {
        DateProcessor.Factory factory = new DateProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        String sourceField = randomAlphaOfLengthBetween(1, 10);
        config.put("field", sourceField);
        config.put("formats", Arrays.asList("dd/MM/yyyy", "dd-MM-yyyy"));

        DateProcessor processor = factory.create(null, null, config);
        assertThat(processor.getFormats(), equalTo(Arrays.asList("dd/MM/yyyy", "dd-MM-yyyy")));
    }

    public void testParseMatchFormatsFailure() throws Exception {
        DateProcessor.Factory factory = new DateProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        String sourceField = randomAlphaOfLengthBetween(1, 10);
        config.put("field", sourceField);
        config.put("formats", "dd/MM/yyyy");

        try {
            factory.create(null, null, config);
            fail("processor creation should have failed");
        } catch(ElasticsearchParseException e) {
            assertThat(e.getMessage(), containsString("[formats] property isn't a list, but of type [java.lang.String]"));
        }
    }

    public void testParseTargetField() throws Exception {
        DateProcessor.Factory factory = new DateProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        String sourceField = randomAlphaOfLengthBetween(1, 10);
        String targetField = randomAlphaOfLengthBetween(1, 10);
        config.put("field", sourceField);
        config.put("target_field", targetField);
        config.put("formats", Arrays.asList("dd/MM/yyyy", "dd-MM-yyyy"));

        DateProcessor processor = factory.create(null, null, config);
        assertThat(processor.getTargetField(), equalTo(targetField));
    }
}
