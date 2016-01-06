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

package org.elasticsearch.ingest.processor;

import org.elasticsearch.test.ESTestCase;
import org.joda.time.DateTimeZone;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class DateProcessorFactoryTests extends ESTestCase {

    public void testBuildDefaults() throws Exception {
        DateProcessor.Factory factory = new DateProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        String sourceField = randomAsciiOfLengthBetween(1, 10);
        config.put("match_field", sourceField);
        config.put("match_formats", Collections.singletonList("dd/MM/yyyyy"));

        DateProcessor processor = factory.create(config);
        assertThat(processor.getMatchField(), equalTo(sourceField));
        assertThat(processor.getTargetField(), equalTo(DateProcessor.DEFAULT_TARGET_FIELD));
        assertThat(processor.getMatchFormats(), equalTo(Collections.singletonList("dd/MM/yyyyy")));
        assertThat(processor.getLocale(), equalTo(Locale.ENGLISH));
        assertThat(processor.getTimezone(), equalTo(DateTimeZone.UTC));
    }

    public void testMatchFieldIsMandatory() throws Exception {
        DateProcessor.Factory factory = new DateProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        String targetField = randomAsciiOfLengthBetween(1, 10);
        config.put("target_field", targetField);
        config.put("match_formats", Collections.singletonList("dd/MM/yyyyy"));

        try {
            factory.create(config);
            fail("processor creation should have failed");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("required property [match_field] is missing"));
        }
    }

    public void testMatchFormatsIsMandatory() throws Exception {
        DateProcessor.Factory factory = new DateProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        String sourceField = randomAsciiOfLengthBetween(1, 10);
        String targetField = randomAsciiOfLengthBetween(1, 10);
        config.put("match_field", sourceField);
        config.put("target_field", targetField);

        try {
            factory.create(config);
            fail("processor creation should have failed");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("required property [match_formats] is missing"));
        }
    }

    public void testParseLocale() throws Exception {
        DateProcessor.Factory factory = new DateProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        String sourceField = randomAsciiOfLengthBetween(1, 10);
        config.put("match_field", sourceField);
        config.put("match_formats", Collections.singletonList("dd/MM/yyyyy"));
        Locale locale = randomLocale(random());
        config.put("locale", locale.toLanguageTag());

        DateProcessor processor = factory.create(config);
        assertThat(processor.getLocale().toLanguageTag(), equalTo(locale.toLanguageTag()));
    }

    public void testParseInvalidLocale() throws Exception {
        DateProcessor.Factory factory = new DateProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        String sourceField = randomAsciiOfLengthBetween(1, 10);
        config.put("match_field", sourceField);
        config.put("match_formats", Collections.singletonList("dd/MM/yyyyy"));
        config.put("locale", "invalid_locale");
        try {
            factory.create(config);
            fail("should fail with invalid locale");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("Invalid language tag specified: invalid_locale"));
        }
    }

    public void testParseTimezone() throws Exception {
        DateProcessor.Factory factory = new DateProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        String sourceField = randomAsciiOfLengthBetween(1, 10);
        config.put("match_field", sourceField);
        config.put("match_formats", Collections.singletonList("dd/MM/yyyyy"));

        DateTimeZone timezone = randomTimezone();
        config.put("timezone", timezone.getID());
        DateProcessor processor = factory.create(config);
        assertThat(processor.getTimezone(), equalTo(timezone));
    }

    public void testParseInvalidTimezone() throws Exception {
        DateProcessor.Factory factory = new DateProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        String sourceField = randomAsciiOfLengthBetween(1, 10);
        config.put("match_field", sourceField);
        config.put("match_formats", Collections.singletonList("dd/MM/yyyyy"));
        config.put("timezone", "invalid_timezone");
        try {
            factory.create(config);
            fail("invalid timezone should fail");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("The datetime zone id 'invalid_timezone' is not recognised"));
        }
    }

    //we generate a timezone out of the available ones in joda, some available in the jdk are not available in joda by default
    private static DateTimeZone randomTimezone() {
        List<String> ids = new ArrayList<>(DateTimeZone.getAvailableIDs());
        Collections.sort(ids);
        return DateTimeZone.forID(randomFrom(ids));
    }


    public void testParseMatchFormats() throws Exception {
        DateProcessor.Factory factory = new DateProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        String sourceField = randomAsciiOfLengthBetween(1, 10);
        config.put("match_field", sourceField);
        config.put("match_formats", Arrays.asList("dd/MM/yyyy", "dd-MM-yyyy"));

        DateProcessor processor = factory.create(config);
        assertThat(processor.getMatchFormats(), equalTo(Arrays.asList("dd/MM/yyyy", "dd-MM-yyyy")));
    }

    public void testParseMatchFormatsFailure() throws Exception {
        DateProcessor.Factory factory = new DateProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        String sourceField = randomAsciiOfLengthBetween(1, 10);
        config.put("match_field", sourceField);
        config.put("match_formats", "dd/MM/yyyy");

        try {
            factory.create(config);
            fail("processor creation should have failed");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("property [match_formats] isn't a list, but of type [java.lang.String]"));
        }
    }

    public void testParseTargetField() throws Exception {
        DateProcessor.Factory factory = new DateProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        String sourceField = randomAsciiOfLengthBetween(1, 10);
        String targetField = randomAsciiOfLengthBetween(1, 10);
        config.put("match_field", sourceField);
        config.put("target_field", targetField);
        config.put("match_formats", Arrays.asList("dd/MM/yyyy", "dd-MM-yyyy"));

        DateProcessor processor = factory.create(config);
        assertThat(processor.getTargetField(), equalTo(targetField));
    }
}
