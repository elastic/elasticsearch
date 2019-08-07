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
        DateProcessor processor = factory.create(null, processorTag, config);
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
            factory.create(null, null, config);
            fail("processor creation should have failed");
        } catch(ElasticsearchParseException e) {
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
            factory.create(null, null, config);
            fail("processor creation should have failed");
        } catch(ElasticsearchParseException e) {
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

        DateProcessor processor = factory.create(null, null, config);
        assertThat(processor.getLocale().newInstance(Collections.emptyMap()).execute(), equalTo(locale.toLanguageTag()));
    }

    public void testParseTimezone() throws Exception {
        Map<String, Object> config = new HashMap<>();
        String sourceField = randomAlphaOfLengthBetween(1, 10);
        config.put("field", sourceField);
        config.put("formats", Collections.singletonList("dd/MM/yyyyy"));

        ZoneId timezone = randomZone();
        config.put("timezone", timezone.getId());
        DateProcessor processor = factory.create(null, null, config);
        assertThat(processor.getTimezone().newInstance(Collections.emptyMap()).execute(), equalTo(timezone.getId()));
    }

    public void testParseMatchFormats() throws Exception {
        Map<String, Object> config = new HashMap<>();
        String sourceField = randomAlphaOfLengthBetween(1, 10);
        config.put("field", sourceField);
        config.put("formats", Arrays.asList("dd/MM/yyyy", "dd-MM-yyyy"));

        DateProcessor processor = factory.create(null, null, config);
        assertThat(processor.getFormats(), equalTo(Arrays.asList("dd/MM/yyyy", "dd-MM-yyyy")));
    }

    public void testParseMatchFormatsFailure() throws Exception {
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
