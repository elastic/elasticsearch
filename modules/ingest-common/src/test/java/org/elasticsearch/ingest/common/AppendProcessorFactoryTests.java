/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.TestTemplateService;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class AppendProcessorFactoryTests extends ESTestCase {

    private AppendProcessor.Factory factory;

    @Before
    public void init() {
        factory = new AppendProcessor.Factory(TestTemplateService.instance());
    }

    public void testCreate() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        Object value;
        if (randomBoolean()) {
            value = "value1";
        } else {
            value = List.of("value1", "value2", "value3");
        }
        config.put("value", value);
        String processorTag = randomAlphaOfLength(10);
        AppendProcessor appendProcessor = factory.create(null, processorTag, null, config);
        assertThat(appendProcessor.getTag(), equalTo(processorTag));
        assertThat(appendProcessor.getField().newInstance(Map.of()).execute(), equalTo("field1"));
        assertThat(appendProcessor.getValue().copyAndResolve(Map.of()), equalTo(value));
    }

    public void testCreateNoFieldPresent() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("value", "value1");
        try {
            factory.create(null, null, null, config);
            fail("factory create should have failed");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), equalTo("[field] required property is missing"));
        }
    }

    public void testCreateNoValuePresent() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        try {
            factory.create(null, null, null, config);
            fail("factory create should have failed");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), equalTo("[value] required property is missing"));
        }
    }

    public void testCreateNullValue() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("value", null);
        try {
            factory.create(null, null, null, config);
            fail("factory create should have failed");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), equalTo("[value] required property is missing"));
        }
    }

    public void testInvalidMustacheTemplate() throws Exception {
        factory = new AppendProcessor.Factory(TestTemplateService.instance(true));
        Map<String, Object> config = new HashMap<>();
        config.put("field", "{{field1}}");
        config.put("value", "value1");
        String processorTag = randomAlphaOfLength(10);
        ElasticsearchException exception = expectThrows(
            ElasticsearchException.class,
            () -> factory.create(null, processorTag, null, config)
        );
        assertThat(exception.getMessage(), equalTo("java.lang.RuntimeException: could not compile script"));
        assertThat(exception.getMetadata("es.processor_tag").get(0), equalTo(processorTag));
    }

    public void testMediaType() throws Exception {
        // valid media type
        String expectedMediaType = randomFrom(ConfigurationUtils.VALID_MEDIA_TYPES);
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("value", "value1");
        config.put("media_type", expectedMediaType);
        String processorTag = randomAlphaOfLength(10);
        AppendProcessor appendProcessor = factory.create(null, processorTag, null, config);
        assertThat(appendProcessor.getTag(), equalTo(processorTag));

        // invalid media type
        expectedMediaType = randomValueOtherThanMany(
            m -> List.of(ConfigurationUtils.VALID_MEDIA_TYPES).contains(m),
            () -> randomAlphaOfLengthBetween(5, 9)
        );
        final Map<String, Object> config2 = new HashMap<>();
        config2.put("field", "field1");
        config2.put("value", "value1");
        config2.put("media_type", expectedMediaType);
        ElasticsearchException e = expectThrows(ElasticsearchException.class, () -> factory.create(null, processorTag, null, config2));
        assertThat(e.getMessage(), containsString("property does not contain a supported media type [" + expectedMediaType + "]"));
    }
}
