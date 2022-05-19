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
import org.elasticsearch.ingest.TestTemplateService;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.equalTo;

public class RemoveProcessorFactoryTests extends ESTestCase {

    private RemoveProcessor.Factory factory;

    @Before
    public void init() {
        factory = new RemoveProcessor.Factory(TestTemplateService.instance());
    }

    public void testCreate() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        String processorTag = randomAlphaOfLength(10);
        RemoveProcessor removeProcessor = factory.create(null, processorTag, null, config);
        assertThat(removeProcessor.getTag(), equalTo(processorTag));
        assertThat(removeProcessor.getFieldsToRemove().get(0).newInstance(Collections.emptyMap()).execute(), equalTo("field1"));
    }

    public void testCreateKeepField() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("keep", Arrays.asList("field1", "field2"));
        String processorTag = randomAlphaOfLength(10);
        RemoveProcessor removeProcessor = factory.create(null, processorTag, null, config);
        assertThat(removeProcessor.getTag(), equalTo(processorTag));
        assertThat(removeProcessor.getFieldsToKeep().get(0).newInstance(Collections.emptyMap()).execute(), equalTo("field1"));
        assertThat(removeProcessor.getFieldsToKeep().get(1).newInstance(Collections.emptyMap()).execute(), equalTo("field2"));
    }

    public void testCreateMultipleFields() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("field", Arrays.asList("field1", "field2"));
        String processorTag = randomAlphaOfLength(10);
        RemoveProcessor removeProcessor = factory.create(null, processorTag, null, config);
        assertThat(removeProcessor.getTag(), equalTo(processorTag));
        assertThat(
            removeProcessor.getFieldsToRemove()
                .stream()
                .map(template -> template.newInstance(Collections.emptyMap()).execute())
                .collect(Collectors.toList()),
            equalTo(Arrays.asList("field1", "field2"))
        );
    }

    public void testCreateMissingField() throws Exception {
        Map<String, Object> config = new HashMap<>();
        try {
            factory.create(null, null, null, config);
            fail("factory create should have failed");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), equalTo("[keep] or [field] must be specified"));
        }
    }

    public void testCreateTooManyFields() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("keep", "field2");
        try {
            factory.create(null, null, null, config);
            fail("factory create should have failed");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), equalTo("[keep] and [field] cannot both be used in the same processor"));
        }
    }

    public void testInvalidMustacheTemplate() throws Exception {
        factory = new RemoveProcessor.Factory(TestTemplateService.instance(true));
        Map<String, Object> config = new HashMap<>();
        config.put("field", "{{field1}}");
        String processorTag = randomAlphaOfLength(10);
        ElasticsearchException exception = expectThrows(
            ElasticsearchException.class,
            () -> factory.create(null, processorTag, null, config)
        );
        assertThat(exception.getMessage(), equalTo("java.lang.RuntimeException: could not compile script"));
        assertThat(exception.getMetadata("es.processor_tag").get(0), equalTo(processorTag));
    }
}
