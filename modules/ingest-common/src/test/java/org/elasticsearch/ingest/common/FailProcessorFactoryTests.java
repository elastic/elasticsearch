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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;

public class FailProcessorFactoryTests extends ESTestCase {

    private FailProcessor.Factory factory;

    @Before
    public void init() {
        factory = new FailProcessor.Factory(TestTemplateService.instance());
    }

    public void testCreate() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("message", "error");
        String processorTag = randomAlphaOfLength(10);
        FailProcessor failProcessor = factory.create(null, processorTag, null, config);
        assertThat(failProcessor.getTag(), equalTo(processorTag));
        assertThat(failProcessor.getMessage().newInstance(Collections.emptyMap()).execute(), equalTo("error"));
    }

    public void testCreateMissingMessageField() throws Exception {
        Map<String, Object> config = new HashMap<>();
        try {
            factory.create(null, null, null, config);
            fail("factory create should have failed");
        } catch(ElasticsearchParseException e) {
            assertThat(e.getMessage(), equalTo("[message] required property is missing"));
        }
    }

    public void testInvalidMustacheTemplate() throws Exception {
        FailProcessor.Factory factory = new FailProcessor.Factory(TestTemplateService.instance(true));
        Map<String, Object> config = new HashMap<>();
        config.put("message", "{{error}}");
        String processorTag = randomAlphaOfLength(10);
        ElasticsearchException exception = expectThrows(ElasticsearchException.class, () -> factory.create(null, processorTag,
            null, config));
        assertThat(exception.getMessage(), equalTo("java.lang.RuntimeException: could not compile script"));
        assertThat(exception.getMetadata("es.processor_tag").get(0), equalTo(processorTag));
    }
}
