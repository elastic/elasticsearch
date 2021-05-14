/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.is;

public abstract class AbstractStringProcessorFactoryTestCase extends ESTestCase {

    protected abstract AbstractStringProcessor.Factory newFactory();

    protected Map<String, Object> modifyConfig(Map<String, Object> config) {
        return config;
    }

    protected void assertProcessor(AbstractStringProcessor<?> processor) {}

    public void testCreate() throws Exception {
        AbstractStringProcessor.Factory factory = newFactory();
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        String processorTag = randomAlphaOfLength(10);

        Map<String, Object> config = new HashMap<>();
        config.put("field", fieldName);

        AbstractStringProcessor<?> processor = factory.create(null, processorTag, null, modifyConfig(config));
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), equalTo(fieldName));
        assertThat(processor.isIgnoreMissing(), is(false));
        assertThat(processor.getTargetField(), equalTo(fieldName));
        assertProcessor(processor);
    }

    public void testCreateWithIgnoreMissing() throws Exception {
        AbstractStringProcessor.Factory factory = newFactory();
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        String processorTag = randomAlphaOfLength(10);

        Map<String, Object> config = new HashMap<>();
        config.put("field", fieldName);
        config.put("ignore_missing", true);

        AbstractStringProcessor<?> processor = factory.create(null, processorTag, null, modifyConfig(config));
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), equalTo(fieldName));
        assertThat(processor.isIgnoreMissing(), is(true));
        assertThat(processor.getTargetField(), equalTo(fieldName));
        assertProcessor(processor);
    }

    public void testCreateWithTargetField() throws Exception {
        AbstractStringProcessor.Factory factory = newFactory();
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        String targetFieldName = RandomDocumentPicks.randomFieldName(random());
        String processorTag = randomAlphaOfLength(10);

        Map<String, Object> config = new HashMap<>();
        config.put("field", fieldName);
        config.put("target_field", targetFieldName);

        AbstractStringProcessor<?> processor = factory.create(null, processorTag, null, modifyConfig(config));
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), equalTo(fieldName));
        assertThat(processor.isIgnoreMissing(), is(false));
        assertThat(processor.getTargetField(), equalTo(targetFieldName));
        assertProcessor(processor);
    }

    public void testCreateMissingField() throws Exception {
        AbstractStringProcessor.Factory factory = newFactory();
        Map<String, Object> config = new HashMap<>();
        try {
            factory.create(null, null, null, config);
            fail("factory create should have failed");
        } catch(ElasticsearchParseException e) {
            assertThat(e.getMessage(), equalTo("[field] required property is missing"));
        }
    }
}
