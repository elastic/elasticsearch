/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.ingest.TestIngestDocument;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
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

        AbstractStringProcessor<?> processor = factory.create(null, processorTag, null, modifyConfig(config), null);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), hasToString(fieldName));
        assertThat(processor.isIgnoreMissing(), is(false));
        assertThat(processor.getTargetField(), hasToString(fieldName));
        assertProcessor(processor);
    }

    public void testCreateWithIgnoreMissing() throws Exception {
        AbstractStringProcessor.Factory factory = newFactory();
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        String processorTag = randomAlphaOfLength(10);

        Map<String, Object> config = new HashMap<>();
        config.put("field", fieldName);
        config.put("ignore_missing", true);

        AbstractStringProcessor<?> processor = factory.create(null, processorTag, null, modifyConfig(config), null);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), hasToString(fieldName));
        assertThat(processor.isIgnoreMissing(), is(true));
        assertThat(processor.getTargetField(), hasToString(fieldName));
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

        AbstractStringProcessor<?> processor = factory.create(null, processorTag, null, modifyConfig(config), null);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), hasToString(fieldName));
        assertThat(processor.isIgnoreMissing(), is(false));
        assertThat(processor.getTargetField(), hasToString(targetFieldName));
        assertProcessor(processor);
    }

    public void testCreateMissingField() throws Exception {
        AbstractStringProcessor.Factory factory = newFactory();
        Map<String, Object> config = new HashMap<>();
        try {
            factory.create(null, null, null, config, null);
            fail("factory create should have failed");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), equalTo("[field] required property is missing"));
        }
    }

    /**
     * Pipelines are stored, so a malformed path in one has to be accepted when the processor is created, with the failure only coming
     * when a document is run through it. This goes through the factory, as creating a processor from a stored pipeline does.
     */
    public void testCreateWithMalformedField() throws Exception {
        AbstractStringProcessor.Factory factory = newFactory();
        Map<String, Object> config = new HashMap<>();
        config.put("field", "a..b");

        AbstractStringProcessor<?> processor = factory.create(null, randomAlphaOfLength(10), null, modifyConfig(config), null);
        assertThat(processor.getField(), hasToString("a..b"));

        IngestDocument ingestDocument = TestIngestDocument.emptyIngestDocument();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertThat(e.getMessage(), equalTo("path [a..b] is not valid"));
    }

    public void testCreateWithMalformedTargetField() throws Exception {
        AbstractStringProcessor.Factory factory = newFactory();
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("target_field", "a..b");

        AbstractStringProcessor<?> processor = factory.create(null, randomAlphaOfLength(10), null, modifyConfig(config), null);
        assertThat(processor.getField(), hasToString("field1"));
        assertThat(processor.getTargetField(), hasToString("a..b"));
    }
}
