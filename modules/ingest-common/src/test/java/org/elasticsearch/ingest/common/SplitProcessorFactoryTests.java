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
import org.elasticsearch.ingest.TestIngestDocument;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;

public class SplitProcessorFactoryTests extends ESTestCase {

    public void testCreate() throws Exception {
        SplitProcessor.Factory factory = new SplitProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("separator", "\\.");
        String processorTag = randomAlphaOfLength(10);
        SplitProcessor splitProcessor = factory.create(null, processorTag, null, config, null);
        assertThat(splitProcessor.getTag(), equalTo(processorTag));
        assertThat(splitProcessor.getField(), hasToString("field1"));
        assertThat(splitProcessor.getSeparator(), equalTo("\\."));
        assertFalse(splitProcessor.isIgnoreMissing());
        assertThat(splitProcessor.getTargetField(), hasToString("field1"));
    }

    public void testCreateNoFieldPresent() throws Exception {
        SplitProcessor.Factory factory = new SplitProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("separator", "\\.");
        try {
            factory.create(null, null, null, config, null);
            fail("factory create should have failed");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), equalTo("[field] required property is missing"));
        }
    }

    public void testCreateNoSeparatorPresent() throws Exception {
        SplitProcessor.Factory factory = new SplitProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        try {
            factory.create(null, null, null, config, null);
            fail("factory create should have failed");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), equalTo("[separator] required property is missing"));
        }
    }

    public void testCreateWithTargetField() throws Exception {
        SplitProcessor.Factory factory = new SplitProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("separator", "\\.");
        config.put("target_field", "target");
        String processorTag = randomAlphaOfLength(10);
        SplitProcessor splitProcessor = factory.create(null, processorTag, null, config, null);
        assertThat(splitProcessor.getTag(), equalTo(processorTag));
        assertThat(splitProcessor.getField(), hasToString("field1"));
        assertThat(splitProcessor.getSeparator(), equalTo("\\."));
        assertFalse(splitProcessor.isIgnoreMissing());
        assertFalse(splitProcessor.isPreserveTrailing());
        assertThat(splitProcessor.getTargetField(), hasToString("target"));
    }

    public void testCreateWithPreserveTrailing() throws Exception {
        SplitProcessor.Factory factory = new SplitProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("separator", "\\.");
        config.put("target_field", "target");
        config.put("preserve_trailing", true);
        String processorTag = randomAlphaOfLength(10);
        SplitProcessor splitProcessor = factory.create(null, processorTag, null, config, null);
        assertThat(splitProcessor.getTag(), equalTo(processorTag));
        assertThat(splitProcessor.getField(), hasToString("field1"));
        assertThat(splitProcessor.getSeparator(), equalTo("\\."));
        assertFalse(splitProcessor.isIgnoreMissing());
        assertThat(splitProcessor.getTargetField(), hasToString("target"));
    }

    /**
     * Pipelines are stored, so a malformed path in one has to be accepted when the processor is created, with the failure only coming
     * when a document is run through it. This goes through the factory, as creating a processor from a stored pipeline does.
     */
    public void testCreateWithMalformedField() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "a..b");
        config.put("separator", "\\.");
        SplitProcessor processor = new SplitProcessor.Factory().create(null, randomAlphaOfLength(10), null, config, null);
        assertThat(processor.getField(), hasToString("a..b"));

        IngestDocument ingestDocument = TestIngestDocument.emptyIngestDocument();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertThat(e.getMessage(), equalTo("path [a..b] is not valid"));
    }

    public void testCreateWithMalformedTargetField() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "source");
        config.put("target_field", "a..b");
        config.put("separator", "\\.");
        SplitProcessor processor = new SplitProcessor.Factory().create(null, randomAlphaOfLength(10), null, config, null);
        assertThat(processor.getTargetField(), hasToString("a..b"));

        // the source is fine, so it is the target that causes the failure
        IngestDocument ingestDocument = TestIngestDocument.emptyIngestDocument();
        ingestDocument.setFieldValue("source", "a.b");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertThat(e.getMessage(), equalTo("path [a..b] is not valid"));
    }
}
