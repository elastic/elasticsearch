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
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;

public class SortProcessorFactoryTests extends ESTestCase {

    public void testCreate() throws Exception {
        String processorTag = randomAlphaOfLength(10);
        String fieldName = RandomDocumentPicks.randomFieldName(random());

        Map<String, Object> config = new HashMap<>();
        config.put("field", fieldName);

        SortProcessor.Factory factory = new SortProcessor.Factory();
        SortProcessor processor = factory.create(null, processorTag, null, config, null);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), hasToString(fieldName));
        assertThat(processor.getOrder(), equalTo(SortProcessor.SortOrder.ASCENDING));
        assertThat(processor.getTargetField(), hasToString(fieldName));
    }

    public void testCreateWithOrder() throws Exception {
        String processorTag = randomAlphaOfLength(10);
        String fieldName = RandomDocumentPicks.randomFieldName(random());

        Map<String, Object> config = new HashMap<>();
        config.put("field", fieldName);
        config.put("order", "desc");

        SortProcessor.Factory factory = new SortProcessor.Factory();
        SortProcessor processor = factory.create(null, processorTag, null, config, null);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), hasToString(fieldName));
        assertThat(processor.getOrder(), equalTo(SortProcessor.SortOrder.DESCENDING));
        assertThat(processor.getTargetField(), hasToString(fieldName));
    }

    public void testCreateWithTargetField() throws Exception {
        String processorTag = randomAlphaOfLength(10);
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        String targetFieldName = RandomDocumentPicks.randomFieldName(random());

        Map<String, Object> config = new HashMap<>();
        config.put("field", fieldName);
        config.put("target_field", targetFieldName);

        SortProcessor.Factory factory = new SortProcessor.Factory();
        SortProcessor processor = factory.create(null, processorTag, null, config, null);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), hasToString(fieldName));
        assertThat(processor.getOrder(), equalTo(SortProcessor.SortOrder.ASCENDING));
        assertThat(processor.getTargetField(), hasToString(targetFieldName));
    }

    public void testCreateWithInvalidOrder() throws Exception {
        String processorTag = randomAlphaOfLength(10);
        String fieldName = RandomDocumentPicks.randomFieldName(random());

        Map<String, Object> config = new HashMap<>();
        config.put("field", fieldName);
        config.put("order", "invalid");

        SortProcessor.Factory factory = new SortProcessor.Factory();
        try {
            factory.create(null, processorTag, null, config, null);
            fail("factory create should have failed");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), equalTo("[order] Sort direction [invalid] not recognized. Valid values are: [asc, desc]"));
        }
    }

    public void testCreateMissingField() throws Exception {
        SortProcessor.Factory factory = new SortProcessor.Factory();
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
        Map<String, Object> config = new HashMap<>();
        config.put("field", "a..b");
        SortProcessor processor = new SortProcessor.Factory().create(null, randomAlphaOfLength(10), null, config, null);
        assertThat(processor.getField(), hasToString("a..b"));

        IngestDocument ingestDocument = TestIngestDocument.emptyIngestDocument();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertThat(e.getMessage(), equalTo("path [a..b] is not valid"));
    }

    public void testCreateWithMalformedTargetField() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "source");
        config.put("target_field", "a..b");
        SortProcessor processor = new SortProcessor.Factory().create(null, randomAlphaOfLength(10), null, config, null);
        assertThat(processor.getTargetField(), hasToString("a..b"));

        // the source is fine, so it is the target that causes the failure
        IngestDocument ingestDocument = TestIngestDocument.emptyIngestDocument();
        ingestDocument.setFieldValue("source", List.of(2, 1));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertThat(e.getMessage(), equalTo("path [a..b] is not valid"));
    }
}
