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

public class SortProcessorFactoryTests extends ESTestCase {

    public void testCreate() throws Exception {
        String processorTag = randomAlphaOfLength(10);
        String fieldName = RandomDocumentPicks.randomFieldName(random());

        Map<String, Object> config = new HashMap<>();
        config.put("field", fieldName);

        SortProcessor.Factory factory = new SortProcessor.Factory();
        SortProcessor processor = factory.create(null, processorTag, null, config);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), equalTo(fieldName));
        assertThat(processor.getOrder(), equalTo(SortProcessor.SortOrder.ASCENDING));
        assertThat(processor.getTargetField(), equalTo(fieldName));
    }

    public void testCreateWithOrder() throws Exception {
        String processorTag = randomAlphaOfLength(10);
        String fieldName = RandomDocumentPicks.randomFieldName(random());

        Map<String, Object> config = new HashMap<>();
        config.put("field", fieldName);
        config.put("order", "desc");

        SortProcessor.Factory factory = new SortProcessor.Factory();
        SortProcessor processor = factory.create(null, processorTag, null, config);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), equalTo(fieldName));
        assertThat(processor.getOrder(), equalTo(SortProcessor.SortOrder.DESCENDING));
        assertThat(processor.getTargetField(), equalTo(fieldName));
    }

    public void testCreateWithTargetField() throws Exception {
        String processorTag = randomAlphaOfLength(10);
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        String targetFieldName = RandomDocumentPicks.randomFieldName(random());

        Map<String, Object> config = new HashMap<>();
        config.put("field", fieldName);
        config.put("target_field", targetFieldName);

        SortProcessor.Factory factory = new SortProcessor.Factory();
        SortProcessor processor = factory.create(null, processorTag, null, config);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), equalTo(fieldName));
        assertThat(processor.getOrder(), equalTo(SortProcessor.SortOrder.ASCENDING));
        assertThat(processor.getTargetField(), equalTo(targetFieldName));
    }

    public void testCreateWithInvalidOrder() throws Exception {
        String processorTag = randomAlphaOfLength(10);
        String fieldName = RandomDocumentPicks.randomFieldName(random());

        Map<String, Object> config = new HashMap<>();
        config.put("field", fieldName);
        config.put("order", "invalid");

        SortProcessor.Factory factory = new SortProcessor.Factory();
        try {
            factory.create(null, processorTag, null, config);
            fail("factory create should have failed");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), equalTo("[order] Sort direction [invalid] not recognized. Valid values are: [asc, desc]"));
        }
    }

    public void testCreateMissingField() throws Exception {
        SortProcessor.Factory factory = new SortProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        try {
            factory.create(null, null, null, config);
            fail("factory create should have failed");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), equalTo("[field] required property is missing"));
        }
    }
}
