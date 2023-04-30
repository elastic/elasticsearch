/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class SplitProcessorFactoryTests extends ESTestCase {

    public void testCreate() throws Exception {
        SplitProcessor.Factory factory = new SplitProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("separator", "\\.");
        String processorTag = randomAlphaOfLength(10);
        SplitProcessor splitProcessor = factory.create(null, processorTag, null, config);
        assertThat(splitProcessor.getTag(), equalTo(processorTag));
        assertThat(splitProcessor.getField(), equalTo("field1"));
        assertThat(splitProcessor.getSeparator(), equalTo("\\."));
        assertFalse(splitProcessor.isIgnoreMissing());
        assertThat(splitProcessor.getTargetField(), equalTo("field1"));
    }

    public void testCreateNoFieldPresent() throws Exception {
        SplitProcessor.Factory factory = new SplitProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("separator", "\\.");
        try {
            factory.create(null, null, null, config);
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
            factory.create(null, null, null, config);
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
        SplitProcessor splitProcessor = factory.create(null, processorTag, null, config);
        assertThat(splitProcessor.getTag(), equalTo(processorTag));
        assertThat(splitProcessor.getField(), equalTo("field1"));
        assertThat(splitProcessor.getSeparator(), equalTo("\\."));
        assertFalse(splitProcessor.isIgnoreMissing());
        assertFalse(splitProcessor.isPreserveTrailing());
        assertThat(splitProcessor.getTargetField(), equalTo("target"));
    }

    public void testCreateWithPreserveTrailing() throws Exception {
        SplitProcessor.Factory factory = new SplitProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("separator", "\\.");
        config.put("target_field", "target");
        config.put("preserve_trailing", true);
        String processorTag = randomAlphaOfLength(10);
        SplitProcessor splitProcessor = factory.create(null, processorTag, null, config);
        assertThat(splitProcessor.getTag(), equalTo(processorTag));
        assertThat(splitProcessor.getField(), equalTo("field1"));
        assertThat(splitProcessor.getSeparator(), equalTo("\\."));
        assertFalse(splitProcessor.isIgnoreMissing());
        assertThat(splitProcessor.getTargetField(), equalTo("target"));
    }

}
