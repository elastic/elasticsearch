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
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class JoinProcessorFactoryTests extends ESTestCase {

    public void testCreate() throws Exception {
        JoinProcessor.Factory factory = new JoinProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("separator", "-");
        String processorTag = randomAlphaOfLength(10);
        JoinProcessor joinProcessor = factory.create(null, processorTag, null, config, null);
        assertThat(joinProcessor.getTag(), equalTo(processorTag));
        assertThat(joinProcessor.getField(), equalTo("field1"));
        assertThat(joinProcessor.getSeparator(), equalTo("-"));
        assertThat(joinProcessor.getTargetField(), equalTo("field1"));
    }

    public void testCreateNoFieldPresent() throws Exception {
        JoinProcessor.Factory factory = new JoinProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("separator", "-");
        try {
            factory.create(null, null, null, config, null);
            fail("factory create should have failed");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), equalTo("[field] required property is missing"));
        }
    }

    public void testCreateNoSeparatorPresent() throws Exception {
        JoinProcessor.Factory factory = new JoinProcessor.Factory();
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
        JoinProcessor.Factory factory = new JoinProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("separator", "-");
        config.put("target_field", "target");
        String processorTag = randomAlphaOfLength(10);
        JoinProcessor joinProcessor = factory.create(null, processorTag, null, config, null);
        assertThat(joinProcessor.getTag(), equalTo(processorTag));
        assertThat(joinProcessor.getField(), equalTo("field1"));
        assertThat(joinProcessor.getSeparator(), equalTo("-"));
        assertThat(joinProcessor.getTargetField(), equalTo("target"));
    }
}
