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
import org.elasticsearch.ingest.TestTemplateService;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class RenameProcessorFactoryTests extends ESTestCase {

    private RenameProcessor.Factory factory;

    @Before
    public void init() {
        factory = new RenameProcessor.Factory(TestTemplateService.instance());
    }

    public void testCreate() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "old_field");
        config.put("target_field", "new_field");
        String processorTag = randomAlphaOfLength(10);
        RenameProcessor renameProcessor = factory.create(null, processorTag, null, config, null);
        assertThat(renameProcessor.getTag(), equalTo(processorTag));
        assertThat(renameProcessor.getField().newInstance(Map.of()).execute(), equalTo("old_field"));
        assertThat(renameProcessor.getTargetField().newInstance(Map.of()).execute(), equalTo("new_field"));
        assertThat(renameProcessor.isIgnoreMissing(), equalTo(false));
        assertThat(renameProcessor.isOverrideEnabled(), equalTo(false));
    }

    public void testCreateWithIgnoreMissing() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "old_field");
        config.put("target_field", "new_field");
        config.put("ignore_missing", true);
        String processorTag = randomAlphaOfLength(10);
        RenameProcessor renameProcessor = factory.create(null, processorTag, null, config, null);
        assertThat(renameProcessor.getTag(), equalTo(processorTag));
        assertThat(renameProcessor.getField().newInstance(Map.of()).execute(), equalTo("old_field"));
        assertThat(renameProcessor.getTargetField().newInstance(Map.of()).execute(), equalTo("new_field"));
        assertThat(renameProcessor.isIgnoreMissing(), equalTo(true));
    }

    public void testCreateWithEnableOverride() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "old_field");
        config.put("target_field", "new_field");
        config.put("override", true);
        String processorTag = randomAlphaOfLength(10);
        RenameProcessor renameProcessor = factory.create(null, processorTag, null, config, null);
        assertThat(renameProcessor.getTag(), equalTo(processorTag));
        assertThat(renameProcessor.getField().newInstance(Map.of()).execute(), equalTo("old_field"));
        assertThat(renameProcessor.getTargetField().newInstance(Map.of()).execute(), equalTo("new_field"));
        assertThat(renameProcessor.isOverrideEnabled(), equalTo(true));
    }

    public void testCreateNoFieldPresent() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("target_field", "new_field");
        try {
            factory.create(null, null, null, config, null);
            fail("factory create should have failed");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), equalTo("[field] required property is missing"));
        }
    }

    public void testCreateNoToPresent() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "old_field");
        try {
            factory.create(null, null, null, config, null);
            fail("factory create should have failed");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), equalTo("[target_field] required property is missing"));
        }
    }
}
