/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.exception.ElasticsearchParseException;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class GsubProcessorFactoryTests extends AbstractStringProcessorFactoryTestCase {

    @Override
    protected AbstractStringProcessor.Factory newFactory() {
        return new GsubProcessor.Factory();
    }

    @Override
    protected Map<String, Object> modifyConfig(Map<String, Object> config) {
        config.put("pattern", "\\.");
        config.put("replacement", "-");
        return config;
    }

    @Override
    protected void assertProcessor(AbstractStringProcessor<?> processor) {
        GsubProcessor gsubProcessor = (GsubProcessor) processor;
        assertThat(gsubProcessor.getPattern().toString(), equalTo("\\."));
        assertThat(gsubProcessor.getReplacement(), equalTo("-"));
    }

    public void testCreateNoPatternPresent() throws Exception {
        GsubProcessor.Factory factory = new GsubProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("replacement", "-");
        try {
            factory.create(null, null, null, config, null);
            fail("factory create should have failed");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), equalTo("[pattern] required property is missing"));
        }
    }

    public void testCreateNoReplacementPresent() throws Exception {
        GsubProcessor.Factory factory = new GsubProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("pattern", "\\.");
        try {
            factory.create(null, null, null, config, null);
            fail("factory create should have failed");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), equalTo("[replacement] required property is missing"));
        }
    }

    public void testCreateInvalidPattern() throws Exception {
        GsubProcessor.Factory factory = new GsubProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("pattern", "[");
        config.put("replacement", "-");
        try {
            factory.create(null, null, null, config, null);
            fail("factory create should have failed");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), containsString("[pattern] Invalid regex pattern. Unclosed character class"));
        }
    }
}
