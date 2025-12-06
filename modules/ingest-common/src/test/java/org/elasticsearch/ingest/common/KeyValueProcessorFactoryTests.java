/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.ingest.TestTemplateService;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class KeyValueProcessorFactoryTests extends ESTestCase {

    private KeyValueProcessor.Factory factory;

    @Before
    public void init() {
        factory = new KeyValueProcessor.Factory(TestTemplateService.instance());
    }

    public void testCreateWithDefaults() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("field_split", "&");
        config.put("value_split", "=");
        String processorTag = randomAlphaOfLength(10);
        KeyValueProcessor processor = factory.create(null, processorTag, null, config, null);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField().newInstance(Map.of()).execute(), equalTo("field1"));
        assertThat(processor.getFieldSplit(), equalTo("&"));
        assertThat(processor.getValueSplit(), equalTo("="));
        assertThat(processor.getIncludeKeys(), is(nullValue()));
        assertThat(processor.getTargetField(), is(nullValue()));
        assertFalse(processor.isIgnoreMissing());
    }

    public void testCreateWithAllFieldsSet() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("field_split", "&");
        config.put("value_split", "=");
        config.put("target_field", "target");
        config.put("include_keys", List.of("a", "b"));
        config.put("exclude_keys", List.of());
        config.put("ignore_missing", true);
        String processorTag = randomAlphaOfLength(10);
        KeyValueProcessor processor = factory.create(null, processorTag, null, config, null);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField().newInstance(Map.of()).execute(), equalTo("field1"));
        assertThat(processor.getFieldSplit(), equalTo("&"));
        assertThat(processor.getValueSplit(), equalTo("="));
        assertThat(processor.getIncludeKeys(), equalTo(Sets.newHashSet("a", "b")));
        assertThat(processor.getExcludeKeys(), equalTo(Set.of()));
        assertThat(processor.getTargetField().newInstance(Map.of()).execute(), equalTo("target"));
        assertTrue(processor.isIgnoreMissing());
    }

    public void testCreateWithMissingField() {
        Map<String, Object> config = new HashMap<>();
        String processorTag = randomAlphaOfLength(10);
        ElasticsearchException exception = expectThrows(
            ElasticsearchParseException.class,
            () -> factory.create(null, processorTag, null, config, null)
        );
        assertThat(exception.getMessage(), equalTo("[field] required property is missing"));
    }

    public void testCreateWithMissingFieldSplit() {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        String processorTag = randomAlphaOfLength(10);
        ElasticsearchException exception = expectThrows(
            ElasticsearchParseException.class,
            () -> factory.create(null, processorTag, null, config, null)
        );
        assertThat(exception.getMessage(), equalTo("[field_split] required property is missing"));
    }

    public void testCreateWithMissingValueSplit() {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("field_split", "&");
        String processorTag = randomAlphaOfLength(10);
        ElasticsearchException exception = expectThrows(
            ElasticsearchParseException.class,
            () -> factory.create(null, processorTag, null, config, null)
        );
        assertThat(exception.getMessage(), equalTo("[value_split] required property is missing"));
    }
}
