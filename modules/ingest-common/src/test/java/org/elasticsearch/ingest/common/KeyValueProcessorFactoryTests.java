/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class KeyValueProcessorFactoryTests extends ESTestCase {

    public void testCreateWithDefaults() throws Exception {
        KeyValueProcessor.Factory factory = new KeyValueProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("field_split", "&");
        config.put("value_split", "=");
        String processorTag = randomAlphaOfLength(10);
        KeyValueProcessor processor = factory.create(null, processorTag, null, config);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), equalTo("field1"));
        assertThat(processor.getFieldSplit(), equalTo("&"));
        assertThat(processor.getValueSplit(), equalTo("="));
        assertThat(processor.getIncludeKeys(), is(nullValue()));
        assertThat(processor.getTargetField(), is(nullValue()));
        assertFalse(processor.isIgnoreMissing());
    }

    public void testCreateWithAllFieldsSet() throws Exception {
        KeyValueProcessor.Factory factory = new KeyValueProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("field_split", "&");
        config.put("value_split", "=");
        config.put("target_field", "target");
        config.put("include_keys", Arrays.asList("a", "b"));
        config.put("exclude_keys", Collections.emptyList());
        config.put("ignore_missing", true);
        String processorTag = randomAlphaOfLength(10);
        KeyValueProcessor processor = factory.create(null, processorTag, null, config);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), equalTo("field1"));
        assertThat(processor.getFieldSplit(), equalTo("&"));
        assertThat(processor.getValueSplit(), equalTo("="));
        assertThat(processor.getIncludeKeys(), equalTo(Sets.newHashSet("a", "b")));
        assertThat(processor.getExcludeKeys(), equalTo(Collections.emptySet()));
        assertThat(processor.getTargetField(), equalTo("target"));
        assertTrue(processor.isIgnoreMissing());
    }

    public void testCreateWithMissingField() {
        KeyValueProcessor.Factory factory = new KeyValueProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        String processorTag = randomAlphaOfLength(10);
        ElasticsearchException exception = expectThrows(ElasticsearchParseException.class,
            () -> factory.create(null, processorTag, null, config));
        assertThat(exception.getMessage(), equalTo("[field] required property is missing"));
    }

    public void testCreateWithMissingFieldSplit() {
        KeyValueProcessor.Factory factory = new KeyValueProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        String processorTag = randomAlphaOfLength(10);
        ElasticsearchException exception = expectThrows(ElasticsearchParseException.class,
            () -> factory.create(null, processorTag, null, config));
        assertThat(exception.getMessage(), equalTo("[field_split] required property is missing"));
    }

    public void testCreateWithMissingValueSplit() {
        KeyValueProcessor.Factory factory = new KeyValueProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("field_split", "&");
        String processorTag = randomAlphaOfLength(10);
        ElasticsearchException exception = expectThrows(ElasticsearchParseException.class,
            () -> factory.create(null, processorTag, null, config));
        assertThat(exception.getMessage(), equalTo("[value_split] required property is missing"));
    }
}
