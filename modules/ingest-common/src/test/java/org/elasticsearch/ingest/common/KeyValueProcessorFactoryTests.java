/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
        KeyValueProcessor processor = factory.create(null, processorTag, config);
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
        KeyValueProcessor processor = factory.create(null, processorTag, config);
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
            () -> factory.create(null, processorTag, config));
        assertThat(exception.getMessage(), equalTo("[field] required property is missing"));
    }

    public void testCreateWithMissingFieldSplit() {
        KeyValueProcessor.Factory factory = new KeyValueProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        String processorTag = randomAlphaOfLength(10);
        ElasticsearchException exception = expectThrows(ElasticsearchParseException.class,
            () -> factory.create(null, processorTag, config));
        assertThat(exception.getMessage(), equalTo("[field_split] required property is missing"));
    }

    public void testCreateWithMissingValueSplit() {
        KeyValueProcessor.Factory factory = new KeyValueProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("field_split", "&");
        String processorTag = randomAlphaOfLength(10);
        ElasticsearchException exception = expectThrows(ElasticsearchParseException.class,
            () -> factory.create(null, processorTag, config));
        assertThat(exception.getMessage(), equalTo("[value_split] required property is missing"));
    }
}
