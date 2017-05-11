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

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class ConvertProcessorFactoryTests extends ESTestCase {

    public void testCreate() throws Exception {
        ConvertProcessor.Factory factory = new ConvertProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        ConvertProcessor.Type type = randomFrom(ConvertProcessor.Type.values());
        config.put("field", "field1");
        config.put("type", type.toString());
        String processorTag = randomAlphaOfLength(10);
        ConvertProcessor convertProcessor = factory.create(null, processorTag, config);
        assertThat(convertProcessor.getTag(), equalTo(processorTag));
        assertThat(convertProcessor.getField(), equalTo("field1"));
        assertThat(convertProcessor.getTargetField(), equalTo("field1"));
        assertThat(convertProcessor.getConvertType(), equalTo(type));
        assertThat(convertProcessor.isIgnoreMissing(), is(false));
    }

    public void testCreateUnsupportedType() throws Exception {
        ConvertProcessor.Factory factory = new ConvertProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        String type = "type-" + randomAlphaOfLengthBetween(1, 10);
        config.put("field", "field1");
        config.put("type", type);
        try {
            factory.create(null, null, config);
            fail("factory create should have failed");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), Matchers.equalTo("[type] type [" + type + "] not supported, cannot convert field."));
            assertThat(e.getHeader("processor_type").get(0), equalTo(ConvertProcessor.TYPE));
            assertThat(e.getHeader("property_name").get(0), equalTo("type"));
            assertThat(e.getHeader("processor_tag"), nullValue());
        }
    }

    public void testCreateNoFieldPresent() throws Exception {
        ConvertProcessor.Factory factory = new ConvertProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        String type = "type-" + randomAlphaOfLengthBetween(1, 10);
        config.put("type", type);
        try {
            factory.create(null, null, config);
            fail("factory create should have failed");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), Matchers.equalTo("[field] required property is missing"));
        }
    }

    public void testCreateNoTypePresent() throws Exception {
        ConvertProcessor.Factory factory = new ConvertProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        try {
            factory.create(null, null, config);
            fail("factory create should have failed");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), Matchers.equalTo("[type] required property is missing"));
        }
    }

    public void testCreateWithExplicitTargetField() throws Exception {
        ConvertProcessor.Factory factory = new ConvertProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        ConvertProcessor.Type type = randomFrom(ConvertProcessor.Type.values());
        config.put("field", "field1");
        config.put("target_field", "field2");
        config.put("type", type.toString());
        String processorTag = randomAlphaOfLength(10);
        ConvertProcessor convertProcessor = factory.create(null, processorTag, config);
        assertThat(convertProcessor.getTag(), equalTo(processorTag));
        assertThat(convertProcessor.getField(), equalTo("field1"));
        assertThat(convertProcessor.getTargetField(), equalTo("field2"));
        assertThat(convertProcessor.getConvertType(), equalTo(type));
        assertThat(convertProcessor.isIgnoreMissing(), is(false));
    }

    public void testCreateWithIgnoreMissing() throws Exception {
        ConvertProcessor.Factory factory = new ConvertProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        ConvertProcessor.Type type = randomFrom(ConvertProcessor.Type.values());
        config.put("field", "field1");
        config.put("type", type.toString());
        config.put("ignore_missing", true);
        String processorTag = randomAlphaOfLength(10);
        ConvertProcessor convertProcessor = factory.create(null, processorTag, config);
        assertThat(convertProcessor.getTag(), equalTo(processorTag));
        assertThat(convertProcessor.getField(), equalTo("field1"));
        assertThat(convertProcessor.getTargetField(), equalTo("field1"));
        assertThat(convertProcessor.getConvertType(), equalTo(type));
        assertThat(convertProcessor.isIgnoreMissing(), is(true));
    }
}
