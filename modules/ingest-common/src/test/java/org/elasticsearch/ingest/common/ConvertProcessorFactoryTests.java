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
        ConvertProcessor convertProcessor = factory.create(null, processorTag, null, config, null);
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
            factory.create(null, null, null, config, null);
            fail("factory create should have failed");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), equalTo("[type] type [" + type + "] not supported, cannot convert field."));
            assertThat(e.getMetadata("es.processor_type").get(0), equalTo(ConvertProcessor.TYPE));
            assertThat(e.getMetadata("es.property_name").get(0), equalTo("type"));
            assertThat(e.getMetadata("es.processor_tag"), nullValue());
        }
    }

    public void testCreateNoFieldPresent() throws Exception {
        ConvertProcessor.Factory factory = new ConvertProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        String type = "type-" + randomAlphaOfLengthBetween(1, 10);
        config.put("type", type);
        try {
            factory.create(null, null, null, config, null);
            fail("factory create should have failed");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), equalTo("[field] required property is missing"));
        }
    }

    public void testCreateNoTypePresent() throws Exception {
        ConvertProcessor.Factory factory = new ConvertProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        try {
            factory.create(null, null, null, config, null);
            fail("factory create should have failed");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), equalTo("[type] required property is missing"));
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
        ConvertProcessor convertProcessor = factory.create(null, processorTag, null, config, null);
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
        ConvertProcessor convertProcessor = factory.create(null, processorTag, null, config, null);
        assertThat(convertProcessor.getTag(), equalTo(processorTag));
        assertThat(convertProcessor.getField(), equalTo("field1"));
        assertThat(convertProcessor.getTargetField(), equalTo("field1"));
        assertThat(convertProcessor.getConvertType(), equalTo(type));
        assertThat(convertProcessor.isIgnoreMissing(), is(true));
    }
}
