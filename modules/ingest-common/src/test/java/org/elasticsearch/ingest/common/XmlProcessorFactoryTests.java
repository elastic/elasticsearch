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

public class XmlProcessorFactoryTests extends ESTestCase {

    public void testCreate() throws Exception {
        XmlProcessor.Factory factory = new XmlProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("target_field", "target");
        config.put("ignore_missing", true);
        config.put("ignore_failure", true);
        config.put("to_lower", true);
        config.put("ignore_empty_value", true);

        String processorTag = randomAlphaOfLength(10);
        XmlProcessor processor = factory.create(null, processorTag, null, config, null);

        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), equalTo("field1"));
        assertThat(processor.getTargetField(), equalTo("target"));
        assertThat(processor.isIgnoreMissing(), equalTo(true));
        assertThat(processor.isIgnoreEmptyValue(), equalTo(true));
    }

    public void testCreateWithDefaults() throws Exception {
        XmlProcessor.Factory factory = new XmlProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");

        String processorTag = randomAlphaOfLength(10);
        XmlProcessor processor = factory.create(null, processorTag, null, config, null);

        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), equalTo("field1"));
        assertThat(processor.getTargetField(), equalTo("field1"));
        assertThat(processor.isIgnoreMissing(), equalTo(false));
        assertThat(processor.isIgnoreEmptyValue(), equalTo(false));
    }

    public void testCreateMissingField() throws Exception {
        XmlProcessor.Factory factory = new XmlProcessor.Factory();
        Map<String, Object> config = new HashMap<>();

        String processorTag = randomAlphaOfLength(10);
        ElasticsearchParseException exception = expectThrows(
            ElasticsearchParseException.class,
            () -> factory.create(null, processorTag, null, config, null)
        );
        assertThat(exception.getMessage(), equalTo("[field] required property is missing"));
    }

    public void testCreateWithIgnoreEmptyValueOnly() throws Exception {
        XmlProcessor.Factory factory = new XmlProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("ignore_empty_value", true);

        String processorTag = randomAlphaOfLength(10);
        XmlProcessor processor = factory.create(null, processorTag, null, config, null);

        assertThat(processor.getField(), equalTo("field1"));
        assertThat(processor.isIgnoreEmptyValue(), equalTo(true));
        assertThat(processor.isIgnoreMissing(), equalTo(false)); // other flags should remain default
    }
}
