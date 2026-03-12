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
import org.elasticsearch.dissect.DissectException;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class DissectProcessorFactoryTests extends ESTestCase {

    public void testCreate() {
        DissectProcessor.Factory factory = new DissectProcessor.Factory();
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        String processorTag = randomAlphaOfLength(10);
        String pattern = "%{a},%{b},%{c}";
        String appendSeparator = ":";

        Map<String, Object> config = new HashMap<>();
        config.put("field", fieldName);
        config.put("pattern", pattern);
        config.put("append_separator", appendSeparator);
        config.put("ignore_missing", true);

        DissectProcessor processor = factory.create(null, processorTag, null, config, null);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.field, equalTo(fieldName));
        assertThat(processor.pattern, equalTo(pattern));
        assertThat(processor.appendSeparator, equalTo(appendSeparator));
        assertThat(processor.dissectParser, is(notNullValue()));
        assertThat(processor.ignoreMissing, is(true));
    }

    public void testCreateMissingField() {
        DissectProcessor.Factory factory = new DissectProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("pattern", "%{a},%{b},%{c}");
        Exception e = expectThrows(ElasticsearchParseException.class, () -> factory.create(null, "_tag", null, config, null));
        assertThat(e.getMessage(), equalTo("[field] required property is missing"));
    }

    public void testCreateMissingPattern() {
        DissectProcessor.Factory factory = new DissectProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("field", randomAlphaOfLength(10));
        Exception e = expectThrows(ElasticsearchParseException.class, () -> factory.create(null, "_tag", null, config, null));
        assertThat(e.getMessage(), equalTo("[pattern] required property is missing"));
    }

    public void testCreateMissingOptionals() {
        DissectProcessor.Factory factory = new DissectProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("pattern", "%{a},%{b},%{c}");
        config.put("field", randomAlphaOfLength(10));
        DissectProcessor processor = factory.create(null, "_tag", null, config, null);
        assertThat(processor.appendSeparator, equalTo(""));
        assertThat(processor.ignoreMissing, is(false));
    }

    public void testCreateBadPattern() {
        DissectProcessor.Factory factory = new DissectProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("pattern", "no keys defined");
        config.put("field", randomAlphaOfLength(10));
        expectThrows(DissectException.class, () -> factory.create(null, "_tag", null, config, null));
    }
}
