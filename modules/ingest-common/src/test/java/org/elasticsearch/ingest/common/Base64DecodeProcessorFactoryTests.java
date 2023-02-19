/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class Base64DecodeProcessorFactoryTests extends ESTestCase {
    public void testFactoryWithDefaultValues() throws Exception {
        Base64DecodeProcessor.Factory factory = new Base64DecodeProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("field", "input.field");
        config.put("target_field", "output.field");
        String processorTag = randomAlphaOfLength(10);
        Base64DecodeProcessor base64DecodeProcessor = factory.create(null, processorTag, null, config);
        assertThat(base64DecodeProcessor.getTag(), equalTo(processorTag));
        assertThat(base64DecodeProcessor.getField(), equalTo("input.field"));
        assertThat(base64DecodeProcessor.getTargetField(), equalTo("output.field"));
        assertThat(base64DecodeProcessor.isIgnoreMissing(), is(false));
        assertThat(base64DecodeProcessor.isSkipFailure(), is(false));
    }
    public void testFactoryWithAssignedValues() throws Exception {
        Base64DecodeProcessor.Factory factory = new Base64DecodeProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("field", "input.field");
        config.put("target_field", "output.field");
        config.put("is_padded", false);
        config.put("ignore_failure", true);
        config.put("ignore_missing", true);
        String processorTag = randomAlphaOfLength(10);
        Base64DecodeProcessor base64DecodeProcessor = factory.create(null, processorTag, null, config);
        assertThat(base64DecodeProcessor.getTag(), equalTo(processorTag));
        assertThat(base64DecodeProcessor.getField(), equalTo("input.field"));
        assertThat(base64DecodeProcessor.getTargetField(), equalTo("output.field"));
        assertThat(base64DecodeProcessor.isIgnoreMissing(), is(true));
        assertThat(base64DecodeProcessor.isSkipFailure(), is(true));
    }
}
