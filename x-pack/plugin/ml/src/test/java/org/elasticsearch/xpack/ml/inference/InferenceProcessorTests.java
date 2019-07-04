/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.inference;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class InferenceProcessorTests extends ESTestCase {

    public void testFactory() {
        InferenceProcessor.Factory factory = new InferenceProcessor.Factory();

        Map<String, Object> config = new HashMap<>();
        config.put("model", "k2");
        config.put("target_field", "lion");
        String processorTag = randomAlphaOfLength(10);

        InferenceProcessor processor = factory.create(Collections.emptyMap(), processorTag, config);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getType(), equalTo("inference"));
        assertThat(processor.getModelId(), equalTo("k2"));
        assertThat(processor.getTargetField(), equalTo("lion"));
    }

    public void testFactory_givenMissingModel() {
        InferenceProcessor.Factory factory = new InferenceProcessor.Factory();

        Map<String, Object> config = new HashMap<>();
        config.put("target_field", "lion");
        String processorTag = randomAlphaOfLength(10);

        ElasticsearchParseException parseException = expectThrows(ElasticsearchParseException.class,
                () -> factory.create(Collections.emptyMap(), processorTag, config));
        assertThat(parseException.getMessage(), equalTo("[model] required property is missing"));
    }
}
