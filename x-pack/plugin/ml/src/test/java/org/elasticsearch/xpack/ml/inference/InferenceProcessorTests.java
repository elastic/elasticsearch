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
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InferenceProcessorTests extends ESTestCase {

    public void testFactory() throws Exception {
        Model mockModel = mock(Model.class);
        ModelLoader mockLoader = mock(ModelLoader.class);
        when(mockLoader.load(eq("k2"), eq("inference"), eq(false), any())).thenReturn(mockModel);

        InferenceProcessor.Factory factory = new InferenceProcessor.Factory(Map.of("test", mockLoader));

        Map<String, Object> config = new HashMap<>();
        config.put("model_id", "k2");
        config.put("model_type", "test");
        String processorTag = randomAlphaOfLength(10);

        InferenceProcessor processor = factory.create(Collections.emptyMap(), processorTag, config);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getType(), equalTo("inference"));
    }

    public void testFactory_givenMissingModel() {
        InferenceProcessor.Factory factory = new InferenceProcessor.Factory(Collections.emptyMap());

        Map<String, Object> config = new HashMap<>();
        config.put("target_field", "lion");
        String processorTag = randomAlphaOfLength(10);

        ElasticsearchParseException parseException = expectThrows(ElasticsearchParseException.class,
                () -> factory.create(Collections.emptyMap(), processorTag, config));
        assertThat(parseException.getMessage(), equalTo("[model_id] required property is missing"));
    }
}
