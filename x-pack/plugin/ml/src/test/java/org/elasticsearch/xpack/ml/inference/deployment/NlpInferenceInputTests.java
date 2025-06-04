/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.deployment;

import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelInput;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class NlpInferenceInputTests extends ESTestCase {

    public void testExtractInput_GivenValidField() {
        String fieldName = randomAlphaOfLength(7);
        Map<String, Object> doc = new HashMap<>();
        doc.put(fieldName, "value");
        doc.put("some other field", "some_other_value");

        String input = NlpInferenceInput.fromDoc(doc).extractInput(new TrainedModelInput(Collections.singletonList(fieldName)));

        assertThat(input, equalTo("value"));
    }

    public void testExtractInput_GivenFieldIsNotPresent() {
        String fieldName = randomAlphaOfLength(7);
        Map<String, Object> doc = new HashMap<>();
        doc.put("some other field", 42);

        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> NlpInferenceInput.fromDoc(doc).extractInput(new TrainedModelInput(Collections.singletonList(fieldName)))
        );

        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(), equalTo("Input field [" + fieldName + "] does not exist in the source document"));
    }

    public void testExtractInput_GivenFieldIsNotString() {
        String fieldName = randomAlphaOfLength(7);
        Map<String, Object> doc = new HashMap<>();
        doc.put(fieldName, 42);
        doc.put("some other field", 42);

        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> NlpInferenceInput.fromDoc(doc).extractInput(new TrainedModelInput(Collections.singletonList(fieldName)))
        );

        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(), equalTo("Input value [42] for field [" + fieldName + "] must be a string"));
    }

    public void testExtractInput_GivenSimpleText() {
        var input = NlpInferenceInput.fromText("foobar");
        assertTrue(input.isTextInput());
        assertEquals("foobar", input.extractInput(new TrainedModelInput(List.of())));
    }
}
