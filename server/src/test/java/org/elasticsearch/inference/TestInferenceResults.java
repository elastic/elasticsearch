/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.inference;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

public class TestInferenceResults implements InferenceResults {

    private final String resultField;
    private final Map<String, Object> inferenceResults;

    public TestInferenceResults(String resultField, Map<String, Object> inferenceResults) {
        this.resultField = resultField;
        this.inferenceResults = inferenceResults;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        throw new UnsupportedEncodingException();
    }

    @Override
    public String getWriteableName() {
        return "test_inference_results";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getResultsField() {
        return resultField;
    }

    @Override
    public Map<String, Object> asMap() {
        Map<String, Object> result = new HashMap<>();
        result.put(resultField, inferenceResults);
        return result;
    }

    @Override
    public Map<String, Object> asMap(String outputField) {
        Map<String, Object> result = new HashMap<>();
        result.put(outputField, inferenceResults);
        return result;
    }

    @Override
    public Object predictedValue() {
        throw new UnsupportedOperationException();
    }
}
