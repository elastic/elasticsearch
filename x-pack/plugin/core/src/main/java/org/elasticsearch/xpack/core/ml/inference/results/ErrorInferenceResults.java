/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public class ErrorInferenceResults implements InferenceResults {

    public static final String NAME = "error";

    private final Exception exception;

    public ErrorInferenceResults(Exception exception) {
        this.exception = Objects.requireNonNull(exception);
    }

    public ErrorInferenceResults(StreamInput in) throws IOException {
        this.exception = in.readException();
    }

    public Exception getException() {
        return exception;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeException(exception);
    }

    @Override
    public boolean equals(Object object) {
        if (object == this) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        ErrorInferenceResults that = (ErrorInferenceResults) object;
        // Just compare the message for serialization test purposes
        return Objects.equals(exception.getMessage(), that.exception.getMessage());
    }

    @Override
    public int hashCode() {
        // Just compare the message for serialization test purposes
        return Objects.hash(exception.getMessage());
    }

    @Override
    public String getResultsField() {
        return NAME;
    }

    @Override
    public Map<String, Object> asMap() {
        Map<String, Object> asMap = new LinkedHashMap<>();
        asMap.put(NAME, exception.getMessage());
        return asMap;
    }

    @Override
    public Map<String, Object> asMap(String outputField) {
        // errors do not have a result
        return asMap();
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public Object predictedValue() {
        return null;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(NAME, exception.getMessage());
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
