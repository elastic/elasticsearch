/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public class CustomResults implements InferenceResults {
    public static final String NAME = "custom_results";
    public static final String CUSTOM_TYPE = TaskType.CUSTOM.toString();

    Map<String, Object> data;

    public CustomResults(Map<String, Object> data) {
        this.data = data;
    }

    public CustomResults(StreamInput in) throws IOException {
        this.data = in.readGenericMap();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(CUSTOM_TYPE, this.asMap());
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeGenericMap(data);
    }

    @Override
    public String getResultsField() {
        return CUSTOM_TYPE;
    }

    @Override
    public Map<String, Object> asMap() {
        return data;
    }

    @Override
    public Map<String, Object> asMap(String outputField) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(outputField, this.asMap());
        return map;
    }

    @Override
    public Map<String, Object> predictedValue() {
        return this.asMap();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CustomResults that = (CustomResults) o;
        return data.equals(that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(data);
    }
}
