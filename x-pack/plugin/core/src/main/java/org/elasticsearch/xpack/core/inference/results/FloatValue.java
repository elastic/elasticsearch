/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class FloatValue implements EmbeddingValue {

    public static final String NAME = "float_value";

    private final Float value;

    public FloatValue(Float value) {
        this.value = value;
    }

    public FloatValue(StreamInput in) throws IOException {
        value = in.readFloat();
    }

    @Override
    public String toString() {
        return value.toString();
    }

    @Override
    public Number getValue() {
        return value;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.value(value);
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FloatValue that = (FloatValue) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeFloat(value);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
