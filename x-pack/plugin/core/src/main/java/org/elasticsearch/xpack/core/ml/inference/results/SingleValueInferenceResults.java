/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public abstract class SingleValueInferenceResults implements InferenceResults {

    public final ParseField VALUE = new ParseField("value");

    private final double value;

    SingleValueInferenceResults(StreamInput in) throws IOException {
        value = in.readDouble();
    }

    SingleValueInferenceResults(double value) {
        this.value = value;
    }

    public Double value() {
        return value;
    }

    public String valueAsString() {
        return String.valueOf(value);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(value);
    }

    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(VALUE.getPreferredName(), value);
        innerToXContent(builder, params);
        builder.endObject();
        return builder;
    }

    abstract XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException;
}
