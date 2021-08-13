/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class PerClassSingleValue implements ToXContentObject, Writeable {

    private static final ParseField CLASS_NAME = new ParseField("class_name");
    private static final ParseField VALUE = new ParseField("value");

    public static final ConstructingObjectParser<PerClassSingleValue, Void> PARSER =
        new ConstructingObjectParser<>("per_class_result", true, a -> new PerClassSingleValue((String) a[0], (double) a[1]));

    static {
        PARSER.declareString(constructorArg(), CLASS_NAME);
        PARSER.declareDouble(constructorArg(), VALUE);
    }

    private final String className;
    private final double value;

    public PerClassSingleValue(String className, double value) {
        this.className = ExceptionsHelper.requireNonNull(className, CLASS_NAME);
        this.value = value;
    }

    public PerClassSingleValue(StreamInput in) throws IOException {
        this.className = in.readString();
        this.value = in.readDouble();
    }

    public String getClassName() {
        return className;
    }

    public double getValue() {
        return value;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(className);
        out.writeDouble(value);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CLASS_NAME.getPreferredName(), className);
        builder.field(VALUE.getPreferredName(), value);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PerClassSingleValue that = (PerClassSingleValue) o;
        return Objects.equals(this.className, that.className)
            && this.value == that.value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(className, value);
    }
}
