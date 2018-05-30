/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.job.config;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

/**
 * A class that describes a condition.
 * The {@linkplain Operator} enum defines the available
 * comparisons a condition can use.
 */
public class Condition implements ToXContentObject, Writeable {
    public static final ParseField CONDITION_FIELD = new ParseField("condition");
    public static final ParseField VALUE_FIELD = new ParseField("value");

    public static final ConstructingObjectParser<Condition, Void> PARSER = new ConstructingObjectParser<>(
            CONDITION_FIELD.getPreferredName(), a -> new Condition((Operator) a[0], (double) a[1]));

    static {
        PARSER.declareField(ConstructingObjectParser.constructorArg(), p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return Operator.fromString(p.text());
            }
            throw new IllegalArgumentException("Unsupported token [" + p.currentToken() + "]");
        }, Operator.OPERATOR_FIELD, ValueType.STRING);
        PARSER.declareDouble(ConstructingObjectParser.constructorArg(), VALUE_FIELD);
    }

    private final Operator op;
    private final double value;

    public Condition(StreamInput in) throws IOException {
        op = Operator.readFromStream(in);
        value = in.readDouble();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        op.writeTo(out);
        out.writeDouble(value);
    }

    public Condition(Operator op, double value) {
        this.op = ExceptionsHelper.requireNonNull(op, Operator.OPERATOR_FIELD.getPreferredName());
        this.value = value;
    }

    public Operator getOperator() {
        return op;
    }

    public double getValue() {
        return value;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Operator.OPERATOR_FIELD.getPreferredName(), op);
        builder.field(VALUE_FIELD.getPreferredName(), value);
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(op, value);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }

        if (getClass() != obj.getClass()) {
            return false;
        }

        Condition other = (Condition) obj;
        return Objects.equals(this.op, other.op) &&
                Objects.equals(this.value, other.value);
    }
}
