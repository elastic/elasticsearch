/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.config;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * A class that describes a condition.
 * The {@linkplain Operator} enum defines the available
 * comparisons a condition can use.
 */
public class Condition implements ToXContentObject, Writeable {
    public static final ParseField CONDITION_FIELD = new ParseField("condition");
    public static final ParseField FILTER_VALUE_FIELD = new ParseField("value");

    public static final ConstructingObjectParser<Condition, Void> PARSER = new ConstructingObjectParser<>(
            CONDITION_FIELD.getPreferredName(), a -> new Condition((Operator) a[0], (String) a[1]));

    static {
        PARSER.declareField(ConstructingObjectParser.constructorArg(), p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return Operator.fromString(p.text());
            }
            throw new IllegalArgumentException("Unsupported token [" + p.currentToken() + "]");
        }, Operator.OPERATOR_FIELD, ValueType.STRING);
        PARSER.declareField(ConstructingObjectParser.constructorArg(), p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return p.text();
            }
            if (p.currentToken() == XContentParser.Token.VALUE_NULL) {
                return null;
            }
            throw new IllegalArgumentException("Unsupported token [" + p.currentToken() + "]");
        }, FILTER_VALUE_FIELD, ValueType.STRING_OR_NULL);
    }

    private final Operator op;
    private final String filterValue;

    public Condition(StreamInput in) throws IOException {
        op = Operator.readFromStream(in);
        filterValue = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        op.writeTo(out);
        out.writeOptionalString(filterValue);
    }

    public Condition(Operator op, String filterValue) {
        if (filterValue == null) {
            throw ExceptionsHelper.badRequestException(Messages.getMessage(Messages.JOB_CONFIG_CONDITION_INVALID_VALUE_NULL));
        }

        if (op.expectsANumericArgument()) {
            try {
                Double.parseDouble(filterValue);
            } catch (NumberFormatException nfe) {
                String msg = Messages.getMessage(Messages.JOB_CONFIG_CONDITION_INVALID_VALUE_NUMBER, filterValue);
                throw ExceptionsHelper.badRequestException(msg);
            }
        } else {
            try {
                Pattern.compile(filterValue);
            } catch (PatternSyntaxException e) {
                String msg = Messages.getMessage(Messages.JOB_CONFIG_CONDITION_INVALID_VALUE_REGEX, filterValue);
                throw ExceptionsHelper.badRequestException(msg);
            }
        }
        this.op = op;
        this.filterValue = filterValue;
    }

    public Operator getOperator() {
        return op;
    }

    public String getValue() {
        return filterValue;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Operator.OPERATOR_FIELD.getPreferredName(), op);
        builder.field(FILTER_VALUE_FIELD.getPreferredName(), filterValue);
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(op, filterValue);
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
                Objects.equals(this.filterValue, other.filterValue);
    }
}
