/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.fleet.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class PostSecretRequest extends ActionRequest {

    public static final ParseField VALUE_FIELD = new ParseField("value");

    public static final ConstructingObjectParser<PostSecretRequest, Void> PARSER = new ConstructingObjectParser<>(
        "post_secret_request",
        args -> {
            return new PostSecretRequest(args[0]);
        }
    );

    static {
        PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> {
            if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return p.text();
            } else if (p.currentToken() == XContentParser.Token.START_ARRAY) {
                return p.list().stream().map(s -> (String) s).toArray(String[]::new);
            } else {
                throw new IllegalArgumentException("Unexpected token: " + p.currentToken());
            }
        }, VALUE_FIELD, ObjectParser.ValueType.STRING_ARRAY);
    }

    public static PostSecretRequest fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private final Object value;

    public PostSecretRequest(Object value) {
        if ((value instanceof String == false) && (value instanceof String[] == false)) {
            throw new IllegalArgumentException("value must be a string or an array of strings");
        }
        this.value = value;
    }

    public PostSecretRequest(StreamInput in) throws IOException {
        super(in);
        if (in.readByte() == 0) {
            this.value = in.readString();
        } else {
            this.value = in.readStringArray();
        }
    }

    public Object value() {
        return value;
    }

    public XContentBuilder toXContent(XContentBuilder builder) throws IOException {
        builder.startObject();
        if (value instanceof String) {
            builder.field(VALUE_FIELD.getPreferredName(), (String) value);
        } else if (value instanceof String[]) {
            builder.field(VALUE_FIELD.getPreferredName(), (String[]) value);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (value instanceof String) {
            out.writeBoolean(true);
            out.writeString((String) value);
        } else if (value instanceof String[]) {
            out.writeBoolean(false);
            out.writeStringArray((String[]) value);
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        if (this.value == null) {
            ActionRequestValidationException exception = new ActionRequestValidationException();
            exception.addValidationError("value is missing");
            return exception;
        }

        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PostSecretRequest that = (PostSecretRequest) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }
}
