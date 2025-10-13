/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.secrets.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class PostConnectorSecretRequest extends LegacyActionRequest {

    private static final ParseField VALUE_FIELD = new ParseField("value");

    public static final ConstructingObjectParser<PostConnectorSecretRequest, Void> PARSER = new ConstructingObjectParser<>(
        "post_secret_request",
        args -> new PostConnectorSecretRequest((String) args[0])
    );

    static {
        PARSER.declareField(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> p.text(),
            VALUE_FIELD,
            ObjectParser.ValueType.STRING
        );
    }

    public static PostConnectorSecretRequest fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private final String value;

    public PostConnectorSecretRequest(String value) {
        this.value = value;
    }

    public PostConnectorSecretRequest(StreamInput in) throws IOException {
        super(in);
        this.value = in.readString();
    }

    public String value() {
        return value;
    }

    public XContentBuilder toXContent(XContentBuilder builder) throws IOException {
        builder.startObject();
        builder.field(VALUE_FIELD.getPreferredName(), this.value);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(value);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;

        if (Strings.isNullOrEmpty(this.value)) {
            validationException = addValidationError("[value] of the connector secret cannot be [null] or [\"\"]", validationException);
        }

        return validationException;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PostConnectorSecretRequest that = (PostConnectorSecretRequest) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }
}
