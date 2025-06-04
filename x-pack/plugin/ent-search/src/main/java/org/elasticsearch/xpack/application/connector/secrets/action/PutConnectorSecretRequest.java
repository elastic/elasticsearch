/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.secrets.action;

import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class PutConnectorSecretRequest extends LegacyActionRequest implements ToXContentObject {

    private final String id;
    private final String value;

    public PutConnectorSecretRequest(String id, String value) {
        this.id = id;
        this.value = value;
    }

    public PutConnectorSecretRequest(StreamInput in) throws IOException {
        super(in);
        this.id = in.readString();
        this.value = in.readString();
    }

    public static final ConstructingObjectParser<PutConnectorSecretRequest, String> PARSER = new ConstructingObjectParser<>(
        "connector_secret_put_request",
        false,
        ((args, id) -> new PutConnectorSecretRequest(id, (String) args[0]))
    );

    static {
        PARSER.declareString(constructorArg(), new ParseField("value"));
    }

    public static PutConnectorSecretRequest fromXContentBytes(String id, BytesReference source, XContentType xContentType) {
        try (XContentParser parser = XContentHelper.createParser(XContentParserConfiguration.EMPTY, source, xContentType)) {
            return PutConnectorSecretRequest.fromXContent(parser, id);
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse: " + source.utf8ToString(), e);
        }
    }

    public static PutConnectorSecretRequest fromXContent(XContentParser parser, String id) throws IOException {
        return PARSER.parse(parser, id);
    }

    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("value", value);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(id);
        out.writeString(value);
    }

    @Override
    public ActionRequestValidationException validate() {

        ActionRequestValidationException exception = null;

        if (Strings.isNullOrEmpty(id())) {
            exception = addValidationError("[id] cannot be [null] or [\"\"]", exception);
        }
        if (Strings.isNullOrEmpty(value())) {
            exception = addValidationError("[value] cannot be [null] or [\"\"]", exception);
        }

        return exception;
    }

    public String id() {
        return id;
    }

    public String value() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PutConnectorSecretRequest request = (PutConnectorSecretRequest) o;
        return Objects.equals(id, request.id) && Objects.equals(value, request.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }
}
