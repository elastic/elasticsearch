/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom.response;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ErrorResponse;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;
import static org.elasticsearch.xpack.inference.services.custom.CustomServiceSettings.ERROR_PARSER;
import static org.elasticsearch.xpack.inference.services.custom.CustomServiceSettings.RESPONSE;

public class ErrorResponseParser implements ToXContentFragment, NamedWriteable, Function<HttpResult, ErrorResponse> {

    private static final String NAME = "error_response_parser";
    public static final String MESSAGE_PATH = "path";

    private final String messagePath;

    public static ErrorResponseParser fromMap(Map<String, Object> responseParserMap, ValidationException validationException) {
        var path = extractRequiredString(responseParserMap, MESSAGE_PATH, RESPONSE, validationException);

        if (path == null) {
            throw validationException;
        }

        return new ErrorResponseParser(path);
    }

    public ErrorResponseParser(String messagePath) {
        this.messagePath = Objects.requireNonNull(messagePath);
    }

    public ErrorResponseParser(StreamInput in) throws IOException {
        this.messagePath = in.readString();
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(messagePath);
    }

    public String getMessagePath() {
        return messagePath;
    }

    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(ERROR_PARSER);
        {
            builder.field(MESSAGE_PATH, messagePath);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ErrorResponseParser that = (ErrorResponseParser) o;
        return Objects.equals(messagePath, that.messagePath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(messagePath);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public ErrorResponse apply(HttpResult httpResult) {
        return null;
    }
}
