/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom.response;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;
import static org.elasticsearch.xpack.inference.services.custom.CustomServiceSettings.JSON_PARSER;

public class CompletionResponseParser implements ResponseParser {

    private static final String NAME = "completion_response_parser";
    public static final String COMPLETION_PARSER_RESULT = "completion_result";

    private final String completionResultPath;

    public static CompletionResponseParser fromMap(Map<String, Object> responseParserMap, ValidationException validationException) {
        var path = extractRequiredString(responseParserMap, COMPLETION_PARSER_RESULT, JSON_PARSER, validationException);

        if (path == null) {
            throw validationException;
        }

        return new CompletionResponseParser(path);
    }

    public CompletionResponseParser(String completionResultPath) {
        this.completionResultPath = Objects.requireNonNull(completionResultPath);
    }

    public CompletionResponseParser(StreamInput in) throws IOException {
        this.completionResultPath = in.readString();
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(completionResultPath);
    }

    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(JSON_PARSER);
        {
            builder.field(COMPLETION_PARSER_RESULT, completionResultPath);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CompletionResponseParser that = (CompletionResponseParser) o;
        return Objects.equals(completionResultPath, that.completionResultPath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(completionResultPath);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public InferenceServiceResults parse(HttpResult response) {
        return null;
    }
}
