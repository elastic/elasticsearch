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

public class SparseEmbeddingResponseParser implements ResponseParser {

    private static final String NAME = "sparse_embedding_response_parser";
    public static final String SPARSE_RESULT_PATH = "path";
    public static final String SPARSE_EMBEDDING_PARSER_TOKEN = "sparse_token";
    public static final String SPARSE_EMBEDDING_PARSER_WEIGHT = "sparse_weight";

    private final String path;
    private final String tokenPath;
    private final String weightPath;

    public static SparseEmbeddingResponseParser fromMap(Map<String, Object> responseParserMap, ValidationException validationException) {
        var path = extractRequiredString(responseParserMap, SPARSE_RESULT_PATH, JSON_PARSER, validationException);

        var tokenPath = extractRequiredString(
            responseParserMap,
            SPARSE_EMBEDDING_PARSER_TOKEN,
            JSON_PARSER,
            validationException
        );

        var weightPath = extractRequiredString(
            responseParserMap,
            SPARSE_EMBEDDING_PARSER_WEIGHT,
            JSON_PARSER,
            validationException
        );

        if (path == null || tokenPath == null || weightPath == null) {
            throw validationException;
        }

        return new SparseEmbeddingResponseParser(path, tokenPath, weightPath);
    }

    public SparseEmbeddingResponseParser(String path, String tokenPath, String weightPath) {
        this.path = Objects.requireNonNull(path);
        this.tokenPath = Objects.requireNonNull(tokenPath);
        this.weightPath = Objects.requireNonNull(weightPath);
    }

    public SparseEmbeddingResponseParser(StreamInput in) throws IOException {
        this.path = in.readString();
        this.tokenPath = in.readString();
        this.weightPath = in.readString();
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(path);
        out.writeString(tokenPath);
        out.writeString(weightPath);
    }

    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(JSON_PARSER);
        {
            builder.field(SPARSE_RESULT_PATH, path);
            builder.field(SPARSE_EMBEDDING_PARSER_TOKEN, tokenPath);
            builder.field(SPARSE_EMBEDDING_PARSER_WEIGHT, weightPath);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SparseEmbeddingResponseParser that = (SparseEmbeddingResponseParser) o;
        return Objects.equals(path, that.path) && Objects.equals(tokenPath, that.tokenPath) && Objects.equals(weightPath, that.weightPath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, tokenPath, weightPath);
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
