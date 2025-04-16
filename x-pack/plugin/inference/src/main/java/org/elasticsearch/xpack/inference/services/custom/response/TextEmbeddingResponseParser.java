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
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;
import static org.elasticsearch.xpack.inference.services.custom.CustomServiceSettings.JSON_PARSER;

public class TextEmbeddingResponseParser implements ResponseParser {

    public static final String NAME = "text_embedding_response_parser";
    public static final String TEXT_EMBEDDING_PARSER_EMBEDDINGS = "text_embeddings";

    private final String textEmbeddingsPath;

    public static TextEmbeddingResponseParser fromMap(Map<String, Object> responseParserMap, ValidationException validationException) {
        var path = extractRequiredString(responseParserMap, TEXT_EMBEDDING_PARSER_EMBEDDINGS, JSON_PARSER, validationException);

        if (path == null) {
            throw validationException;
        }

        return new TextEmbeddingResponseParser(path);
    }

    public TextEmbeddingResponseParser(String textEmbeddingsPath) {
        this.textEmbeddingsPath = Objects.requireNonNull(textEmbeddingsPath);
    }

    public TextEmbeddingResponseParser(StreamInput in) throws IOException {
        this.textEmbeddingsPath = in.readString();
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(textEmbeddingsPath);
    }

    public String getPath() {
        return textEmbeddingsPath;
    }

    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(JSON_PARSER);
        {
            builder.field(TEXT_EMBEDDING_PARSER_EMBEDDINGS, textEmbeddingsPath);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TextEmbeddingResponseParser that = (TextEmbeddingResponseParser) o;
        return Objects.equals(textEmbeddingsPath, that.textEmbeddingsPath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(textEmbeddingsPath);
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
