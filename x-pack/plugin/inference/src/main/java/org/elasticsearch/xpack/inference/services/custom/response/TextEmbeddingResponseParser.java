/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom.response;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResults;
import org.elasticsearch.xpack.inference.common.MapPathExtractor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;
import static org.elasticsearch.xpack.inference.services.custom.CustomServiceSettings.JSON_PARSER;

public class TextEmbeddingResponseParser extends BaseCustomResponseParser<TextEmbeddingFloatResults> {

    public static final String NAME = "text_embedding_response_parser";
    public static final String TEXT_EMBEDDING_PARSER_EMBEDDINGS = "text_embeddings";

    private final String textEmbeddingsPath;

    public static TextEmbeddingResponseParser fromMap(
        Map<String, Object> responseParserMap,
        String scope,
        ValidationException validationException
    ) {
        var path = extractRequiredString(
            responseParserMap,
            TEXT_EMBEDDING_PARSER_EMBEDDINGS,
            String.join(".", scope, JSON_PARSER),
            validationException
        );

        if (validationException.validationErrors().isEmpty() == false) {
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

    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
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
    protected TextEmbeddingFloatResults transform(Map<String, Object> map) {
        var extractedResult = MapPathExtractor.extract(map, textEmbeddingsPath);
        var mapResultsList = validateList(extractedResult.extractedObject(), extractedResult.getArrayFieldName(0));

        var embeddings = new ArrayList<TextEmbeddingFloatResults.Embedding>(mapResultsList.size());

        for (int i = 0; i < mapResultsList.size(); i++) {
            try {
                var entry = mapResultsList.get(i);
                var embeddingsAsListFloats = convertToListOfFloats(entry, extractedResult.getArrayFieldName(1));
                embeddings.add(TextEmbeddingFloatResults.Embedding.of(embeddingsAsListFloats));
            } catch (Exception e) {
                throw new IllegalArgumentException(
                    Strings.format("Failed to parse text embedding entry [%d], error: %s", i, e.getMessage()),
                    e
                );
            }
        }

        return new TextEmbeddingFloatResults(embeddings);
    }
}
