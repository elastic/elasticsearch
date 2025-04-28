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
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResults;
import org.elasticsearch.xpack.core.ml.search.WeightedToken;
import org.elasticsearch.xpack.inference.common.MapPathExtractor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;
import static org.elasticsearch.xpack.inference.services.custom.CustomServiceSettings.JSON_PARSER;

public class SparseEmbeddingResponseParser extends BaseCustomResponseParser<SparseEmbeddingResults> {

    public static final String NAME = "sparse_embedding_response_parser";
    public static final String SPARSE_EMBEDDING_TOKEN_PATH = "token_path";
    public static final String SPARSE_EMBEDDING_WEIGHT_PATH = "weight_path";

    private final String tokenPath;
    private final String weightPath;

    public static SparseEmbeddingResponseParser fromMap(Map<String, Object> responseParserMap, ValidationException validationException) {
        var tokenPath = extractRequiredString(responseParserMap, SPARSE_EMBEDDING_TOKEN_PATH, JSON_PARSER, validationException);

        var weightPath = extractRequiredString(responseParserMap, SPARSE_EMBEDDING_WEIGHT_PATH, JSON_PARSER, validationException);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new SparseEmbeddingResponseParser(tokenPath, weightPath);
    }

    public SparseEmbeddingResponseParser(String tokenPath, String weightPath) {
        this.tokenPath = Objects.requireNonNull(tokenPath);
        this.weightPath = Objects.requireNonNull(weightPath);
    }

    public SparseEmbeddingResponseParser(StreamInput in) throws IOException {
        this.tokenPath = in.readString();
        this.weightPath = in.readString();
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(tokenPath);
        out.writeString(weightPath);
    }

    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(JSON_PARSER);
        {
            builder.field(SPARSE_EMBEDDING_TOKEN_PATH, tokenPath);
            builder.field(SPARSE_EMBEDDING_WEIGHT_PATH, weightPath);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SparseEmbeddingResponseParser that = (SparseEmbeddingResponseParser) o;
        return Objects.equals(tokenPath, that.tokenPath) && Objects.equals(weightPath, that.weightPath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tokenPath, weightPath);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected SparseEmbeddingResults transform(Map<String, Object> map) {
        // These will be List<List<T>>
        var tokenResult = MapPathExtractor.extract(map, tokenPath);
        var tokens = validateList(tokenResult.extractedObject(), tokenResult.getArrayFieldName(0));

        // These will be List<List<T>>
        var weightResult = MapPathExtractor.extract(map, weightPath);
        var weights = validateList(weightResult.extractedObject(), weightResult.getArrayFieldName(0));

        validateListsSize(tokens, weights);

        var tokenEntryFieldName = tokenResult.getArrayFieldName(1);
        var weightEntryFieldName = weightResult.getArrayFieldName(1);
        var embeddings = new ArrayList<SparseEmbeddingResults.Embedding>();
        for (int responseCounter = 0; responseCounter < tokens.size(); responseCounter++) {
            try {
                var tokenEntryList = validateList(tokens.get(responseCounter), tokenEntryFieldName);
                var weightEntryList = validateList(weights.get(responseCounter), weightEntryFieldName);

                validateListsSize(tokenEntryList, weightEntryList);

                embeddings.add(createEmbedding(tokenEntryList, weightEntryList, weightEntryFieldName));
            } catch (Exception e) {
                throw new IllegalStateException(
                    Strings.format("Failed to parse sparse embedding entry [%d], error: %s", responseCounter, e.getMessage()),
                    e
                );
            }
        }

        return new SparseEmbeddingResults(Collections.unmodifiableList(embeddings));
    }

    private static void validateListsSize(List<?> tokens, List<?> weights) {
        if (tokens.size() != weights.size()) {
            throw new IllegalStateException(
                Strings.format(
                    "The extracted tokens list is size [%d] but the weights list is size [%d]. The list sizes must be equal.",
                    tokens.size(),
                    weights.size()
                )
            );
        }
    }

    private static SparseEmbeddingResults.Embedding createEmbedding(
        List<?> tokenEntryList,
        List<?> weightEntryList,
        String weightFieldName
    ) {
        var weightedTokens = new ArrayList<WeightedToken>();

        for (int embeddingCounter = 0; embeddingCounter < tokenEntryList.size(); embeddingCounter++) {
            var token = tokenEntryList.get(embeddingCounter);
            var weight = weightEntryList.get(embeddingCounter);

            // Alibaba can return a token id which is an integer and needs to be converted to a string
            var tokenIdAsString = token.toString();
            try {
                var weightAsFloat = toFloat(weight, weightFieldName);
                weightedTokens.add(new WeightedToken(tokenIdAsString, weightAsFloat));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                    Strings.format("Failed to parse weight item: [%d] of array, error: %s", embeddingCounter, e.getMessage()),
                    e
                );
            }
        }

        return new SparseEmbeddingResults.Embedding(weightedTokens, false);
    }
}
