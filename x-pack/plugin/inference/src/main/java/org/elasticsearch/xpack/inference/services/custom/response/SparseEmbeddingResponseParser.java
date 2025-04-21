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
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;
import static org.elasticsearch.xpack.inference.services.custom.CustomServiceSettings.JSON_PARSER;

public class SparseEmbeddingResponseParser extends BaseCustomResponseParser<SparseEmbeddingResults> {

    public static final String NAME = "sparse_embedding_response_parser";
    public static final String SPARSE_RESULT_PATH = "path";
    public static final String SPARSE_EMBEDDING_TOKEN_FIELD_NAME = "token_field_name";
    public static final String SPARSE_EMBEDDING_WEIGHT_FIELD_NAME = "weight_field_name";

    private final String path;
    private final String tokenFieldName;
    private final String weightFieldName;

    public static SparseEmbeddingResponseParser fromMap(Map<String, Object> responseParserMap, ValidationException validationException) {
        var path = extractRequiredString(responseParserMap, SPARSE_RESULT_PATH, JSON_PARSER, validationException);

        var tokenFieldName = extractRequiredString(responseParserMap, SPARSE_EMBEDDING_TOKEN_FIELD_NAME, JSON_PARSER, validationException);

        var weightFieldName = extractRequiredString(
            responseParserMap,
            SPARSE_EMBEDDING_WEIGHT_FIELD_NAME,
            JSON_PARSER,
            validationException
        );

        if (path == null || tokenFieldName == null || weightFieldName == null) {
            throw validationException;
        }

        return new SparseEmbeddingResponseParser(path, tokenFieldName, weightFieldName);
    }

    public SparseEmbeddingResponseParser(String path, String tokenFieldName, String weightFieldName) {
        this.path = Objects.requireNonNull(path);
        this.tokenFieldName = Objects.requireNonNull(tokenFieldName);
        this.weightFieldName = Objects.requireNonNull(weightFieldName);
    }

    public SparseEmbeddingResponseParser(StreamInput in) throws IOException {
        this.path = in.readString();
        this.tokenFieldName = in.readString();
        this.weightFieldName = in.readString();
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(path);
        out.writeString(tokenFieldName);
        out.writeString(weightFieldName);
    }

    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(JSON_PARSER);
        {
            builder.field(SPARSE_RESULT_PATH, path);
            builder.field(SPARSE_EMBEDDING_TOKEN_FIELD_NAME, tokenFieldName);
            builder.field(SPARSE_EMBEDDING_WEIGHT_FIELD_NAME, weightFieldName);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SparseEmbeddingResponseParser that = (SparseEmbeddingResponseParser) o;
        return Objects.equals(path, that.path)
            && Objects.equals(tokenFieldName, that.tokenFieldName)
            && Objects.equals(weightFieldName, that.weightFieldName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, tokenFieldName, weightFieldName);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    /**
     * The result from the upstream server must be in the following format
     * <pre>
     * {@code
     *   {
     *      "sparse_embeddings": [
     *            {
     *                "embedding": [
     *                    {
     *                        "tokenId": 6,
     *                        "weight": 0.10137939453125
     *                    },
     *                    {
     *                        "tokenId": 163040,
     *                        "weight": 0.2841796875
     *                    }
     *                ]
     *            }
     *        ]
     *    }
     * }
     *
     * Where the token ids and weight are in a map.
     * </pre>
     */
    @Override
    protected SparseEmbeddingResults transform(Map<String, Object> map) {
        // This should be a List<List<Map<String, Object>>>
        var pathList = validateList(MapPathExtractor.extract(map, path));

        var embeddings = new ArrayList<SparseEmbeddingResults.Embedding>();

        for (var listEntry : pathList) {
            embeddings.add(parseExpansionResult(listEntry));
        }

        return new SparseEmbeddingResults(Collections.unmodifiableList(embeddings));
    }

    private SparseEmbeddingResults.Embedding parseExpansionResult(Object listEntry) {
        var weightedTokens = new ArrayList<WeightedToken>();

        // This should be a List<Map<String, Object>>
        var listTokenIdWeights = validateList(listEntry);

        for (var tokenIdWeightEntry : listTokenIdWeights) {
            var tokenIdWeightMap = validateMap(tokenIdWeightEntry);
            var tokenId = tokenIdWeightMap.get(tokenFieldName);
            if (tokenId == null) {
                throw new IllegalStateException(Strings.format("Failed to find token id field: [%s]", tokenFieldName));
            }

            // Alibaba can return a token id which is an integer and needs to be converted to a string
            var tokenIdAsString = tokenId.toString();

            var weight = tokenIdWeightMap.get(weightFieldName);
            if (weight == null) {
                throw new IllegalStateException(Strings.format("Failed to find weight field: [%s]", weightFieldName));
            }

            var weightAsFloat = toFloat(weight);
            weightedTokens.add(new WeightedToken(tokenIdAsString, weightAsFloat));
        }

        return new SparseEmbeddingResults.Embedding(weightedTokens, false);
    }
}
