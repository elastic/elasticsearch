/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;
import org.elasticsearch.xpack.core.ml.search.WeightedToken;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig.DEFAULT_RESULTS_FIELD;

public record SparseEmbeddingResults(List<Embedding> embeddings) implements EmbeddingResults<SparseEmbeddingResults.Embedding> {

    public static final String NAME = "sparse_embedding_results";
    public static final String SPARSE_EMBEDDING = TaskType.SPARSE_EMBEDDING.toString();

    public SparseEmbeddingResults(StreamInput in) throws IOException {
        this(in.readCollectionAsList(SparseEmbeddingResults.Embedding::new));
    }

    public static SparseEmbeddingResults of(List<? extends InferenceResults> results) {
        List<Embedding> embeddings = new ArrayList<>(results.size());

        for (InferenceResults result : results) {
            if (result instanceof TextExpansionResults expansionResults) {
                embeddings.add(
                    SparseEmbeddingResults.Embedding.create(expansionResults.getWeightedTokens(), expansionResults.isTruncated())
                );
            } else if (result instanceof org.elasticsearch.xpack.core.ml.inference.results.ErrorInferenceResults errorResult) {
                if (errorResult.getException() instanceof ElasticsearchStatusException statusException) {
                    throw statusException;
                } else {
                    throw new ElasticsearchStatusException(
                        "Received error inference result.",
                        RestStatus.INTERNAL_SERVER_ERROR,
                        errorResult.getException()
                    );
                }
            } else {
                throw new IllegalArgumentException(
                    "Received invalid legacy inference result, of type "
                        + result.getClass().getName()
                        + " but expected SparseEmbeddingResults."
                );
            }
        }

        return new SparseEmbeddingResults(embeddings);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return ChunkedToXContentHelper.array(SPARSE_EMBEDDING, embeddings.iterator());
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(embeddings);
    }

    public Map<String, Object> asMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        var embeddingList = embeddings.stream().map(SparseEmbeddingResults.Embedding::asMap).toList();

        map.put(SPARSE_EMBEDDING, embeddingList);
        return map;
    }

    @Override
    public List<? extends InferenceResults> transformToCoordinationFormat() {
        return embeddings.stream()
            .map(
                embedding -> new TextExpansionResults(
                    DEFAULT_RESULTS_FIELD,
                    embedding.tokens()
                        .stream()
                        .map(weightedToken -> new WeightedToken(weightedToken.token(), weightedToken.weight()))
                        .toList(),
                    embedding.isTruncated
                )
            )
            .toList();
    }

    public record Embedding(List<WeightedToken> tokens, boolean isTruncated)
        implements
            Writeable,
            ToXContentObject,
            EmbeddingResults.Embedding<Embedding> {

        public static final String EMBEDDING = "embedding";
        public static final String IS_TRUNCATED = "is_truncated";

        public Embedding(StreamInput in) throws IOException {
            this(in.readCollectionAsList(WeightedToken::new), in.readBoolean());
        }

        public static Embedding create(List<WeightedToken> weightedTokens, boolean isTruncated) {
            return new Embedding(
                weightedTokens.stream().map(token -> new WeightedToken(token.token(), token.weight())).toList(),
                isTruncated
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(tokens);
            out.writeBoolean(isTruncated);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(IS_TRUNCATED, isTruncated);
            builder.startObject(EMBEDDING);

            for (var weightedToken : tokens) {
                weightedToken.toXContent(builder, params);
            }

            builder.endObject();
            builder.endObject();
            return builder;
        }

        public Map<String, Object> asMap() {
            var embeddingMap = new LinkedHashMap<String, Object>(
                tokens.stream().collect(Collectors.toMap(WeightedToken::token, WeightedToken::weight))
            );

            return new LinkedHashMap<>(Map.of(IS_TRUNCATED, isTruncated, EMBEDDING, embeddingMap));
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }

        @Override
        public Embedding merge(Embedding embedding) {
            // This code assumes that the tokens are sorted by weight in descending order.
            // If that's not the case, the resulting merged embedding will be incorrect.
            List<WeightedToken> mergedTokens = new ArrayList<>();
            Set<String> seenTokens = new HashSet<>();
            int i = 0;
            int j = 0;
            // TODO: maybe truncate tokens here when it's getting too large?
            while (i < tokens().size() || j < embedding.tokens().size()) {
                WeightedToken token;
                if (i == tokens().size()) {
                    token = embedding.tokens().get(j++);
                } else if (j == embedding.tokens().size()) {
                    token = tokens().get(i++);
                } else if (tokens.get(i).weight() > embedding.tokens().get(j).weight()) {
                    token = tokens().get(i++);
                } else {
                    token = embedding.tokens().get(j++);
                }
                if (seenTokens.add(token.token())) {
                    mergedTokens.add(token);
                }
            }
            boolean mergedIsTruncated = isTruncated || embedding.isTruncated();
            return new Embedding(mergedTokens, mergedIsTruncated);
        }

        @Override
        public BytesReference toBytesRef(XContent xContent) throws IOException {
            XContentBuilder b = XContentBuilder.builder(xContent);
            b.startObject();
            for (var weightedToken : tokens) {
                weightedToken.toXContent(b, ToXContent.EMPTY_PARAMS);
            }
            b.endObject();
            return BytesReference.bytes(b);
        }
    }
}
