/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public record ChunkedInferenceServiceResults(List<Result> results) implements InferenceServiceResults {
    public static final String NAME = "chunked_inference_service_result";

    public ChunkedInferenceServiceResults(StreamInput in) throws IOException {
        this(in.readCollectionAsList(Result::new));
    }

    public static ChunkedInferenceServiceResults of(List<String> originalText, List<ChunkedInference> chunkedInferenceList) {
        if (originalText.size() != chunkedInferenceList.size()) {
            throw new IllegalArgumentException("Original text and chunked inference list must have the same size.");
        }

        List<Result> chunkedInferenceResults = new ArrayList<>();
        for (int i = 0; i < originalText.size(); i++) {
            var chunkedInference = chunkedInferenceList.get(i);
            if (chunkedInference instanceof ChunkedInferenceEmbedding chunkedInferenceEmbedding) {
                chunkedInferenceResults.add(
                    new Result(chunkedInferenceEmbeddingAsChunksWithChunkText(originalText.get(i), chunkedInferenceEmbedding))
                );
            } else if (chunkedInference instanceof ChunkedInferenceError chunkedInferenceError) {
                throw new ElasticsearchStatusException(
                    "Received error chunked inference result.",
                    RestStatus.INTERNAL_SERVER_ERROR,
                    chunkedInferenceError.exception()
                );

            } else {
                throw new IllegalArgumentException(
                    "Received invalid chunked inference result of type "
                        + chunkedInference.getClass().getName()
                        + " but expected ChunkedInferenceEmbedding"
                );
            }
        }

        return new ChunkedInferenceServiceResults(chunkedInferenceResults);
    }

    private static List<ChunkWithChunkText> chunkedInferenceEmbeddingAsChunksWithChunkText(
        String originalText,
        ChunkedInferenceEmbedding chunkedInferenceEmbedding
    ) {
        List<ChunkedInferenceServiceResults.ChunkWithChunkText> chunkedInferenceChunks = new ArrayList<>();
        for (EmbeddingResults.Chunk embeddingResultsChunk : chunkedInferenceEmbedding.chunks()) {
            chunkedInferenceChunks.add(
                new ChunkedInferenceServiceResults.ChunkWithChunkText(
                    originalText.substring(embeddingResultsChunk.offset().start(), embeddingResultsChunk.offset().end()),
                    embeddingResultsChunk.embedding()
                )
            );
        }
        return chunkedInferenceChunks;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(results);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return ChunkedToXContentHelper.array(NAME, results.iterator());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ChunkedInferenceServiceResults that = (ChunkedInferenceServiceResults) o;
        return Objects.equals(results, that.results);
    }

    @Override
    public int hashCode() {
        return Objects.hash(results);
    }

    public record Result(List<ChunkWithChunkText> chunks) implements ToXContentObject, Writeable {
        public Result(StreamInput in) throws IOException {
            this(in.readCollectionAsList(ChunkWithChunkText::new));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(chunks);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startArray();
            for (ChunkWithChunkText chunk : chunks) {
                chunk.toXContent(builder, params);
            }
            builder.endArray();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Result that = (Result) o;
            return Objects.equals(chunks, that.chunks);
        }

        @Override
        public int hashCode() {
            return Objects.hash(chunks);
        }
    }

    private enum EmbeddingType {
        SPARSE,
        TEXT_FLOAT,
        TEXT_BYTE
    }

    record ChunkWithChunkText(String chunkText, EmbeddingResults.Embedding<?> embedding) implements ToXContentObject, Writeable {
        ChunkWithChunkText(StreamInput in) throws IOException {
            this(in.readString(), readEmbedding(in));
        }

        private static EmbeddingResults.Embedding<?> readEmbedding(StreamInput in) throws IOException {
            var embeddingType = in.readEnum(EmbeddingType.class);
            return switch (embeddingType) {
                case SPARSE -> new SparseEmbeddingResults.Embedding(in);
                case TEXT_FLOAT -> new TextEmbeddingFloatResults.Embedding(in);
                case TEXT_BYTE -> new TextEmbeddingByteResults.Embedding(in);
            };
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("chunk_text", chunkText);
            builder.field("embedding", embedding);
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(chunkText);
            out.writeEnum(getEmbeddingTypeFromEmbedding());
            out.writeWriteable(embedding);
        }

        private EmbeddingType getEmbeddingTypeFromEmbedding() {
            if (embedding instanceof SparseEmbeddingResults.Embedding) {
                return EmbeddingType.SPARSE;
            } else if (embedding instanceof TextEmbeddingFloatResults.Embedding) {
                return EmbeddingType.TEXT_FLOAT;
            } else if (embedding instanceof TextEmbeddingByteResults.Embedding) {
                return EmbeddingType.TEXT_BYTE;
            } else {
                throw new IllegalArgumentException("Unknown embedding type: " + embedding.getClass().getName());
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ChunkWithChunkText that = (ChunkWithChunkText) o;
            return Objects.equals(chunkText, that.chunkText) && Objects.equals(embedding, that.embedding);
        }

        @Override
        public int hashCode() {
            return Objects.hash(chunkText, embedding);
        }
    }
}
