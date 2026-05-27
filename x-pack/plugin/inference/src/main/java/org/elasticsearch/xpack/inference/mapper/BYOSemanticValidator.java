/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Static validation utilities for BYO (Bring Your Own) semantic text fields.
 * Validates individual chunks, detects overlaps across batches, and verifies
 * full coverage of the source text at commit time.
 */
public final class BYOSemanticValidator {

    private BYOSemanticValidator() {}

    /**
     * Validates a single chunk's offsets and embeddings.
     *
     * @param startOffset  character start offset (inclusive), must be &ge; 0
     * @param endOffset    character end offset (exclusive), must be &gt; startOffset
     * @param embeddings   raw embedding bytes; must be non-null and non-empty
     * @param textLength   total character length of the source text
     * @throws ElasticsearchStatusException if any constraint is violated
     */
    public static void validateChunk(int startOffset, int endOffset, BytesReference embeddings, int textLength) {
        if (startOffset < 0) {
            throw new ElasticsearchStatusException("start_offset [{}] must be non-negative", RestStatus.BAD_REQUEST, startOffset);
        }
        if (endOffset <= 0) {
            throw new ElasticsearchStatusException("end_offset [{}] must be positive", RestStatus.BAD_REQUEST, endOffset);
        }
        if (startOffset >= endOffset) {
            throw new ElasticsearchStatusException(
                "start_offset [{}] must be less than end_offset [{}]",
                RestStatus.BAD_REQUEST,
                startOffset,
                endOffset
            );
        }
        if (endOffset > textLength) {
            throw new ElasticsearchStatusException(
                "end_offset [{}] exceeds text_length [{}]",
                RestStatus.BAD_REQUEST,
                endOffset,
                textLength
            );
        }
        if (embeddings == null || embeddings.length() == 0) {
            throw new ElasticsearchStatusException("embeddings are required", RestStatus.BAD_REQUEST);
        }
    }

    /**
     * Validates that no chunk in {@code incomingChunks} overlaps with any chunk in
     * {@code existingChunks}, and that no two chunks within the incoming batch overlap
     * each other. Gaps between chunks are permitted at stage time.
     *
     * <p>Algorithm: merge both lists, sort by {@code startOffset}, then verify pairwise
     * that {@code curr.startOffset >= prev.endOffset}.
     *
     * @param existingChunks chunks already staged for this document field
     * @param incomingChunks chunks being submitted in the current request
     * @throws ElasticsearchStatusException if any overlap is detected
     */
    public static void validateNoOverlaps(List<SemanticTextField.Chunk> existingChunks, List<SemanticTextField.Chunk> incomingChunks) {
        List<SemanticTextField.Chunk> all = new ArrayList<>(existingChunks.size() + incomingChunks.size());
        all.addAll(existingChunks);
        all.addAll(incomingChunks);
        all.sort(Comparator.comparingInt(SemanticTextField.Chunk::startOffset));

        for (int i = 1; i < all.size(); i++) {
            SemanticTextField.Chunk prev = all.get(i - 1);
            SemanticTextField.Chunk curr = all.get(i);
            if (curr.startOffset() < prev.endOffset()) {
                throw new ElasticsearchStatusException(
                    "Chunk [{}, {}) overlaps with existing chunk [{}, {})",
                    RestStatus.BAD_REQUEST,
                    curr.startOffset(),
                    curr.endOffset(),
                    prev.startOffset(),
                    prev.endOffset()
                );
            }
        }
    }

    /**
     * Validates that the provided chunks form a contiguous, gap-free cover of
     * {@code [0, textLength)}. The chunks need not be ordered on input; this method
     * sorts them by {@code startOffset} before checking.
     *
     * @param chunks     all committed chunks for the field
     * @param textLength expected total character length of the source text
     * @throws ElasticsearchStatusException if the chunks leave any gap or do not start at 0
     */
    public static void validateFullCoverage(List<SemanticTextField.Chunk> chunks, int textLength) {
        if (chunks.isEmpty()) {
            throw new ElasticsearchStatusException("No chunks provided; the full text must be covered", RestStatus.BAD_REQUEST);
        }

        List<SemanticTextField.Chunk> sorted = new ArrayList<>(chunks);
        sorted.sort(Comparator.comparingInt(SemanticTextField.Chunk::startOffset));

        SemanticTextField.Chunk first = sorted.get(0);
        if (first.startOffset() != 0) {
            throw new ElasticsearchStatusException(
                "Gap detected: text is not covered starting from position 0. Gap detected at [{}, {})",
                RestStatus.BAD_REQUEST,
                0,
                first.startOffset()
            );
        }

        for (int i = 1; i < sorted.size(); i++) {
            SemanticTextField.Chunk prev = sorted.get(i - 1);
            SemanticTextField.Chunk curr = sorted.get(i);
            if (curr.startOffset() != prev.endOffset()) {
                int gapStart = prev.endOffset();
                int gapEnd = curr.startOffset();
                throw new ElasticsearchStatusException(
                    "Gap detected in chunk coverage at [{}, {})",
                    RestStatus.BAD_REQUEST,
                    gapStart,
                    gapEnd
                );
            }
        }

        SemanticTextField.Chunk last = sorted.get(sorted.size() - 1);
        if (last.endOffset() != textLength) {
            throw new ElasticsearchStatusException(
                "Gap detected in chunk coverage at [{}, {})",
                RestStatus.BAD_REQUEST,
                last.endOffset(),
                textLength
            );
        }
    }

    /**
     * Validates that the embedding vector stored in {@code embeddings} has exactly
     * {@code expectedDims} dimensions. The embeddings may be either a bare JSON array
     * (e.g. {@code [0.1,0.2,0.3]}) or an object with a {@code "values"} array
     * (e.g. {@code {"values":[0.1,0.2,0.3]}}).
     *
     * @param embeddings   raw embedding bytes in JSON format
     * @param expectedDims the number of dimensions the model produces
     * @param fieldName    name of the semantic_text field (for error messages)
     * @param startOffset  chunk start offset (for error messages)
     * @param endOffset    chunk end offset (for error messages)
     * @throws ElasticsearchStatusException if the dimension count does not match
     */
    public static void validateEmbeddingDimensions(
        BytesReference embeddings,
        int expectedDims,
        String fieldName,
        int startOffset,
        int endOffset
    ) {
        int actualDims = countEmbeddingDimensions(embeddings);
        if (actualDims != expectedDims) {
            throw new ElasticsearchStatusException(
                "Expected {} dimensions for field [{}] chunk [{}, {}), but got {}",
                RestStatus.BAD_REQUEST,
                expectedDims,
                fieldName,
                startOffset,
                endOffset,
                actualDims
            );
        }
    }

    private static int countEmbeddingDimensions(BytesReference embeddings) {
        try (
            XContentParser parser = XContentHelper.createParserNotCompressed(
                XContentParserConfiguration.EMPTY,
                embeddings,
                XContentType.JSON
            )
        ) {
            XContentParser.Token token = parser.nextToken();
            if (token == XContentParser.Token.START_ARRAY) {
                // Bare array format [0.1, 0.2, ...] — matches the inference pipeline output
                int count = 0;
                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                    count++;
                }
                return count;
            } else if (token == XContentParser.Token.START_OBJECT) {
                // Object format {"values": [0.1, 0.2, ...]}
                while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                    String fieldName = parser.currentName();
                    parser.nextToken();
                    if ("values".equals(fieldName)) {
                        int count = 0;
                        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                            count++;
                        }
                        return count;
                    } else {
                        parser.skipChildren();
                    }
                }
                throw new ElasticsearchStatusException("Embedding JSON does not contain a 'values' array", RestStatus.BAD_REQUEST);
            } else {
                throw new ElasticsearchStatusException("Unexpected embedding format", RestStatus.BAD_REQUEST);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
