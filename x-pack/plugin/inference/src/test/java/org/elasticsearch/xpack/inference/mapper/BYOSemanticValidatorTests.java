/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class BYOSemanticValidatorTests extends ESTestCase {

    // -----------------------------------------------------------------------
    // Task 3: Per-Chunk Validation
    // -----------------------------------------------------------------------

    private static BytesReference validEmbeddings() {
        return new BytesArray("{\"values\":[0.1,0.2,0.3]}");
    }

    public void testValidChunk() {
        // Should not throw
        BYOSemanticValidator.validateChunk(0, 100, validEmbeddings(), 200);
    }

    public void testNegativeStartOffset() {
        ElasticsearchStatusException ex = expectThrows(
            ElasticsearchStatusException.class,
            () -> BYOSemanticValidator.validateChunk(-1, 100, validEmbeddings(), 200)
        );
        assertThat(ex.getMessage(), containsString("start_offset [-1] must be non-negative"));
    }

    public void testNegativeEndOffset() {
        ElasticsearchStatusException ex = expectThrows(
            ElasticsearchStatusException.class,
            () -> BYOSemanticValidator.validateChunk(0, -5, validEmbeddings(), 200)
        );
        assertThat(ex.getMessage(), containsString("end_offset [-5] must be positive"));
    }

    public void testStartOffsetEqualsEndOffset() {
        ElasticsearchStatusException ex = expectThrows(
            ElasticsearchStatusException.class,
            () -> BYOSemanticValidator.validateChunk(50, 50, validEmbeddings(), 200)
        );
        assertThat(ex.getMessage(), containsString("start_offset [50] must be less than end_offset [50]"));
    }

    public void testStartOffsetGreaterThanEndOffset() {
        ElasticsearchStatusException ex = expectThrows(
            ElasticsearchStatusException.class,
            () -> BYOSemanticValidator.validateChunk(100, 50, validEmbeddings(), 200)
        );
        assertThat(ex.getMessage(), containsString("start_offset [100] must be less than end_offset [50]"));
    }

    public void testEndOffsetExceedsTextLength() {
        ElasticsearchStatusException ex = expectThrows(
            ElasticsearchStatusException.class,
            () -> BYOSemanticValidator.validateChunk(0, 201, validEmbeddings(), 200)
        );
        assertThat(ex.getMessage(), containsString("end_offset [201] exceeds text_length [200]"));
    }

    public void testEndOffsetEqualsTextLengthIsValid() {
        // Boundary: endOffset == textLength is valid
        BYOSemanticValidator.validateChunk(0, 200, validEmbeddings(), 200);
    }

    public void testNullEmbeddings() {
        ElasticsearchStatusException ex = expectThrows(
            ElasticsearchStatusException.class,
            () -> BYOSemanticValidator.validateChunk(0, 100, null, 200)
        );
        assertThat(ex.getMessage(), containsString("embeddings are required"));
    }

    public void testEmptyEmbeddings() {
        ElasticsearchStatusException ex = expectThrows(
            ElasticsearchStatusException.class,
            () -> BYOSemanticValidator.validateChunk(0, 100, new BytesArray(new byte[0]), 200)
        );
        assertThat(ex.getMessage(), containsString("embeddings are required"));
    }

    public void testChunkSpanningEntireText() {
        // Should not throw
        BYOSemanticValidator.validateChunk(0, 500, validEmbeddings(), 500);
    }

    public void testChunkAtEndOfText() {
        // Should not throw
        BYOSemanticValidator.validateChunk(150, 200, validEmbeddings(), 200);
    }

    // -----------------------------------------------------------------------
    // Task 4: Cross-Chunk Overlap Detection
    // -----------------------------------------------------------------------

    private static SemanticTextField.Chunk chunk(int start, int end) {
        return new SemanticTextField.Chunk(start, end, new BytesArray("{\"values\":[0.1]}"));
    }

    public void testNoOverlapAdjacentChunks() {
        List<SemanticTextField.Chunk> existing = List.of(chunk(0, 100));
        List<SemanticTextField.Chunk> incoming = List.of(chunk(100, 200));
        // Should not throw
        BYOSemanticValidator.validateNoOverlaps(existing, incoming);
    }

    public void testOverlapWithExistingChunk() {
        List<SemanticTextField.Chunk> existing = List.of(chunk(0, 100));
        List<SemanticTextField.Chunk> incoming = List.of(chunk(50, 150));
        ElasticsearchStatusException ex = expectThrows(
            ElasticsearchStatusException.class,
            () -> BYOSemanticValidator.validateNoOverlaps(existing, incoming)
        );
        assertThat(ex.getMessage(), containsString("overlaps"));
    }

    public void testOverlapWithinIncomingBatch() {
        List<SemanticTextField.Chunk> existing = Collections.emptyList();
        List<SemanticTextField.Chunk> incoming = List.of(chunk(0, 100), chunk(50, 150));
        ElasticsearchStatusException ex = expectThrows(
            ElasticsearchStatusException.class,
            () -> BYOSemanticValidator.validateNoOverlaps(existing, incoming)
        );
        assertThat(ex.getMessage(), containsString("overlaps"));
    }

    public void testDuplicateRangeRejected() {
        List<SemanticTextField.Chunk> existing = List.of(chunk(0, 100));
        List<SemanticTextField.Chunk> incoming = List.of(chunk(0, 100));
        ElasticsearchStatusException ex = expectThrows(
            ElasticsearchStatusException.class,
            () -> BYOSemanticValidator.validateNoOverlaps(existing, incoming)
        );
        assertThat(ex.getMessage(), containsString("overlaps"));
    }

    public void testNonContiguousNonOverlappingChunksAllowed() {
        // Gaps are OK at stage time
        List<SemanticTextField.Chunk> existing = List.of(chunk(0, 50));
        List<SemanticTextField.Chunk> incoming = List.of(chunk(100, 200));
        // Should not throw
        BYOSemanticValidator.validateNoOverlaps(existing, incoming);
    }

    public void testOutOfOrderChunksNoOverlap() {
        List<SemanticTextField.Chunk> existing = List.of(chunk(200, 300));
        List<SemanticTextField.Chunk> incoming = List.of(chunk(0, 100), chunk(100, 200));
        // Should not throw
        BYOSemanticValidator.validateNoOverlaps(existing, incoming);
    }

    public void testManyChunksPartialOverlap() {
        List<SemanticTextField.Chunk> existing = List.of(chunk(0, 100), chunk(100, 200), chunk(200, 300));
        List<SemanticTextField.Chunk> incoming = List.of(chunk(299, 400));
        ElasticsearchStatusException ex = expectThrows(
            ElasticsearchStatusException.class,
            () -> BYOSemanticValidator.validateNoOverlaps(existing, incoming)
        );
        assertThat(ex.getMessage(), containsString("overlaps"));
    }

    public void testSameChunkTwiceInIncomingBatch() {
        List<SemanticTextField.Chunk> existing = Collections.emptyList();
        List<SemanticTextField.Chunk> incoming = List.of(chunk(0, 100), chunk(0, 100));
        ElasticsearchStatusException ex = expectThrows(
            ElasticsearchStatusException.class,
            () -> BYOSemanticValidator.validateNoOverlaps(existing, incoming)
        );
        assertThat(ex.getMessage(), containsString("overlaps"));
    }

    // -----------------------------------------------------------------------
    // Task 5: Commit-Time Coverage + Dimensional Validation
    // -----------------------------------------------------------------------

    public void testFullCoverage() {
        List<SemanticTextField.Chunk> chunks = List.of(chunk(0, 100), chunk(100, 200), chunk(200, 300));
        // Should not throw
        BYOSemanticValidator.validateFullCoverage(chunks, 300);
    }

    public void testFullCoverageOutOfOrder() {
        // Method must sort before checking
        List<SemanticTextField.Chunk> chunks = List.of(chunk(200, 300), chunk(0, 100), chunk(100, 200));
        // Should not throw
        BYOSemanticValidator.validateFullCoverage(chunks, 300);
    }

    public void testGapAtStart() {
        List<SemanticTextField.Chunk> chunks = List.of(chunk(10, 100));
        ElasticsearchStatusException ex = expectThrows(
            ElasticsearchStatusException.class,
            () -> BYOSemanticValidator.validateFullCoverage(chunks, 100)
        );
        assertThat(ex.getMessage(), containsString("[0, 10)"));
    }

    public void testGapInMiddle() {
        List<SemanticTextField.Chunk> chunks = List.of(chunk(0, 100), chunk(150, 300));
        ElasticsearchStatusException ex = expectThrows(
            ElasticsearchStatusException.class,
            () -> BYOSemanticValidator.validateFullCoverage(chunks, 300)
        );
        assertThat(ex.getMessage(), containsString("[100, 150)"));
    }

    public void testGapAtEnd() {
        List<SemanticTextField.Chunk> chunks = List.of(chunk(0, 100));
        ElasticsearchStatusException ex = expectThrows(
            ElasticsearchStatusException.class,
            () -> BYOSemanticValidator.validateFullCoverage(chunks, 200)
        );
        assertThat(ex.getMessage(), containsString("[100, 200)"));
    }

    public void testEmptyChunksListFails() {
        ElasticsearchStatusException ex = expectThrows(
            ElasticsearchStatusException.class,
            () -> BYOSemanticValidator.validateFullCoverage(Collections.emptyList(), 100)
        );
        assertThat(ex.getMessage(), containsString("No chunks"));
    }

    public void testSingleChunkFullCoverage() {
        List<SemanticTextField.Chunk> chunks = List.of(chunk(0, 500));
        // Should not throw
        BYOSemanticValidator.validateFullCoverage(chunks, 500);
    }

    public void testMaximumSizeTextFieldCoverage() {
        int textLength = 1_000_000;
        int chunkSize = 512;
        List<SemanticTextField.Chunk> chunks = new ArrayList<>();
        int offset = 0;
        while (offset < textLength) {
            int end = Math.min(offset + chunkSize, textLength);
            chunks.add(chunk(offset, end));
            offset = end;
        }
        // Should not throw (~1953 chunks)
        BYOSemanticValidator.validateFullCoverage(chunks, textLength);
    }

    public void testValidDimensions() {
        BytesReference embeddings = new BytesArray("{\"values\":[0.1,0.2,0.3]}");
        // Should not throw
        BYOSemanticValidator.validateEmbeddingDimensions(embeddings, 3, "my_field", 0, 100);
    }

    public void testDimensionMismatch() {
        BytesReference embeddings = new BytesArray("{\"values\":[0.1,0.2,0.3]}");
        ElasticsearchStatusException ex = expectThrows(
            ElasticsearchStatusException.class,
            () -> BYOSemanticValidator.validateEmbeddingDimensions(embeddings, 768, "my_field", 0, 100)
        );
        assertThat(ex.getMessage(), containsString("Expected 768 dimensions"));
    }
}
