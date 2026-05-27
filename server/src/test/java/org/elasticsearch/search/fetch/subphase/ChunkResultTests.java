/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch.subphase;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class ChunkResultTests extends ESTestCase {

    public void testChunkResultSerialization() throws IOException {
        ChunkResult original = new ChunkResult("test text", 0, 100, 0.95f);
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        ChunkResult deserialized = new ChunkResult(in);
        assertEquals(original, deserialized);
    }

    public void testChunkResultSerializationEmptyText() throws IOException {
        ChunkResult original = new ChunkResult("", 0, 0, 0.0f);
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        ChunkResult deserialized = new ChunkResult(in);
        assertEquals(original, deserialized);
    }

    public void testChunkResultSerializationLargeOffsets() throws IOException {
        ChunkResult original = new ChunkResult("some text", 100000, 999999, 0.5f);
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        ChunkResult deserialized = new ChunkResult(in);
        assertEquals(original, deserialized);
    }

    public void testChunkResultSerializationScoreZero() throws IOException {
        ChunkResult original = new ChunkResult("chunk text", 10, 50, 0.0f);
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        ChunkResult deserialized = new ChunkResult(in);
        assertEquals(original, deserialized);
    }

    public void testChunkResultSerializationScoreOne() throws IOException {
        ChunkResult original = new ChunkResult("chunk text", 10, 50, 1.0f);
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        ChunkResult deserialized = new ChunkResult(in);
        assertEquals(original, deserialized);
    }

    public void testOffsetValidationRejectsInvalidRange() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> new ChunkResult("text", 100, 50, 0.5f));
        assertTrue(ex.getMessage().contains("endOffset"));
        assertTrue(ex.getMessage().contains("startOffset"));
    }

    public void testOffsetValidationAcceptsEqualOffsets() {
        // startOffset == endOffset is valid (empty range)
        ChunkResult chunk = new ChunkResult("", 42, 42, 0.0f);
        assertEquals(42, chunk.startOffset());
        assertEquals(42, chunk.endOffset());
    }
}
