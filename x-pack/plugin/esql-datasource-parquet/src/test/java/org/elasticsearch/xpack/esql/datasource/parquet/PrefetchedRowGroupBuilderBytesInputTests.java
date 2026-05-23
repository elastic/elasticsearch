/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.bytes.BytesInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Tests the wrap-don't-copy invariant of
 * {@link PrefetchedRowGroupBuilder#bytesInputFrom(ByteBuffer)} and
 * {@link PrefetchedRowGroupBuilder#bytesInputFromSlice(ByteBuffer, int, int)} for direct buffers.
 *
 * <p>"No copy" is verified by mutating the source buffer <em>after</em> constructing the
 * {@link BytesInput} and asserting the mutation is visible through it. If a copy had been made,
 * the {@code BytesInput} would still hold the original bytes and the assertion would fail.
 * (Reference identity does not work — the helpers wrap via {@code buffer.duplicate()}, which
 * returns a new {@code ByteBuffer} instance that shares backing memory.)
 *
 * <p>This invariant is what lets {@code PrefetchedPageReader.decompressToDirectBuffer} take its
 * zero-copy path on the prefetched route. Locking it here so an innocuous refactor of either
 * helper can't silently re-introduce a per-page heap copy.
 */
public class PrefetchedRowGroupBuilderBytesInputTests extends ESTestCase {

    public void testBytesInputFromDirectBufferSharesMemory() throws IOException {
        byte[] payload = randomByteArrayOfLength(128);
        ByteBuffer direct = ByteBuffer.allocateDirect(payload.length);
        direct.put(payload).flip();

        BytesInput bytesInput = PrefetchedRowGroupBuilder.bytesInputFrom(direct);

        // Mutate the source after wrapping; if no copy happened the BytesInput sees the change.
        // (~x is always != x for any byte, so the sentinel is guaranteed different from the original.)
        byte sentinel = (byte) ~payload[0];
        direct.put(0, sentinel);
        assertEquals(sentinel, bytesInput.toByteArray()[0]);
    }

    public void testBytesInputFromSliceDirectBufferSharesMemory() throws IOException {
        byte[] payload = randomByteArrayOfLength(256);
        ByteBuffer direct = ByteBuffer.allocateDirect(payload.length);
        direct.put(payload).flip();

        int offset = 64;
        int length = 96;
        BytesInput bytesInput = PrefetchedRowGroupBuilder.bytesInputFromSlice(direct, offset, length);

        // Mutate the first byte inside the slice window via the source; the slice must see it.
        // Also verifies that the slice starts at the requested offset (readBack[0] aligns with source[offset]).
        byte sentinel = (byte) ~payload[offset];
        direct.put(offset, sentinel);

        byte[] readBack = bytesInput.toByteArray();
        assertEquals(length, readBack.length);
        assertEquals(sentinel, readBack[0]);
        assertEquals("source position must not be advanced by slicing", 0, direct.position());
    }

    public void testBytesInputFromSliceZeroLengthReturnsEmpty() {
        // Zero-length hits an early return in the helper and never reads the buffer's bytes,
        // so the content/position/limit don't matter here.
        BytesInput bytesInput = PrefetchedRowGroupBuilder.bytesInputFromSlice(ByteBuffer.allocateDirect(16), 4, 0);
        assertEquals(0L, bytesInput.size());
    }
}
