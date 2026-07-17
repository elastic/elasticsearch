/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.compress;

import com.github.luben.zstd.Zstd;

import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.nio.ByteBuffer;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Behavioral tests for {@link PanamaZstd}, the shared zero-copy zstd helper that {@code parquet}
 * (and, in the future, {@code orc} / {@code iceberg}) format readers route their direct-buffer
 * page decompression through. Compresses with the {@code zstd-jni} byte-array API to produce a
 * known-good frame, then asserts the Panama path decompresses it identically with absolute
 * offsets and without mutating either buffer's cursor.
 */
public class PanamaZstdTests extends ESTestCase {

    @Before
    public void requireNativeZstd() throws Exception {
        assumeTrue("PanamaZstd requires the native zstd binding to be available on this platform", PanamaZstd.instance().isAvailable());
    }

    public void testInstanceIsCachedAndAvailable() {
        PanamaZstd one = PanamaZstd.instance();
        PanamaZstd two = PanamaZstd.instance();
        assertSame(one, two);
        assertTrue("expected Panama zstd to be available on a platform where native loaded", one.isAvailable());
    }

    public void testRoundTripSingleByte() {
        assertRoundTrip(new byte[] { 'z' });
    }

    public void testRoundTripCompressible() {
        byte[] data = new byte[between(100, 4096)];
        java.util.Arrays.fill(data, (byte) 7);
        assertRoundTrip(data);
    }

    public void testRoundTripIncompressible() {
        byte[] data = randomByteArrayOfLength(between(100, 4096));
        assertRoundTrip(data);
    }

    /**
     * Round-trip with non-zero leading slack on both buffers and {@code position()} / {@code limit()}
     * set to a window that does <em>not</em> match the slack. The Panama call must use the absolute
     * offsets passed in and leave both cursors untouched. Regression test for
     * {@code MemorySegment.ofBuffer(buf).asSlice(offset, size)} accidentally being position-relative.
     */
    public void testAbsoluteOffsetsIgnoreCurrentPositionAndLimit() {
        byte[] original = randomByteArrayOfLength(between(100, 4096));
        byte[] compressed = jniCompress(original);

        int srcLeading = between(1, 64);
        int srcTrailing = between(0, 64);
        ByteBuffer src = ByteBuffer.allocateDirect(srcLeading + compressed.length + srcTrailing);
        for (int i = 0; i < compressed.length; i++) {
            src.put(srcLeading + i, compressed[i]);
        }
        // Set a misleading window: position/limit don't match the absolute offset/size we pass.
        src.position(0);
        src.limit(src.capacity());

        int dstLeading = between(1, 64);
        int dstTrailing = between(0, 64);
        ByteBuffer dst = ByteBuffer.allocateDirect(dstLeading + original.length + dstTrailing);
        dst.position(0);
        dst.limit(dst.capacity());

        int written = PanamaZstd.instance().decompressDirect(dst, dstLeading, original.length, src, srcLeading, compressed.length);

        assertThat(written, equalTo(original.length));
        // Cursors must be untouched.
        assertThat(src.position(), equalTo(0));
        assertThat(src.limit(), equalTo(src.capacity()));
        assertThat(dst.position(), equalTo(0));
        assertThat(dst.limit(), equalTo(dst.capacity()));
        // Decompressed payload lands at the absolute dst offset.
        for (int i = 0; i < original.length; i++) {
            assertThat("byte " + i, dst.get(dstLeading + i), equalTo(original[i]));
        }
    }

    // Argument-validation behavior (null buffers, heap rejection, out-of-range offsets, corrupt
    // frames) is owned by org.elasticsearch.nativeaccess.Zstd and exhaustively covered by
    // ZstdTests in libs/native — re-testing it here would only re-prove delegation. The
    // wrapper-specific coverage stays focused on singleton identity, availability, absolute
    // offsets, and the IllegalStateException unavailable path.

    /**
     * The unavailable case is exercised via the package-private constructor (since the singleton
     * caches whatever NativeAccess returns at class init). Verifies callers get a clear
     * {@link IllegalStateException} instead of an obscure NPE.
     */
    public void testUnavailableInstanceThrowsIllegalState() {
        PanamaZstd unavailable = new PanamaZstd(null);
        assertFalse(unavailable.isAvailable());
        ByteBuffer dst = ByteBuffer.allocateDirect(8);
        ByteBuffer src = ByteBuffer.allocateDirect(8);
        var ex = expectThrows(IllegalStateException.class, () -> unavailable.decompressDirect(dst, 0, 1, src, 0, 1));
        assertThat(ex.getMessage(), containsString("not available"));
    }

    // ---------- Heap byte[] overloads (Phase 2: replaces the parquet cold path's zstd-jni call) ----------

    public void testDecompressHeapRoundTrip() {
        byte[] original = randomByteArrayOfLength(between(100, 4096));
        byte[] compressed = jniCompress(original);
        byte[] decompressed = new byte[original.length];
        int written = PanamaZstd.instance().decompressHeap(decompressed, compressed);
        assertThat(written, equalTo(original.length));
        assertArrayEquals(original, decompressed);
    }

    public void testCompressHeapByteForByteWithZstdJni() {
        byte[] original = randomByteArrayOfLength(between(100, 4096));
        // Both libraries call into the same libzstd binary, so for the same input + level they
        // must produce the same frame. This is the strongest cross-check that compressHeap binds
        // ZSTD_compress with the right calling convention.
        byte[] jniCompressed = jniCompress(original);
        byte[] panamaOut = new byte[PanamaZstd.instance().compressBound(original.length)];
        int panamaLen = PanamaZstd.instance().compressHeap(panamaOut, 0, panamaOut.length, original, 0, original.length, 3);
        byte[] panamaCompressed = java.util.Arrays.copyOf(panamaOut, panamaLen);
        assertArrayEquals(jniCompressed, panamaCompressed);
    }

    /**
     * Cross-direction: compress via Panama, decompress via the zstd-jni reference. Catches a
     * regression in compressHeap's libzstd error reporting (e.g. truncating a valid frame).
     */
    public void testPanamaCompressRoundTripsViaZstdJni() {
        byte[] original = randomByteArrayOfLength(between(100, 4096));
        byte[] panamaOut = new byte[PanamaZstd.instance().compressBound(original.length)];
        int panamaLen = PanamaZstd.instance().compressHeap(panamaOut, 0, panamaOut.length, original, 0, original.length, 3);
        byte[] compressed = java.util.Arrays.copyOf(panamaOut, panamaLen);
        byte[] decompressed = Zstd.decompress(compressed, original.length);
        assertArrayEquals(original, decompressed);
    }

    public void testUnavailableDecompressHeapThrowsIllegalState() {
        PanamaZstd unavailable = new PanamaZstd(null);
        var ex = expectThrows(IllegalStateException.class, () -> unavailable.decompressHeap(new byte[8], new byte[8]));
        assertThat(ex.getMessage(), containsString("not available"));
    }

    public void testUnavailableCompressHeapThrowsIllegalState() {
        PanamaZstd unavailable = new PanamaZstd(null);
        var ex = expectThrows(IllegalStateException.class, () -> unavailable.compressHeap(new byte[64], 0, 64, new byte[8], 0, 8, 3));
        assertThat(ex.getMessage(), containsString("not available"));
    }

    private void assertRoundTrip(byte[] original) {
        byte[] compressed = jniCompress(original);

        ByteBuffer src = ByteBuffer.allocateDirect(compressed.length);
        src.put(compressed).flip();
        ByteBuffer dst = ByteBuffer.allocateDirect(original.length);

        int written = PanamaZstd.instance().decompressDirect(dst, 0, original.length, src, 0, compressed.length);
        assertThat(written, equalTo(original.length));

        byte[] roundTripped = new byte[original.length];
        for (int i = 0; i < original.length; i++) {
            roundTripped[i] = dst.get(i);
        }
        assertArrayEquals(original, roundTripped);
    }

    private static byte[] jniCompress(byte[] data) {
        return Zstd.compress(data);
    }
}
