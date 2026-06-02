/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess;

import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;
import org.junit.BeforeClass;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.hamcrest.Matchers.equalTo;

public class ZstdTests extends ESTestCase {

    static NativeAccess nativeAccess;
    static Zstd zstd;

    @BeforeClass
    public static void getZstd() {
        nativeAccess = NativeAccess.instance();
        zstd = nativeAccess.getZstd();
    }

    public void testCompressBound() {
        assertThat(zstd.compressBound(0), Matchers.greaterThanOrEqualTo(1));
        assertThat(zstd.compressBound(100), Matchers.greaterThanOrEqualTo(100));
        expectThrows(IllegalArgumentException.class, () -> zstd.compressBound(Integer.MAX_VALUE));
        expectThrows(IllegalArgumentException.class, () -> zstd.compressBound(-1));
        expectThrows(IllegalArgumentException.class, () -> zstd.compressBound(-100));
        expectThrows(IllegalArgumentException.class, () -> zstd.compressBound(Integer.MIN_VALUE));
    }

    public void testCompressValidation() {
        try (var src = nativeAccess.newConfinedBuffer(1000); var dst = nativeAccess.newConfinedBuffer(500)) {
            var srcBuf = src.buffer();
            var dstBuf = dst.buffer();

            var npe1 = expectThrows(NullPointerException.class, () -> zstd.compress(null, src, 0));
            assertThat(npe1.getMessage(), equalTo("Null destination buffer"));
            var npe2 = expectThrows(NullPointerException.class, () -> zstd.compress(dst, null, 0));
            assertThat(npe2.getMessage(), equalTo("Null source buffer"));

            // dst capacity too low
            for (int i = 0; i < srcBuf.remaining(); ++i) {
                srcBuf.put(i, randomByte());
            }
            var e = expectThrows(IllegalArgumentException.class, () -> zstd.compress(dst, src, 0));
            assertThat(e.getMessage(), equalTo("Destination buffer is too small"));
        }
    }

    public void testDecompressValidation() {
        try (
            var original = nativeAccess.newConfinedBuffer(1000);
            var compressed = nativeAccess.newConfinedBuffer(500);
            var restored = nativeAccess.newConfinedBuffer(500)
        ) {
            var originalBuf = original.buffer();
            var compressedBuf = compressed.buffer();

            var npe1 = expectThrows(NullPointerException.class, () -> zstd.decompress(null, original));
            assertThat(npe1.getMessage(), equalTo("Null destination buffer"));
            var npe2 = expectThrows(NullPointerException.class, () -> zstd.decompress(compressed, (CloseableByteBuffer) null));
            assertThat(npe2.getMessage(), equalTo("Null source buffer"));

            // Invalid compressed format
            for (int i = 0; i < originalBuf.remaining(); ++i) {
                originalBuf.put(i, (byte) i);
            }
            var e = expectThrows(IllegalArgumentException.class, () -> zstd.decompress(compressed, original));
            assertThat(e.getMessage(), equalTo("Unknown frame descriptor"));

            int compressedLength = zstd.compress(compressed, original, 0);
            compressedBuf.limit(compressedLength);
            e = expectThrows(IllegalArgumentException.class, () -> zstd.decompress(restored, compressed));
            assertThat(e.getMessage(), equalTo("Destination buffer is too small"));

        }
    }

    public void testDecompressDirectByteBufferValidation() {
        try (var dst = nativeAccess.newConfinedBuffer(500)) {
            var npe1 = expectThrows(NullPointerException.class, () -> zstd.decompress(null, ByteBuffer.allocateDirect(1)));
            assertThat(npe1.getMessage(), equalTo("Null destination buffer"));
            var npe2 = expectThrows(NullPointerException.class, () -> zstd.decompress(dst, (ByteBuffer) null));
            assertThat(npe2.getMessage(), equalTo("Null source buffer"));

            var heapBuf = ByteBuffer.allocate(100);
            var iae = expectThrows(IllegalArgumentException.class, () -> zstd.decompress(dst, heapBuf));
            assertThat(iae.getMessage(), equalTo("Source buffer must be direct"));
        }
    }

    /**
     * Validation surface of the explicit-offset {@code decompress(ByteBuffer, int, int, ByteBuffer, int, int)} overload:
     * null guards, direct-only enforcement on both sides, and absolute-offset bounds checking.
     */
    public void testDecompressDirectByteBufferExplicitOffsetValidation() {
        ByteBuffer direct = ByteBuffer.allocateDirect(64);
        ByteBuffer heap = ByteBuffer.allocate(64);

        var npe1 = expectThrows(NullPointerException.class, () -> zstd.decompress(null, 0, 1, direct, 0, 1));
        assertThat(npe1.getMessage(), equalTo("Null destination buffer"));
        var npe2 = expectThrows(NullPointerException.class, () -> zstd.decompress(direct, 0, 1, null, 0, 1));
        assertThat(npe2.getMessage(), equalTo("Null source buffer"));

        var iae1 = expectThrows(IllegalArgumentException.class, () -> zstd.decompress(heap, 0, 1, direct, 0, 1));
        assertThat(iae1.getMessage(), equalTo("Destination buffer must be direct"));
        var iae2 = expectThrows(IllegalArgumentException.class, () -> zstd.decompress(direct, 0, 1, heap, 0, 1));
        assertThat(iae2.getMessage(), equalTo("Source buffer must be direct"));

        // Offset / size must fit within capacity (capacity, not remaining — these are absolute offsets).
        expectThrows(IllegalArgumentException.class, () -> zstd.decompress(direct, -1, 1, direct, 0, 1));
        expectThrows(IllegalArgumentException.class, () -> zstd.decompress(direct, 0, -1, direct, 0, 1));
        expectThrows(IllegalArgumentException.class, () -> zstd.decompress(direct, 64, 1, direct, 0, 1));
        expectThrows(IllegalArgumentException.class, () -> zstd.decompress(direct, 0, 65, direct, 0, 1));
        // overflow-style: offset + size overflows int but is caught by the (offset > capacity - size) check
        expectThrows(IllegalArgumentException.class, () -> zstd.decompress(direct, Integer.MAX_VALUE, 1, direct, 0, 1));
    }

    public void testOneByteDirectByteBufferExplicitOffset() {
        doTestRoundtripWithExplicitOffset(new byte[] { 'z' });
    }

    /**
     * Regression for the case where the caller passes a sliced direct {@link ByteBuffer}
     * — i.e. {@code parent.slice(parentOffset, sliceCapacity)} — instead of a freshly allocated
     * one. {@code MemorySegment.ofBuffer} accounts for the slice's offset into its parent
     * automatically for direct buffers, so {@code dst.duplicate().clear()} followed by
     * {@code asSlice(offset, size)} must address bytes within the slice's window, never the
     * surrounding parent memory.
     */
    public void testRoundtripWithSlicedDirectByteBuffer() {
        byte[] data = new byte[randomIntBetween(100, 1000)];
        for (int i = 0; i < data.length; ++i) {
            data[i] = (byte) (i & 0x0F);
        }

        byte[] compressedBytes;
        try (
            var original = nativeAccess.newConfinedBuffer(data.length);
            var compressed = nativeAccess.newConfinedBuffer(zstd.compressBound(data.length))
        ) {
            original.buffer().put(0, data);
            int compressedLength = zstd.compress(compressed, original, randomIntBetween(-3, 9));
            compressedBytes = new byte[compressedLength];
            for (int i = 0; i < compressedLength; i++) {
                compressedBytes[i] = compressed.buffer().get(i);
            }
        }

        // Build large parent buffers and take slices that start at non-zero offsets in their
        // parents. The slices are what the API receives; their capacity is the slice window only.
        final int srcParentLeading = randomIntBetween(8, 64);
        final int srcSliceLeading = randomIntBetween(1, 32);
        final int srcSliceTrailing = randomIntBetween(0, 32);
        final int srcSliceCapacity = srcSliceLeading + compressedBytes.length + srcSliceTrailing;
        ByteBuffer srcParent = ByteBuffer.allocateDirect(srcParentLeading + srcSliceCapacity + randomIntBetween(0, 32));
        // Pre-fill the parent before/after the slice with sentinel bytes; if our slicing is wrong
        // and we read past the slice, the decompression will see garbage and fail.
        for (int i = 0; i < srcParent.capacity(); i++) {
            srcParent.put(i, (byte) 0xCC);
        }
        for (int i = 0; i < compressedBytes.length; i++) {
            srcParent.put(srcParentLeading + srcSliceLeading + i, compressedBytes[i]);
        }
        ByteBuffer src = srcParent.slice(srcParentLeading, srcSliceCapacity);

        final int dstParentLeading = randomIntBetween(8, 64);
        final int dstSliceLeading = randomIntBetween(1, 32);
        final int dstSliceTrailing = randomIntBetween(0, 32);
        final int dstSliceCapacity = dstSliceLeading + data.length + dstSliceTrailing;
        ByteBuffer dstParent = ByteBuffer.allocateDirect(dstParentLeading + dstSliceCapacity + randomIntBetween(0, 32));
        for (int i = 0; i < dstParent.capacity(); i++) {
            dstParent.put(i, (byte) 0xDD);
        }
        ByteBuffer dst = dstParent.slice(dstParentLeading, dstSliceCapacity);

        int decompressed = zstd.decompress(dst, dstSliceLeading, data.length, src, srcSliceLeading, compressedBytes.length);

        assertThat(decompressed, equalTo(data.length));
        // Decompressed bytes must land at the slice's logical offset, leaving the slice's
        // leading and trailing sentinel bytes (and the parent's surrounding memory) untouched.
        for (int i = 0; i < data.length; i++) {
            assertThat("byte " + i + " in slice", dst.get(dstSliceLeading + i), equalTo(data[i]));
        }
        for (int i = 0; i < dstSliceLeading; i++) {
            assertThat("dst slice leading byte " + i + " must remain sentinel", dst.get(i), equalTo((byte) 0xDD));
        }
        for (int i = 0; i < dstSliceTrailing; i++) {
            assertThat(
                "dst slice trailing byte " + i + " must remain sentinel",
                dst.get(dstSliceLeading + data.length + i),
                equalTo((byte) 0xDD)
            );
        }
        // Parent memory before and after the slice must be untouched.
        for (int i = 0; i < dstParentLeading; i++) {
            assertThat("dst parent leading byte " + i + " must remain sentinel", dstParent.get(i), equalTo((byte) 0xDD));
        }
        for (int i = dstParentLeading + dstSliceCapacity; i < dstParent.capacity(); i++) {
            assertThat("dst parent trailing byte " + i + " must remain sentinel", dstParent.get(i), equalTo((byte) 0xDD));
        }
    }

    public void testConstantDirectByteBufferExplicitOffset() {
        byte[] b = new byte[randomIntBetween(100, 1000)];
        Arrays.fill(b, randomByte());
        doTestRoundtripWithExplicitOffset(b);
    }

    public void testCycleDirectByteBufferExplicitOffset() {
        byte[] b = new byte[randomIntBetween(100, 1000)];
        for (int i = 0; i < b.length; ++i) {
            b[i] = (byte) (i & 0x0F);
        }
        doTestRoundtripWithExplicitOffset(b);
    }

    /**
     * Round-trip exercising {@link Zstd#decompress(ByteBuffer, int, int, ByteBuffer, int, int)} with absolute,
     * non-zero offsets and non-zero {@link ByteBuffer#position()}/{@link ByteBuffer#limit()} on both buffers.
     * Mirrors how parquet-mr's {@code BytesInputDecompressor} feeds the API: the caller passes
     * {@code input.position()} as the source offset, which must be interpreted absolutely (not added on top
     * of the buffer's current position), and the call must not modify either buffer's cursor.
     */
    private void doTestRoundtripWithExplicitOffset(byte[] data) {
        // First produce a compressed payload using the existing CloseableByteBuffer API so we have known-good bytes.
        byte[] compressedBytes;
        try (
            var original = nativeAccess.newConfinedBuffer(data.length);
            var compressed = nativeAccess.newConfinedBuffer(zstd.compressBound(data.length))
        ) {
            original.buffer().put(0, data);
            int compressedLength = zstd.compress(compressed, original, randomIntBetween(-3, 9));
            compressedBytes = new byte[compressedLength];
            for (int i = 0; i < compressedLength; i++) {
                compressedBytes[i] = compressed.buffer().get(i);
            }
        }

        // Place the compressed bytes at a non-zero offset in a larger direct buffer, similar to how parquet
        // hands a compressed page slice that starts inside a larger column-chunk buffer.
        final int srcLeading = randomIntBetween(1, 64);
        final int srcTrailing = randomIntBetween(0, 64);
        ByteBuffer src = ByteBuffer.allocateDirect(srcLeading + compressedBytes.length + srcTrailing);
        for (int i = 0; i < compressedBytes.length; i++) {
            src.put(srcLeading + i, compressedBytes[i]);
        }
        // Set position/limit to a window that does not start at 0 — the explicit offset must override these.
        src.position(srcLeading);
        src.limit(srcLeading + compressedBytes.length);

        final int dstLeading = randomIntBetween(1, 64);
        final int dstTrailing = randomIntBetween(0, 64);
        ByteBuffer dst = ByteBuffer.allocateDirect(dstLeading + data.length + dstTrailing);
        // Set position/limit on dst to an unrelated, mismatched window: the explicit (dstLeading, length)
        // offset/size pair must override these and land bytes at the absolute dstLeading offset, not at
        // position(). This catches any regression where position is implicitly added to the offset.
        final int dstUnrelatedPos = (dstLeading > 0) ? dstLeading - 1 : dstLeading + 1;
        dst.position(Math.min(dstUnrelatedPos, dst.capacity()));
        dst.limit(dst.capacity());
        final int dstStartPos = dst.position();
        final int dstStartLimit = dst.limit();

        int decompressed = zstd.decompress(dst, dstLeading, data.length, src, srcLeading, compressedBytes.length);

        assertThat(decompressed, equalTo(data.length));
        // Position/limit on both buffers must be unchanged — javadoc contract.
        assertThat(src.position(), equalTo(srcLeading));
        assertThat(src.limit(), equalTo(srcLeading + compressedBytes.length));
        assertThat(dst.position(), equalTo(dstStartPos));
        assertThat(dst.limit(), equalTo(dstStartLimit));
        // Decompressed payload must land at the absolute offset, not relative to position().
        for (int i = 0; i < data.length; i++) {
            assertThat("byte " + i, dst.get(dstLeading + i), equalTo(data[i]));
        }
    }

    public void testOneByte() {
        doTestRoundtrip(new byte[] { 'z' });
    }

    public void testOneByteDirectByteBuffer() {
        doTestRoundtripWithDirectByteBuffer(new byte[] { 'z' });
    }

    public void testConstant() {
        byte[] b = new byte[randomIntBetween(100, 1000)];
        Arrays.fill(b, randomByte());
        doTestRoundtrip(b);
    }

    public void testConstantDirectByteBuffer() {
        byte[] b = new byte[randomIntBetween(100, 1000)];
        Arrays.fill(b, randomByte());
        doTestRoundtripWithDirectByteBuffer(b);
    }

    public void testCycle() {
        byte[] b = new byte[randomIntBetween(100, 1000)];
        for (int i = 0; i < b.length; ++i) {
            b[i] = (byte) (i & 0x0F);
        }
        doTestRoundtrip(b);
    }

    public void testCycleDirectByteBuffer() {
        byte[] b = new byte[randomIntBetween(100, 1000)];
        for (int i = 0; i < b.length; ++i) {
            b[i] = (byte) (i & 0x0F);
        }
        doTestRoundtripWithDirectByteBuffer(b);
    }

    /**
     * Compress with CloseableByteBuffer, then decompress using a direct ByteBuffer source
     * to exercise the {@link Zstd#decompress(CloseableByteBuffer, ByteBuffer)} overload.
     */
    private void doTestRoundtripWithDirectByteBuffer(byte[] data) {
        try (
            var original = nativeAccess.newConfinedBuffer(data.length);
            var compressed = nativeAccess.newConfinedBuffer(zstd.compressBound(data.length));
            var restored = nativeAccess.newConfinedBuffer(data.length)
        ) {
            original.buffer().put(0, data);
            int compressedLength = zstd.compress(compressed, original, randomIntBetween(-3, 9));

            ByteBuffer directSrc = ByteBuffer.allocateDirect(compressedLength);
            for (int i = 0; i < compressedLength; i++) {
                directSrc.put(i, compressed.buffer().get(i));
            }

            int decompressedLength = zstd.decompress(restored, directSrc);
            assertThat(restored.buffer(), equalTo(original.buffer()));
            assertThat(decompressedLength, equalTo(data.length));
        }
    }

    private void doTestRoundtrip(byte[] data) {
        try (
            var original = nativeAccess.newConfinedBuffer(data.length);
            var compressed = nativeAccess.newConfinedBuffer(zstd.compressBound(data.length));
            var restored = nativeAccess.newConfinedBuffer(data.length)
        ) {
            original.buffer().put(0, data);
            int compressedLength = zstd.compress(compressed, original, randomIntBetween(-3, 9));
            compressed.buffer().limit(compressedLength);
            int decompressedLength = zstd.decompress(restored, compressed);
            assertThat(restored.buffer(), equalTo(original.buffer()));
            assertThat(decompressedLength, equalTo(data.length));
        }

        // Now with non-zero offsets
        final int compressedOffset = randomIntBetween(1, 1000);
        final int decompressedOffset = randomIntBetween(1, 1000);
        try (
            var original = nativeAccess.newConfinedBuffer(decompressedOffset + data.length);
            var compressed = nativeAccess.newConfinedBuffer(compressedOffset + zstd.compressBound(data.length));
            var restored = nativeAccess.newConfinedBuffer(decompressedOffset + data.length)
        ) {
            original.buffer().put(decompressedOffset, data);
            original.buffer().position(decompressedOffset);
            compressed.buffer().position(compressedOffset);
            int compressedLength = zstd.compress(compressed, original, randomIntBetween(-3, 9));
            compressed.buffer().limit(compressedOffset + compressedLength);
            restored.buffer().position(decompressedOffset);
            int decompressedLength = zstd.decompress(restored, compressed);
            assertThat(decompressedLength, equalTo(data.length));
            assertThat(
                restored.buffer().slice(decompressedOffset, data.length),
                equalTo(original.buffer().slice(decompressedOffset, data.length))
            );
        }
    }

    /**
     * Regression test for the {@code JAVA_INT} vs {@code JAVA_LONG} descriptor bug on the heap
     * {@code decompress(byte[], int, int, byte[], int, int)} path (fixed in {@code JdkZstdLibrary}).
     * <p>
     * The wrong descriptor caused C2 to emit a native call with stale upper-32 bits in the {@code srcSize}
     * register on x86-64, making ZSTD see a {@code srcSize} like {@code 0xXXXXXXXX_00000020} instead of
     * {@code 0x20}. This test validates the correct round-trip on the heap path using large enough payloads
     * to span the range of sizes where the upper-bits corruption would produce a wrong value.
     */
    public void testHeapRoundtripSmall() {
        byte[] data = new byte[randomIntBetween(1, 100)];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) randomInt();
        }
        doTestHeapRoundtrip(data);
    }

    public void testHeapRoundtripMedium() {
        byte[] data = new byte[randomIntBetween(1_000, 100_000)];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) randomInt();
        }
        doTestHeapRoundtrip(data);
    }

    private void doTestHeapRoundtrip(byte[] data) {
        byte[] compressed = new byte[zstd.compressBound(data.length)];
        int compressedLen = zstd.compress(compressed, 0, compressed.length, data, 0, data.length, 1);
        byte[] restored = new byte[data.length];
        int written = zstd.decompress(restored, 0, restored.length, compressed, 0, compressedLen);
        assertThat(written, equalTo(data.length));
        assertThat(restored, equalTo(data));
    }

    // ---- Streaming API (DStream) tests -----------------------------------------------------------

    public void testDStreamInSizeIsReasonable() {
        // ZSTD_DStreamInSize is constant across libzstd's lifetime; for 1.5.7 it returns 128 KB but
        // we don't bake the exact value in — any sane recommendation in the 4 KB .. 16 MB range
        // catches an accidental misbinding to a different libzstd export (e.g. ZSTD_CStreamInSize)
        // without coupling to a specific upstream release.
        int size = zstd.dStreamInSize();
        assertThat(size, Matchers.greaterThanOrEqualTo(4 * 1024));
        assertThat(size, Matchers.lessThanOrEqualTo(16 * 1024 * 1024));

        int outSize = zstd.dStreamOutSize();
        assertThat(outSize, Matchers.greaterThanOrEqualTo(4 * 1024));
        assertThat(outSize, Matchers.lessThanOrEqualTo(16 * 1024 * 1024));
    }

    public void testDStreamRoundTripSmall() {
        doTestDStreamRoundTrip(new byte[] { 'z' });
        byte[] one = new byte[1024];
        for (int i = 0; i < one.length; i++) {
            one[i] = (byte) (i & 0x1F);
        }
        doTestDStreamRoundTrip(one);
    }

    public void testDStreamRoundTripMedium() {
        byte[] data = new byte[1024 * 1024];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) ((i * 31) & 0xFF);
        }
        doTestDStreamRoundTrip(data);
    }

    public void testDStreamRoundTripLarge() {
        // 10 MB exercises the multi-chunk refill loop end-to-end with the default 128 KB input
        // staging buffer (the production wrapper feeds the decoder in srcBuffSize chunks).
        byte[] data = new byte[10 * 1024 * 1024];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) ((i * 17) ^ (i >>> 8));
        }
        doTestDStreamRoundTrip(data);
    }

    public void testDStreamPartialInputDripFeed() {
        // Feed the decoder one byte at a time and assert it progresses without throwing. This is the
        // configuration that historically catches refill-condition bugs in the streaming wrapper.
        byte[] data = new byte[8 * 1024];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i & 0x0F);
        }
        byte[] compressed = compressBytes(data);

        byte[] decompressed = new byte[data.length];
        try (Zstd.DStream dstream = zstd.newDStream()) {
            int srcConsumed = 0;
            int dstWritten = 0;
            long lastHint = 1L;
            // Drive the decoder one input byte at a time; each call also passes the (potentially
            // grown) output prefix and lets libzstd resume writing where it left off.
            while (srcConsumed < compressed.length) {
                lastHint = dstream.decompress(decompressed, dstWritten, decompressed.length, compressed, srcConsumed, srcConsumed + 1);
                srcConsumed = dstream.lastSrcPos();
                dstWritten = dstream.lastDstPos();
            }
            assertThat("frame should be fully decoded", lastHint, equalTo(0L));
            assertThat(dstWritten, equalTo(data.length));
        }
        assertArrayEquals(data, decompressed);
    }

    public void testDStreamPartialOutputForcesMultipleCalls() {
        // Provide an output window much smaller than the decoded payload; libzstd will return a
        // positive hint each time, telling us to keep going. Verifies the hint > 0 / hint == 0
        // contract our wrapper depends on.
        byte[] data = new byte[64 * 1024];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i & 0x7F);
        }
        byte[] compressed = compressBytes(data);
        // Output slot is intentionally small enough to require many iterations through the loop.
        final int outChunk = 17;
        byte[] outBuf = new byte[outChunk];
        byte[] all = new byte[data.length];
        try (Zstd.DStream dstream = zstd.newDStream()) {
            int srcPos = 0;
            int totalWritten = 0;
            long hint;
            do {
                hint = dstream.decompress(outBuf, 0, outChunk, compressed, srcPos, compressed.length);
                int wroteNow = dstream.lastDstPos();
                System.arraycopy(outBuf, 0, all, totalWritten, wroteNow);
                totalWritten += wroteNow;
                srcPos = dstream.lastSrcPos();
            } while (hint != 0L || totalWritten < data.length);
            assertThat(totalWritten, equalTo(data.length));
        }
        assertArrayEquals(data, all);
    }

    public void testDStreamMultiFrameConcatenated() {
        // libzstd implicitly resets the decoder between concatenated frames. Verify that the same
        // DStream decodes a two-frame payload byte-for-byte to its uncompressed concatenation.
        byte[] frameAData = new byte[8 * 1024];
        byte[] frameBData = new byte[5 * 1024];
        for (int i = 0; i < frameAData.length; i++) {
            frameAData[i] = (byte) (i & 0x3F);
        }
        for (int i = 0; i < frameBData.length; i++) {
            frameBData[i] = (byte) ((i + 0x80) & 0xFF);
        }
        byte[] compA = compressBytes(frameAData);
        byte[] compB = compressBytes(frameBData);
        byte[] concatenated = new byte[compA.length + compB.length];
        System.arraycopy(compA, 0, concatenated, 0, compA.length);
        System.arraycopy(compB, 0, concatenated, compA.length, compB.length);

        byte[] decompressed = new byte[frameAData.length + frameBData.length];
        try (Zstd.DStream dstream = zstd.newDStream()) {
            long hint = dstream.decompress(decompressed, 0, decompressed.length, concatenated, 0, concatenated.length);
            // Two frames concatenated: depending on the libzstd build, the call may stop at the
            // boundary of the first frame (hint==0) and require a second decompress call for the
            // second frame, or return after consuming both. Loop until consumed.
            int srcPos = dstream.lastSrcPos();
            int dstPos = dstream.lastDstPos();
            while (srcPos < concatenated.length) {
                hint = dstream.decompress(decompressed, dstPos, decompressed.length, concatenated, srcPos, concatenated.length);
                srcPos = dstream.lastSrcPos();
                dstPos = dstream.lastDstPos();
            }
            assertThat("final frame should be fully decoded", hint, equalTo(0L));
            assertThat(dstPos, equalTo(decompressed.length));
        }
        // First frame's bytes appear first, then the second frame's.
        byte[] expected = new byte[decompressed.length];
        System.arraycopy(frameAData, 0, expected, 0, frameAData.length);
        System.arraycopy(frameBData, 0, expected, frameAData.length, frameBData.length);
        assertArrayEquals(expected, decompressed);
    }

    public void testDStreamCorruptedInputThrows() {
        // Feeding random bytes that look nothing like a zstd frame triggers ZSTD_error_prefix_unknown
        // from the very first byte — the most reliable corruption case for an SPI-level assertion.
        byte[] junk = new byte[256];
        for (int i = 0; i < junk.length; i++) {
            junk[i] = (byte) (0xAA ^ i);
        }
        byte[] decompressed = new byte[1024];
        try (Zstd.DStream dstream = zstd.newDStream()) {
            var ex = expectThrows(
                IllegalArgumentException.class,
                () -> dstream.decompress(decompressed, 0, decompressed.length, junk, 0, junk.length)
            );
            // The message comes from libzstd's ZSTD_getErrorName, surfaced verbatim by the facade.
            assertNotNull(ex.getMessage());
        }
    }

    public void testDStreamCloseIsIdempotent() {
        Zstd.DStream dstream = zstd.newDStream();
        dstream.close();
        // The second close must be a no-op — double-free would either crash the JVM (best case) or
        // silently corrupt unrelated native memory. Asserting the call returns is the only signal
        // we have at this level; the JVM's own checks handle the worst case.
        dstream.close();
    }

    public void testDStreamRejectsUseAfterClose() {
        Zstd.DStream dstream = zstd.newDStream();
        dstream.close();
        byte[] dst = new byte[1];
        byte[] src = new byte[1];
        expectThrows(IllegalStateException.class, () -> dstream.decompress(dst, 0, 1, src, 0, 1));
    }

    public void testDStreamRejectsOutOfBoundsArgs() {
        try (Zstd.DStream dstream = zstd.newDStream()) {
            byte[] dst = new byte[16];
            byte[] src = new byte[16];
            // Java-side bounds checks happen before any FFI call — keeps libzstd safe from us
            // passing it a struct pointing past the heap segment we just stamped in.
            expectThrows(IndexOutOfBoundsException.class, () -> dstream.decompress(dst, -1, 16, src, 0, 16));
            expectThrows(IndexOutOfBoundsException.class, () -> dstream.decompress(dst, 0, 17, src, 0, 16));
            expectThrows(IndexOutOfBoundsException.class, () -> dstream.decompress(dst, 0, 16, src, 0, 17));
            expectThrows(NullPointerException.class, () -> dstream.decompress(null, 0, 0, src, 0, 0));
            expectThrows(NullPointerException.class, () -> dstream.decompress(dst, 0, 0, null, 0, 0));
        }
    }

    // ---------- One-shot heap byte[] overloads (Phase 2) ----------
    // The block API critical(true) downcalls used by PanamaZstd.decompressHeap / compressHeap.
    // Coverage here pins the contract that JdkZstdLibrary's heap overloads accept heap segments
    // directly without an off-heap staging copy (the JDK-8318645 limitation that constrains the
    // streaming path does not apply — these are flat downcalls).

    public void testHeapRoundTripSmall() {
        doTestHeapRoundTrip("the quick brown fox jumps over the lazy dog".getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    public void testHeapRoundTripEmpty() {
        doTestHeapRoundTrip(new byte[0]);
    }

    public void testHeapRoundTripVariousSizes() {
        for (int size : new int[] { 1, 100, 1024, 64 * 1024, 1024 * 1024 }) {
            byte[] data = new byte[size];
            for (int i = 0; i < size; i++) {
                data[i] = (byte) ((i * 31) ^ (i >>> 8));
            }
            doTestHeapRoundTrip(data);
        }
    }

    public void testHeapDecompressRejectsCorruption() {
        byte[] junk = new byte[64];
        for (int i = 0; i < junk.length; i++) {
            junk[i] = (byte) randomInt(255);
        }
        byte[] dst = new byte[256];
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> zstd.decompress(dst, 0, dst.length, junk, 0, junk.length)
        );
        assertNotNull(e.getMessage());
    }

    public void testHeapDecompressRejectsOutOfBounds() {
        byte[] dst = new byte[16];
        byte[] src = new byte[16];
        // The facade validates ranges Java-side before calling into libzstd.
        expectThrows(IllegalArgumentException.class, () -> zstd.decompress(dst, -1, 16, src, 0, 16));
        expectThrows(IllegalArgumentException.class, () -> zstd.decompress(dst, 0, 17, src, 0, 16));
        expectThrows(IllegalArgumentException.class, () -> zstd.decompress(dst, 0, 16, src, 0, 17));
        expectThrows(NullPointerException.class, () -> zstd.decompress(null, 0, 0, src, 0, 0));
        expectThrows(NullPointerException.class, () -> zstd.decompress(dst, 0, 0, null, 0, 0));
    }

    public void testHeapCompressRejectsTooSmallDst() {
        byte[] src = new byte[1024];
        for (int i = 0; i < src.length; i++) {
            src[i] = (byte) i;
        }
        // Way below compressBound — libzstd returns the "Destination buffer is too small" error
        // which the facade translates to IllegalArgumentException.
        byte[] dst = new byte[2];
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> zstd.compress(dst, 0, dst.length, src, 0, src.length, 3)
        );
        assertNotNull(e.getMessage());
    }

    /**
     * Heap-overload round-trip via the Panama critical(true) downcall. Compresses through the
     * libzstd heap binding and decompresses back through the same binding — proving the two
     * heap method handles round-trip with the same library that powers the direct-buffer overload.
     */
    private void doTestHeapRoundTrip(byte[] data) {
        byte[] compressed = new byte[zstd.compressBound(data.length)];
        int compressedLen = zstd.compress(compressed, 0, compressed.length, data, 0, data.length, 3);
        assertThat("compress returned non-positive for non-empty input", compressedLen, Matchers.greaterThan(0));
        byte[] decompressed = new byte[data.length];
        int decompressedLen = zstd.decompress(decompressed, 0, decompressed.length, compressed, 0, compressedLen);
        assertThat(decompressedLen, equalTo(data.length));
        assertArrayEquals(data, decompressed);
    }

    /**
     * Round-trip via the streaming API mirroring the production wrapper's algorithm: feed the
     * compressed input in {@code dStreamInSize} chunks and let the lib refill the caller's
     * destination array up to its requested length (the SPI internally caps each call at
     * {@code dStreamOutSize}; the loop here handles multi-call drains).
     */
    private void doTestDStreamRoundTrip(byte[] data) {
        byte[] compressed = compressBytes(data);
        byte[] decompressed = new byte[data.length];
        int chunk = zstd.dStreamInSize();
        try (Zstd.DStream dstream = zstd.newDStream()) {
            int srcConsumed = 0;
            int dstPos = 0;
            long hint = 1L;
            while (srcConsumed < compressed.length || hint != 0L) {
                int srcChunkLen = Math.min(chunk, compressed.length - srcConsumed);
                hint = dstream.decompress(decompressed, dstPos, decompressed.length, compressed, srcConsumed, srcConsumed + srcChunkLen);
                srcConsumed = dstream.lastSrcPos();
                dstPos = dstream.lastDstPos();
                if (srcConsumed >= compressed.length && hint == 0L) {
                    break;
                }
            }
            assertThat("frame should be fully decoded", hint, equalTo(0L));
            assertThat(dstPos, equalTo(data.length));
        }
        assertArrayEquals(data, decompressed);
    }

    /**
     * Compress via the block API (the only path on this module's classpath) to get known-good zstd
     * bytes for streaming-side decoder tests.
     */
    private byte[] compressBytes(byte[] data) {
        try (
            var original = nativeAccess.newConfinedBuffer(data.length);
            var compressed = nativeAccess.newConfinedBuffer(zstd.compressBound(data.length))
        ) {
            if (data.length > 0) {
                original.buffer().put(0, data);
            }
            int compressedLength = zstd.compress(compressed, original, 3);
            byte[] out = new byte[compressedLength];
            for (int i = 0; i < compressedLength; i++) {
                out[i] = compressed.buffer().get(i);
            }
            return out;
        }
    }
}
