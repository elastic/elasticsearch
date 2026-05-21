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
}
