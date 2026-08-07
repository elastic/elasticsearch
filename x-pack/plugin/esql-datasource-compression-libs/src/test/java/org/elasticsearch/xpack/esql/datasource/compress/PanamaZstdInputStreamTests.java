/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.compress;

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;

import org.elasticsearch.nativeaccess.NativeAccess;
import org.elasticsearch.nativeaccess.Zstd;
import org.elasticsearch.test.ESTestCase;
import org.junit.BeforeClass;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Behavior tests for {@link PanamaZstdInputStream}. The decisive ones cross-check byte-for-byte
 * against {@link ZstdInputStream} (zstd-jni) so any regression introduced while porting away from
 * the established implementation surfaces as a test failure rather than mid-query.
 */
public class PanamaZstdInputStreamTests extends ESTestCase {

    private static Zstd zstd;

    @BeforeClass
    public static void resolveNative() {
        assumeTrue("native zstd binding required", PanamaZstd.instance().isAvailable());
        zstd = NativeAccess.instance().getZstd();
    }

    public void testRoundTripSmall() throws IOException {
        byte[] data = "the quick brown fox jumps over the lazy dog\n".getBytes(StandardCharsets.UTF_8);
        assertRoundTrip(data);
    }

    public void testRoundTripEmpty() throws IOException {
        // A minimal zstd frame still has a header — decompressing it should yield zero bytes.
        assertRoundTrip(new byte[0]);
    }

    public void testRoundTripVariousSizes() throws IOException {
        // Sizes chosen to straddle the libzstd staging-buffer boundaries (≈ 128 KB input / 132 KB
        // output) so we exercise both single-pass and multi-pass paths in the wrapper.
        for (int size : new int[] { 1, 100, 1024, 64 * 1024, 1024 * 1024, 4 * 1024 * 1024 }) {
            byte[] data = randomBytesForCompression(size);
            assertRoundTrip(data);
        }
    }

    /**
     * Wraps the same compressed input in both {@link ZstdInputStream} (zstd-jni) and
     * {@link PanamaZstdInputStream}; if either implementation regresses, the assertion catches it.
     */
    public void testByteForByteMatchWithZstdJni() throws IOException {
        byte[] data = randomBytesForCompression(2 * 1024 * 1024);
        byte[] compressed = compress(data);

        byte[] viaJni;
        try (InputStream s = new ZstdInputStream(new ByteArrayInputStream(compressed))) {
            viaJni = s.readAllBytes();
        }
        byte[] viaPanama;
        try (InputStream s = new PanamaZstdInputStream(new ByteArrayInputStream(compressed), zstd)) {
            viaPanama = s.readAllBytes();
        }
        assertArrayEquals(viaJni, viaPanama);
        assertArrayEquals(data, viaPanama);
    }

    public void testMultiFrameConcatenated() throws IOException {
        byte[] frameA = "first frame payload\n".getBytes(StandardCharsets.UTF_8);
        byte[] frameB = "second frame payload\n".getBytes(StandardCharsets.UTF_8);
        byte[] compA = compress(frameA);
        byte[] compB = compress(frameB);
        byte[] joined = new byte[compA.length + compB.length];
        System.arraycopy(compA, 0, joined, 0, compA.length);
        System.arraycopy(compB, 0, joined, compA.length, compB.length);

        try (InputStream s = new PanamaZstdInputStream(new ByteArrayInputStream(joined), zstd)) {
            byte[] all = s.readAllBytes();
            byte[] expected = new byte[frameA.length + frameB.length];
            System.arraycopy(frameA, 0, expected, 0, frameA.length);
            System.arraycopy(frameB, 0, expected, frameA.length, frameB.length);
            assertArrayEquals(expected, all);
        }
    }

    public void testSingleByteReadDrainsStream() throws IOException {
        byte[] data = randomBytesForCompression(8 * 1024);
        byte[] compressed = compress(data);
        ByteArrayOutputStream collected = new ByteArrayOutputStream(data.length);
        try (InputStream s = new PanamaZstdInputStream(new ByteArrayInputStream(compressed), zstd)) {
            int b;
            while ((b = s.read()) != -1) {
                collected.write(b);
            }
            assertEquals(-1, s.read());
        }
        assertArrayEquals(data, collected.toByteArray());
    }

    public void testReadAllBytesUsesAvailableSignal() throws IOException {
        // readAllBytes uses available() to size its read window. A regression in our available()
        // returning 0 too eagerly would manifest as a truncated result. Cover with a payload bigger
        // than the input staging buffer so available() must return 1 mid-stream.
        byte[] data = randomBytesForCompression(512 * 1024);
        byte[] compressed = compress(data);
        try (InputStream s = new PanamaZstdInputStream(new ByteArrayInputStream(compressed), zstd)) {
            byte[] result = s.readAllBytes();
            assertArrayEquals(data, result);
        }
    }

    /**
     * Slow / non-blocking upstream simulation — emits one byte per {@code read()} and reports
     * {@code available()=0}. This is the configuration where zstd-jni's
     * {@code in.available() > 0 || dstPos == offset} refill condition matters; a faithful port
     * must round-trip the data without hanging.
     */
    public void testDripFeedUpstreamWithZeroAvailable() throws IOException {
        byte[] data = randomBytesForCompression(32 * 1024);
        byte[] compressed = compress(data);
        InputStream slow = new InputStream() {
            int idx = 0;

            @Override
            public int read() {
                return idx < compressed.length ? compressed[idx++] & 0xff : -1;
            }

            @Override
            public int read(byte[] b, int off, int len) {
                if (idx >= compressed.length) {
                    return -1;
                }
                // Hand over exactly one byte per call — the wrapper must still make progress.
                b[off] = compressed[idx++];
                return 1;
            }

            @Override
            public int available() {
                return 0;
            }
        };
        try (InputStream s = new PanamaZstdInputStream(slow, zstd)) {
            assertArrayEquals(data, s.readAllBytes());
        }
    }

    public void testTruncatedInputThrows() throws IOException {
        byte[] data = randomBytesForCompression(64 * 1024);
        byte[] compressed = compress(data);
        // Drop the last 64 bytes — well past the header so we hit the truncation path inside the
        // decoder rather than a header-validation error.
        byte[] truncated = new byte[compressed.length - 64];
        System.arraycopy(compressed, 0, truncated, 0, truncated.length);

        try (InputStream s = new PanamaZstdInputStream(new ByteArrayInputStream(truncated), zstd)) {
            expectThrows(IOException.class, s::readAllBytes);
        }
    }

    public void testCorruptInputThrows() throws IOException {
        // A pile of random bytes definitely doesn't start with a valid zstd frame header — the
        // decoder should throw on the first decompress call.
        byte[] junk = randomByteArrayOfLength(1024);
        try (InputStream s = new PanamaZstdInputStream(new ByteArrayInputStream(junk), zstd)) {
            expectThrows(IOException.class, s::readAllBytes);
        }
    }

    public void testCloseIsIdempotent() throws IOException {
        byte[] compressed = compress("data".getBytes(StandardCharsets.UTF_8));
        InputStream s = new PanamaZstdInputStream(new ByteArrayInputStream(compressed), zstd);
        s.close();
        s.close();
    }

    public void testCloseClosesUpstream() throws IOException {
        byte[] compressed = compress("data".getBytes(StandardCharsets.UTF_8));
        CountingInputStream upstream = new CountingInputStream(new ByteArrayInputStream(compressed));
        try (InputStream s = new PanamaZstdInputStream(upstream, zstd)) {
            s.readAllBytes();
        }
        // The upstream gets closed exactly once: any leak would silently hang on to file/blob
        // descriptors in production, so cover it explicitly.
        assertEquals("upstream should be closed exactly once", 1, upstream.closeCount);
    }

    public void testReadAfterCloseThrows() throws IOException {
        byte[] compressed = compress("data".getBytes(StandardCharsets.UTF_8));
        InputStream s = new PanamaZstdInputStream(new ByteArrayInputStream(compressed), zstd);
        s.close();
        // ESQL formats wrap our stream with BufferedReader / Channels — those rely on a clear
        // "Stream closed" IOException to distinguish accidental reuse from EOF. Don't return -1
        // or hang; throw.
        expectThrows(IOException.class, s::readAllBytes);
        expectThrows(IOException.class, s::read);
        expectThrows(IOException.class, s::available);
    }

    public void testSkipAdvancesAndPreservesAlignment() throws IOException {
        byte[] data = new byte[8 * 1024];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) i;
        }
        byte[] compressed = compress(data);
        try (InputStream s = new PanamaZstdInputStream(new ByteArrayInputStream(compressed), zstd)) {
            long skipped = s.skip(1000);
            assertThat(skipped, greaterThanOrEqualTo(1L));
            // After skipping, the next read must return the byte logically at offset `skipped`.
            int b = s.read();
            assertEquals(data[(int) skipped] & 0xff, b);
        }
    }

    public void testAvailableSignalsEofAfterDrain() throws IOException {
        byte[] compressed = compress("abc".getBytes(StandardCharsets.UTF_8));
        try (InputStream s = new PanamaZstdInputStream(new ByteArrayInputStream(compressed), zstd)) {
            s.readAllBytes();
            // Drained: available() must defer to the upstream which is also drained (= 0).
            assertEquals(0, s.available());
        }
    }

    /**
     * Mid-stream {@code available()} must return {@code 1} as long as the decoder holds buffered
     * input/output the caller hasn't drained yet. Zstd-jni keeps the same contract — without it,
     * {@code BufferedReader} (and other consumers that probe {@code available()} between reads)
     * mistakes "we're still working" for EOF and hangs or short-reads.
     *
     * <p>Trigger: feed a multi-KB compressed payload, then ask for a single byte. The decoder
     * produces 1 byte into the caller's buffer, the rest stays in the off-heap output staging
     * buffer / libzstd's internal state, and the wrapper's {@code needRead} flag flips to
     * {@code false}. The {@code !needRead} branch of {@code available()} fires.
     */
    public void testAvailableSignalsOneWhileBuffered() throws IOException {
        byte[] data = randomBytesForCompression(16 * 1024);
        byte[] compressed = compress(data);
        try (InputStream s = new PanamaZstdInputStream(new ByteArrayInputStream(compressed), zstd)) {
            // Read a single byte to force the decoder to produce output without exhausting the
            // staged input chunk — this is exactly the condition the available() != needRead
            // branch was added to handle.
            int first = s.read();
            assertEquals(data[0] & 0xff, first);
            // The decoder still has bytes pending — available() must signal that as 1.
            assertEquals(1, s.available());
            // Reading the rest must complete the payload byte-for-byte.
            byte[] rest = s.readAllBytes();
            byte[] expected = new byte[data.length - 1];
            System.arraycopy(data, 1, expected, 0, expected.length);
            assertArrayEquals(expected, rest);
        }
    }

    public void testMarkSupportedFalse() throws IOException {
        byte[] compressed = compress(new byte[] { 1, 2, 3 });
        try (InputStream s = new PanamaZstdInputStream(new ByteArrayInputStream(compressed), zstd)) {
            assertFalse(s.markSupported());
        }
    }

    public void testIndexOutOfBoundsArguments() throws IOException {
        byte[] compressed = compress(new byte[] { 1, 2, 3 });
        try (InputStream s = new PanamaZstdInputStream(new ByteArrayInputStream(compressed), zstd)) {
            byte[] dst = new byte[8];
            expectThrows(IndexOutOfBoundsException.class, () -> s.read(dst, -1, 1));
            expectThrows(IndexOutOfBoundsException.class, () -> s.read(dst, 0, -1));
            expectThrows(IndexOutOfBoundsException.class, () -> s.read(dst, 1, 8));
            // Zero-length read short-circuits without touching the decoder.
            assertEquals(0, s.read(dst, 0, 0));
        }
    }

    /**
     * Regression for the WrongThreadException that surfaced in the CSV/NdJSON QA ITs: the codec
     * constructs the wrapper eagerly when the file is opened, but `StreamingParallelParsingCoordinator`
     * actually reads it from a segmentator worker thread. A confined arena binds to the constructing
     * thread and any MemorySegment.copy from a different thread throws WrongThreadException. This
     * test reproduces the exact construct-on-A-read-on-B shape and would have caught the bug in unit
     * tests, off the QA cluster.
     */
    public void testReadFromDifferentThreadThanConstructor() throws Exception {
        byte[] data = randomBytesForCompression(96 * 1024);
        byte[] compressed = compress(data);
        PanamaZstdInputStream s = new PanamaZstdInputStream(new ByteArrayInputStream(compressed), zstd);
        try {
            byte[][] decoded = new byte[1][];
            Throwable[] thrown = new Throwable[1];
            Thread t = new Thread(() -> {
                try {
                    decoded[0] = s.readAllBytes();
                } catch (Throwable err) {
                    thrown[0] = err;
                }
            }, "panama-zstd-cross-thread-test");
            t.start();
            t.join();
            if (thrown[0] != null) {
                throw new AssertionError("read on worker thread failed: " + thrown[0], thrown[0]);
            }
            assertArrayEquals(data, decoded[0]);
        } finally {
            s.close();
        }
    }

    // The constructor's "free DStream if byte[] allocation throws" invariant was previously covered
    // by a placebo test that called newDStream() + close() without ever exercising the failure path.
    // Deleted: a real check needs a poisoned `Zstd` whose `newDStream()` returns a counting
    // DStream double *and* a way to force `new byte[srcBuffSize]` to throw (the field is static
    // final, so this requires either reflection or a test-only constructor seam). The cleanup
    // invariant is two lines in PanamaZstdInputStream's constructor and is read on every change
    // to that constructor; investing test plumbing for that one OOM corner is not pulling its
    // weight. If the invariant ever becomes load-bearing (e.g. an allocation appears between
    // newDStream and the byte[] alloc), revisit.

    private static byte[] compress(byte[] data) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(data.length + 32);
        try (ZstdOutputStream out = new ZstdOutputStream(baos)) {
            out.write(data);
        }
        return baos.toByteArray();
    }

    /**
     * Random bytes biased toward repeated values so the compressor produces a non-trivial frame
     * structure (otherwise we'd get incompressible payloads where the streaming path is mostly
     * just memcpy and we wouldn't exercise the decoder's internal buffering).
     */
    private byte[] randomBytesForCompression(int size) {
        byte[] data = new byte[size];
        for (int i = 0; i < size; i++) {
            // Periodic pattern with mild randomness — produces ~10:1 compression on libzstd 1.5.7
            // at the default level. Plenty of internal state to exercise.
            data[i] = (byte) ((i * 31) ^ (randomInt() & 0x0F));
        }
        return data;
    }

    private void assertRoundTrip(byte[] data) throws IOException {
        byte[] compressed = compress(data);
        try (InputStream s = new PanamaZstdInputStream(new ByteArrayInputStream(compressed), zstd)) {
            assertArrayEquals(data, s.readAllBytes());
        }
    }

    /**
     * Counts {@link #close()} invocations so we can prove the wrapper propagates {@code close()}
     * to the upstream exactly once. {@link java.io.FilterInputStream}'s default {@code close} chains
     * by default, but the wrapper still owns ordering (free the native handle first, then close
     * upstream) — pinning that contract explicitly catches a future refactor that drops the chain.
     */
    private static final class CountingInputStream extends InputStream {
        private final InputStream delegate;
        int closeCount = 0;

        CountingInputStream(InputStream delegate) {
            this.delegate = delegate;
        }

        @Override
        public int read() throws IOException {
            return delegate.read();
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return delegate.read(b, off, len);
        }

        @Override
        public int available() throws IOException {
            return delegate.available();
        }

        @Override
        public void close() throws IOException {
            closeCount++;
            delegate.close();
        }
    }
}
