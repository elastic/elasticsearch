/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.zip.CRC32;
import java.util.zip.Deflater;
import java.util.zip.GZIPOutputStream;
import java.util.zip.Inflater;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Fast-path gzip inflate must stay byte-identical to {@link java.util.zip.GZIPInputStream} on the page corpus:
 * empty, header flags, corrupt FHCRC, multi-member, bad CRC, bad ISIZE, truncation, trailing junk, destLen
 * too small or too large. Extra uncompressed bytes keep today's page-header error; trailing
 * junk after a complete member is ignored like {@link java.util.zip.GZIPInputStream}, not rejected like the
 * text-codec decoder. The one exception is a header-only suffix after a complete member: JDK 21 and JDK 25
 * disagree there, so the fast path is pinned to end of stream without an oracle.
 */
public class GzipPageInflaterTests extends ESTestCase {

    public void testInputStreamBufferSizeIsMin64kAndCompressedSize() {
        assertEquals(1, GzipPageInflater.inputStreamBufferSize(0));
        assertEquals(1, GzipPageInflater.inputStreamBufferSize(-1));
        assertEquals(1, GzipPageInflater.inputStreamBufferSize(1));
        assertEquals(512, GzipPageInflater.inputStreamBufferSize(512));
        assertEquals(64 * 1024, GzipPageInflater.inputStreamBufferSize(64 * 1024));
        assertEquals(64 * 1024, GzipPageInflater.inputStreamBufferSize(64 * 1024 + 1));
        assertEquals(64 * 1024, GzipPageInflater.inputStreamBufferSize(Integer.MAX_VALUE));
    }

    public void testEmptyPayload() throws IOException {
        assertFastMatchesInputStream(gzip(new byte[0]), 0);
    }

    public void testRoundTripRandomPayload() throws IOException {
        byte[] data = randomByteArrayOfLength(between(1, 16_384));
        assertFastMatchesInputStream(gzip(data), data.length);
    }

    public void testHeaderFlags() throws IOException {
        byte[] data = compressibleBytes(between(1, 4096));
        boolean extra = randomBoolean();
        boolean name = randomBoolean();
        boolean comment = randomBoolean();
        boolean headerCrc = randomBoolean();
        if (extra == false && name == false && comment == false && headerCrc == false) {
            headerCrc = true;
        }
        byte[] gz = gzipWithHeaderFields(data, extra, name, comment, headerCrc);
        assertFastMatchesInputStream(gz, data.length);
    }

    public void testMultiMemberConcatenates() throws IOException {
        byte[] first = randomByteArrayOfLength(between(0, 2048));
        byte[] second = randomByteArrayOfLength(between(1, 2048));
        byte[] gz = concat(gzip(first), gzip(second));
        assertFastMatchesInputStream(gz, first.length + second.length);
    }

    public void testCorruptHeaderCrc() throws IOException {
        byte[] data = compressibleBytes(between(1, 2048));
        byte[] gz = gzipWithHeaderFields(data, false, false, false, true);
        gz[10] ^= 0x55; // first FHCRC byte after the 10-byte fixed header
        assertFastMatchesInputStreamFailure(gz, data.length, "Corrupt GZIP header");
    }

    public void testBadCrc() throws IOException {
        byte[] data = compressibleBytes(between(1, 2048));
        byte[] gz = gzip(data);
        gz[gz.length - 8] ^= 1;
        assertFastMatchesInputStreamFailure(gz, data.length, "Corrupt GZIP trailer");
    }

    public void testBadIsize() throws IOException {
        byte[] data = compressibleBytes(between(1, 2048));
        byte[] gz = gzip(data);
        gz[gz.length - 1] ^= 1;
        assertFastMatchesInputStreamFailure(gz, data.length, "Corrupt GZIP trailer");
    }

    public void testMultiMemberTruncationFails() throws IOException {
        byte[] first = randomByteArrayOfLength(between(0, 2048));
        byte[] second = randomByteArrayOfLength(between(1, 2048));
        byte[] firstGz = gzip(first);
        byte[] gz = concat(firstGz, gzip(second));
        int destLen = first.length + second.length;
        assertFastMatchesInputStream(gz, destLen);
        int cut = between(firstGz.length + 1, gz.length - 1);
        assertFastMatchesInputStreamFailure(Arrays.copyOf(gz, cut), destLen);
    }

    /**
     * Member 2 produces every dest byte but never sees BFINAL. Old {@code completedMember}
     * treated that extra-probe {@code needsInput} as EOS; GZIPInputStream rejects it.
     */
    public void testUnfinishedSecondMemberAfterDestFilledFails() throws IOException {
        byte[] first = compressibleBytes(between(8, 64));
        byte[] second = compressibleBytes(between(32, 256));
        byte[] gz = concat(gzip(first), gzipUnfinishedMember(second));
        assertFastMatchesInputStreamFailure(gz, first.length + second.length);
    }

    public void testTruncation() throws IOException {
        byte[] data = compressibleBytes(between(64, 4096));
        byte[] gz = gzip(data);
        assertFastMatchesInputStream(gz, data.length);
        int cut = between(1, gz.length - 1);
        assertFastMatchesInputStreamFailure(Arrays.copyOf(gz, cut), data.length);
    }

    public void testHeaderOnlySuffixAfterCompleteMemberIsEos() throws IOException {
        byte[] data = compressibleBytes(between(1, 256));
        byte[] headerOnly = new byte[] { 0x1f, (byte) 0x8b, 8, 0, 0, 0, 0, 0, 0, (byte) 0xff };
        byte[] gz = concat(gzip(data), headerOnly);
        // GZIPInputStream is not a valid oracle here: JDK 21 treats a header-only suffix as end of stream, while JDK 25
        // always starts the next member and fails with EOFException. The fast path defines it as end of stream.
        assertArrayEquals(data, inflateViaFastPath(gz, data.length));

        byte[] oversized = new byte[data.length + between(1, 32)];
        Arrays.fill(oversized, (byte) 0x5A);
        Inflater inflater = new Inflater(true);
        try {
            GzipPageInflater.inflate(inflater, BytesInput.from(gz), oversized, data.length);
        } finally {
            inflater.end();
        }
        assertArrayEquals(data, Arrays.copyOf(oversized, data.length));
        for (int i = data.length; i < oversized.length; i++) {
            assertEquals((byte) 0x5A, oversized[i]);
        }
    }

    public void testTrailingJunkIgnoredWhenDestLenMatches() throws IOException {
        byte[] data = compressibleBytes(between(1, 2048));
        byte[] junk = randomByteArrayOfLength(between(1, 64));
        int firstJunk = randomValueOtherThan(0x1f, () -> between(0, 255));
        junk[0] = (byte) firstJunk;
        byte[] gz = concat(gzip(data), junk);
        assertFastMatchesInputStream(gz, data.length);
    }

    public void testTrailingZeroPaddingIgnoredWhenDestLenMatches() throws IOException {
        byte[] data = compressibleBytes(between(1, 2048));
        byte[] gz = concat(gzip(data), new byte[between(1, 32)]);
        assertFastMatchesInputStream(gz, data.length);
    }

    public void testDestLenTooSmall() throws IOException {
        byte[] data = compressibleBytes(between(32, 2048));
        byte[] gz = gzip(data);
        int destLen = between(1, data.length - 1);
        assertFastMatchesInputStreamFailure(gz, destLen, extraBytesMessage(destLen));
        assertExtraBytesMessage(gz, destLen);
    }

    public void testDestLenTooLarge() throws IOException {
        byte[] data = compressibleBytes(between(8, 2048));
        byte[] gz = gzip(data);
        assertFastMatchesInputStreamFailure(gz, data.length + between(1, 64), "Premature end of GZIP stream");
    }

    public void testDestLenZeroRejectsNonEmpty() throws IOException {
        byte[] data = compressibleBytes(between(1, 64));
        assertExtraBytesMessage(gzip(data), 0);
    }

    public void testInflaterReuseAcrossPages() throws IOException {
        Inflater inflater = new Inflater(true);
        try {
            for (int i = 0; i < 8; i++) {
                byte[] data = randomByteArrayOfLength(between(0, 4096));
                byte[] dest = new byte[Math.max(1, data.length)];
                GzipPageInflater.inflate(inflater, BytesInput.from(gzip(data)), dest, data.length);
                assertArrayEquals(data, Arrays.copyOf(dest, data.length));
            }
        } finally {
            inflater.end();
        }
    }

    public void testDirectBufferFallsBackToGzipInputStream() throws IOException {
        byte[] data = randomByteArrayOfLength(between(16, 2048));
        byte[] gz = gzip(data);
        ByteBuffer direct = ByteBuffer.allocateDirect(gz.length);
        direct.put(gz).flip();
        byte[] dest = new byte[data.length];
        Inflater inflater = new Inflater(true);
        try {
            GzipPageInflater.inflate(inflater, BytesInput.from(direct), dest, data.length);
            assertArrayEquals(data, dest);
            // Inflater still reusable after the fallback path.
            byte[] dest2 = new byte[data.length];
            GzipPageInflater.inflate(inflater, BytesInput.from(gz), dest2, data.length);
            assertArrayEquals(data, dest2);
        } finally {
            inflater.end();
        }
    }

    public void testHeaderOnlyDestLenZeroFails() {
        byte[] headerOnly = new byte[] { 0x1f, (byte) 0x8b, 8, 0, 0, 0, 0, 0, 0, (byte) 0xff };
        assertFastMatchesInputStreamFailure(headerOnly, 0);
    }

    public void testEmptyCompressedInputFails() {
        assertFastMatchesInputStreamFailure(new byte[0], 0);
    }

    public void testGzipDecompressorStaysSingleton() throws IOException {
        PlainCompressionCodecFactory factory = new PlainCompressionCodecFactory();
        try {
            var first = factory.getDecompressor(CompressionCodecName.GZIP);
            byte[] data = randomByteArrayOfLength(between(16, 256));
            first.decompress(BytesInput.from(gzip(data)), data.length);
            assertSame(first, factory.getDecompressor(CompressionCodecName.GZIP));
        } finally {
            factory.release();
        }
    }

    private void assertExtraBytesMessage(byte[] gz, int destLen) {
        byte[] dest = new byte[destLen + 8];
        Arrays.fill(dest, (byte) 0x5A);
        Inflater inflater = new Inflater(true);
        try {
            IOException e = expectThrows(IOException.class, () -> GzipPageInflater.inflate(inflater, BytesInput.from(gz), dest, destLen));
            assertThat(e.getMessage(), equalTo(extraBytesMessage(destLen)));
            for (int i = destLen; i < dest.length; i++) {
                assertEquals((byte) 0x5A, dest[i]);
            }
        } finally {
            inflater.end();
        }
    }

    private void assertFastMatchesInputStream(byte[] gz, int destLen) throws IOException {
        byte[] viaStream = inflateViaGzipInputStream(gz, destLen);
        byte[] viaFast = inflateViaFastPath(gz, destLen);
        assertArrayEquals(viaStream, viaFast);
        if (destLen > 0) {
            byte[] oversized = new byte[destLen + between(1, 32)];
            Arrays.fill(oversized, (byte) 0x5A);
            Inflater inflater = new Inflater(true);
            try {
                GzipPageInflater.inflate(inflater, BytesInput.from(gz), oversized, destLen);
            } finally {
                inflater.end();
            }
            assertArrayEquals(viaStream, Arrays.copyOf(oversized, destLen));
            for (int i = destLen; i < oversized.length; i++) {
                assertEquals((byte) 0x5A, oversized[i]);
            }
        }
    }

    private void assertFastMatchesInputStreamFailure(byte[] gz, int destLen) {
        assertFastMatchesInputStreamFailure(gz, destLen, null);
    }

    private void assertFastMatchesInputStreamFailure(byte[] gz, int destLen, String messageSubstring) {
        IOException streamEx = expectThrows(IOException.class, () -> inflateViaGzipInputStream(gz, destLen));
        IOException fastEx = expectThrows(IOException.class, () -> inflateViaFastPath(gz, destLen));
        if (messageSubstring != null) {
            assertThat("GZIPInputStream path: " + streamEx, streamEx.getMessage(), containsString(messageSubstring));
            assertThat("fast path: " + fastEx, fastEx.getMessage(), containsString(messageSubstring));
        }
    }

    private static String extraBytesMessage(int destLen) {
        return "GZIP stream produced more than " + destLen + " uncompressed bytes declared by the page header";
    }

    private static byte[] inflateViaFastPath(byte[] gz, int destLen) throws IOException {
        byte[] dest = destLen == 0 ? new byte[0] : new byte[destLen];
        Inflater inflater = new Inflater(true);
        try {
            GzipPageInflater.inflate(inflater, BytesInput.from(gz), dest, destLen);
            return dest;
        } finally {
            inflater.end();
        }
    }

    private static byte[] inflateViaGzipInputStream(byte[] gz, int destLen) throws IOException {
        byte[] dest = destLen == 0 ? new byte[0] : new byte[destLen];
        GzipPageInflater.decompressWithInputStream(BytesInput.from(gz), dest, destLen);
        return dest;
    }

    /** Gzip header plus nowrap DEFLATE of {@code data} with SYNC_FLUSH; no BFINAL, no trailer. */
    private static byte[] gzipUnfinishedMember(byte[] data) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes(new byte[] { 0x1f, (byte) 0x8b, 8, 0, 0, 0, 0, 0, 0, (byte) 0xff });
        Deflater deflater = new Deflater(Deflater.DEFAULT_COMPRESSION, true);
        try {
            deflater.setInput(data);
            byte[] chunk = new byte[Math.max(64, data.length * 2 + 32)];
            while (deflater.needsInput() == false) {
                int n = deflater.deflate(chunk, 0, chunk.length, Deflater.SYNC_FLUSH);
                out.write(chunk, 0, n);
            }
            int n = deflater.deflate(chunk, 0, chunk.length, Deflater.SYNC_FLUSH);
            out.write(chunk, 0, n);
        } finally {
            deflater.end();
        }
        return out.toByteArray();
    }

    private static byte[] gzip(byte[] data) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (GZIPOutputStream gos = new GZIPOutputStream(baos)) {
            gos.write(data);
        }
        return baos.toByteArray();
    }

    private static byte[] concat(byte[]... parts) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] part : parts) {
            out.writeBytes(part);
        }
        return out.toByteArray();
    }

    private static byte[] compressibleBytes(int length) {
        byte[] b = new byte[length];
        for (int i = 0; i < length; i++) {
            b[i] = (byte) ('a' + (i * 31 + i / 7) % 23);
        }
        return b;
    }

    /** Builds a gzip member by hand, with the optional header fields of RFC 1952 section 2.3. */
    private byte[] gzipWithHeaderFields(byte[] data, boolean extra, boolean name, boolean comment, boolean headerCrc) throws IOException {
        ByteArrayOutputStream header = new ByteArrayOutputStream();
        int flags = (extra ? 4 : 0) | (name ? 8 : 0) | (comment ? 16 : 0) | (headerCrc ? 2 : 0);
        header.writeBytes(new byte[] { 0x1f, (byte) 0x8b, 8, (byte) flags, 1, 2, 3, 4, 0, (byte) 0xff });
        if (extra) {
            byte[] field = randomByteArrayOfLength(between(0, 300));
            header.write(field.length & 0xff);
            header.write(field.length >> 8);
            header.writeBytes(field);
        }
        if (name) {
            header.writeBytes("col.parquet".getBytes(StandardCharsets.UTF_8));
            header.write(0);
        }
        if (comment) {
            header.writeBytes("a comment".getBytes(StandardCharsets.UTF_8));
            header.write(0);
        }
        if (headerCrc) {
            CRC32 crc = new CRC32();
            crc.update(header.toByteArray());
            int v = (int) crc.getValue() & 0xffff;
            header.write(v & 0xff);
            header.write(v >> 8);
        }
        Deflater deflater = new Deflater(Deflater.DEFAULT_COMPRESSION, true);
        try {
            deflater.setInput(data);
            deflater.finish();
            byte[] chunk = new byte[8192];
            while (deflater.finished() == false) {
                int n = deflater.deflate(chunk);
                header.write(chunk, 0, n);
            }
        } finally {
            deflater.end();
        }
        CRC32 crc = new CRC32();
        crc.update(data);
        for (long v : new long[] { crc.getValue(), data.length }) {
            for (int i = 0; i < 4; i++) {
                header.write((int) (v >> (8 * i)) & 0xff);
            }
        }
        return header.toByteArray();
    }
}
