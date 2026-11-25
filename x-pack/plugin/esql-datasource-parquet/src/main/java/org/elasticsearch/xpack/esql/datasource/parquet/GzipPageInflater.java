/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.bytes.BytesInput;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.zip.CRC32;
import java.util.zip.DataFormatException;
import java.util.zip.GZIPInputStream;
import java.util.zip.Inflater;
import java.util.zip.ZipException;

/**
 * Gzip inflate for Parquet pages. The mutable {@link Inflater} lives on
 * {@link PrefetchedPageReader} so the factory's gzip decompressor can stay a singleton.
 *
 * <p>The fast path parses the RFC 1952 header (magic, CM=8, FEXTRA/FNAME/FCOMMENT/FHCRC),
 * inflates with the caller-owned {@code nowrap} inflater, and checks CRC32/ISIZE. Extra
 * uncompressed bytes use the same error as today's {@link GZIPInputStream} copy:
 * {@code GZIP stream produced more than … declared by the page header}. Trailing junk after
 * a complete member is ignored, matching {@link GZIPInputStream} — not the text-codec
 * trailing-zero policy.
 *
 * <p>Inputs that are not array-backed, and the shared {@code decompress(BytesInput, int)}
 * path, use {@link GZIPInputStream} with a raw buffer of {@code min(64KiB, compressedSize)}.
 * Prefetched dictionary pages use {@link #inflate} with the reader-owned inflater.
 */
final class GzipPageInflater {

    private static final int MAGIC_1 = 0x1f;
    private static final int MAGIC_2 = 0x8b;
    private static final int METHOD_DEFLATE = 8;
    private static final int FHCRC = 2;
    private static final int FEXTRA = 4;
    private static final int FNAME = 8;
    private static final int FCOMMENT = 16;
    private static final int TRAILER_BYTES = 8;
    private static final int MAX_INPUT_STREAM_BUFFER = 64 * 1024;

    private GzipPageInflater() {}

    /**
     * Inflate a gzip-compressed Parquet page into {@code dest[0..destLen)} using {@code inflater}.
     * {@code inflater} must have been constructed with {@code nowrap=true}
     * ({@code new Inflater(true)}). Reset on entry; the caller owns {@link Inflater#end()}.
     */
    static void inflate(Inflater inflater, BytesInput compressed, byte[] dest, int destLen) throws IOException {
        requireDestCapacity(dest, destLen);
        ByteBuffer input = compressed.toByteBuffer();
        if (input.hasArray()) {
            inflateArray(inflater, input.array(), input.arrayOffset() + input.position(), input.remaining(), dest, destLen);
            return;
        }
        decompressWithInputStream(compressed, dest, destLen);
    }

    /**
     * {@link GZIPInputStream} copy used by {@code decompress(BytesInput, int)} and the non-array
     * fallback. Raw buffer is {@code min(64KiB, compressedSize)} instead of the JDK default of
     * 512 bytes.
     */
    static void decompressWithInputStream(BytesInput compressed, byte[] dest, int destLen) throws IOException {
        requireDestCapacity(dest, destLen);
        int bufferSize = inputStreamBufferSize(compressed.size());
        try (GZIPInputStream gis = new GZIPInputStream(compressed.toInputStream(), bufferSize)) {
            int off = 0;
            while (off < destLen) {
                int read = gis.read(dest, off, destLen - off);
                if (read < 0) {
                    throw prematureEnd(destLen, off);
                }
                off += read;
            }
            if (gis.read() >= 0) {
                throw extraUncompressed(destLen);
            }
        }
    }

    static int inputStreamBufferSize(long compressedSize) {
        if (compressedSize <= 0L) {
            return 1;
        }
        return (int) Math.min(MAX_INPUT_STREAM_BUFFER, compressedSize);
    }

    private static void inflateArray(Inflater inflater, byte[] src, int off, int len, byte[] dest, int destLen) throws IOException {
        Objects.checkFromIndexSize(off, len, src.length);
        inflater.reset();
        int end = off + len;
        int inPos = off;
        int written = 0;
        boolean firstMember = true;
        boolean completedMember = false;
        CRC32 crc = new CRC32();
        byte[] probe = null;
        while (true) {
            int deflatePos;
            try {
                deflatePos = parseGzipHeader(src, inPos, end - inPos);
            } catch (IOException headerEx) {
                if (firstMember == false && written == destLen) {
                    // Same as GZIPInputStream.readTrailer: a malformed next header is EOS.
                    return;
                }
                if (firstMember == false) {
                    throw prematureEnd(destLen, written);
                }
                throw headerEx;
            }
            firstMember = false;
            inflater.reset();
            crc.reset();
            inflater.setInput(src, deflatePos, end - deflatePos);
            while (written < destLen && inflater.finished() == false) {
                int n;
                try {
                    n = inflater.inflate(dest, written, destLen - written);
                } catch (DataFormatException e) {
                    throw zip(e);
                }
                if (n > 0) {
                    crc.update(dest, written, n);
                    written += n;
                } else if (inflater.needsDictionary()) {
                    throw new ZipException("Preset dictionaries are not supported in gzip data");
                } else if (inflater.needsInput()) {
                    throw prematureEnd(destLen, written);
                } else if (inflater.finished() == false) {
                    throw new IOException("GZIP inflater stalled");
                }
            }
            if (written == destLen && inflater.finished() == false) {
                if (probe == null) {
                    probe = new byte[1];
                }
                int n;
                try {
                    n = inflater.inflate(probe, 0, 1);
                } catch (DataFormatException e) {
                    throw zip(e);
                }
                if (n > 0) {
                    throw extraUncompressed(destLen);
                }
                if (inflater.finished() == false) {
                    if (inflater.needsInput()) {
                        if (completedMember) {
                            // dest already filled; truncated next member is EOS.
                            return;
                        }
                        throw prematureEnd(destLen, written);
                    }
                    throw new IOException("GZIP inflater stalled");
                }
            }
            if (inflater.finished() == false) {
                throw prematureEnd(destLen, written);
            }
            int remaining = inflater.getRemaining();
            if (remaining < TRAILER_BYTES) {
                throw new EOFException("Unexpected end of ZLIB input stream");
            }
            int trailerPos = end - remaining;
            long trailerCrc = readUInt32LE(src, trailerPos);
            long trailerSize = readUInt32LE(src, trailerPos + 4);
            if (trailerCrc != crc.getValue() || trailerSize != (inflater.getBytesWritten() & 0xffffffffL)) {
                throw new ZipException("Corrupt GZIP trailer");
            }
            inPos = trailerPos + TRAILER_BYTES;
            completedMember = true;
            if (written < destLen) {
                if (inPos >= end) {
                    throw prematureEnd(destLen, written);
                }
                continue;
            }
            if (inPos >= end) {
                return;
            }
        }
    }

    /**
     * Parses a gzip member header. Copied in spirit from
     * {@code MultiMemberGzipInputStream.readHeader} (magic, CM=8, FEXTRA/FNAME/FCOMMENT/FHCRC)
     * but reserved flag bits are tolerated so the result matches {@link GZIPInputStream}.
     *
     * @return index of the first DEFLATE byte
     */
    private static int parseGzipHeader(byte[] src, int off, int len) throws IOException {
        Objects.checkFromIndexSize(off, len, src.length);
        int end = off + len;
        int i = off;
        requireRemaining(end, i, 2);
        if ((src[i++] & 0xff) != MAGIC_1 || (src[i++] & 0xff) != MAGIC_2) {
            throw new ZipException("Not in GZIP format");
        }
        requireRemaining(end, i, 1);
        if ((src[i++] & 0xff) != METHOD_DEFLATE) {
            throw new ZipException("Unsupported compression method");
        }
        requireRemaining(end, i, 1);
        int flags = src[i++] & 0xff;
        requireRemaining(end, i, 6);
        i += 6; // MTIME (4), XFL (1), OS (1)
        if ((flags & FEXTRA) != 0) {
            requireRemaining(end, i, 2);
            int extraLength = (src[i] & 0xff) | ((src[i + 1] & 0xff) << 8);
            i += 2;
            requireRemaining(end, i, extraLength);
            i += extraLength;
        }
        if ((flags & FNAME) != 0) {
            i = skipZeroTerminated(src, i, end);
        }
        if ((flags & FCOMMENT) != 0) {
            i = skipZeroTerminated(src, i, end);
        }
        if ((flags & FHCRC) != 0) {
            CRC32 headerCrc = new CRC32();
            headerCrc.update(src, off, i - off);
            int expected = (int) headerCrc.getValue() & 0xffff;
            requireRemaining(end, i, 2);
            int actual = (src[i] & 0xff) | ((src[i + 1] & 0xff) << 8);
            if (actual != expected) {
                throw new ZipException("Corrupt GZIP header");
            }
            i += 2;
        }
        return i;
    }

    /** Bounds check via remaining length so {@code i + n} cannot overflow near {@code Integer.MAX_VALUE}. */
    private static void requireRemaining(int end, int i, int n) throws EOFException {
        if (n < 0 || end - i < n) {
            throw new EOFException("Unexpected end of ZLIB input stream");
        }
    }

    private static int skipZeroTerminated(byte[] src, int i, int end) throws IOException {
        while (i < end) {
            if (src[i++] == 0) {
                return i;
            }
        }
        throw new EOFException("Unexpected end of ZLIB input stream");
    }

    private static long readUInt32LE(byte[] src, int off) {
        return (src[off] & 0xffL) | ((src[off + 1] & 0xffL) << 8) | ((src[off + 2] & 0xffL) << 16) | ((src[off + 3] & 0xffL) << 24);
    }

    private static ZipException zip(DataFormatException e) {
        String message = e.getMessage();
        ZipException zip = new ZipException(message != null ? message : "Invalid ZLIB data format");
        zip.initCause(e);
        return zip;
    }

    private static void requireDestCapacity(byte[] dest, int destLen) {
        if (destLen < 0 || destLen > dest.length) {
            throw new IllegalArgumentException(
                "decompress destLen [" + destLen + "] is out of range for dest.length [" + dest.length + "]"
            );
        }
    }

    private static IOException prematureEnd(int destLen, int got) {
        return new IOException("Premature end of GZIP stream: expected " + destLen + " bytes, got " + got);
    }

    private static IOException extraUncompressed(int destLen) {
        return new IOException("GZIP stream produced more than " + destLen + " uncompressed bytes declared by the page header");
    }
}
