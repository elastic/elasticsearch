/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.gzip;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.zip.CRC32;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;
import java.util.zip.ZipException;

/**
 * Gzip decoder that reads every member of a (possibly multi-member) gzip stream and fails on trailing data.
 *
 * <p>The JDK's {@link java.util.zip.GZIPInputStream} decides whether another member follows a trailer by calling
 * {@code in.available()}, which means "readable without blocking", not "bytes left". On a network stream that is
 * routinely 0 mid-body, so the JDK silently stops after any member (elastic/esql-planning#2121). It also treats a
 * malformed next header as the end of the stream and consumes those bytes from its private buffer, so trailing garbage
 * can never be reported. This class never calls {@code available()}: after each trailer it reads the next byte, which
 * is either end-of-stream, the start of another member, zero padding to the end of the stream, or an error.
 *
 * <p>Inflation uses the same {@link Inflater} and raw read buffer size as the JDK class, so throughput is unchanged.
 */
final class MultiMemberGzipInputStream extends InputStream {

    private static final int MAGIC_1 = 0x1f;
    private static final int MAGIC_2 = 0x8b;
    private static final int METHOD_DEFLATE = 8;

    private static final int FHCRC = 2;
    private static final int FEXTRA = 4;
    private static final int FNAME = 8;
    private static final int FCOMMENT = 16;
    private static final int RESERVED_FLAGS = 0xE0;

    private static final int TRAILER_BYTES = 8;

    private final InputStream in;
    private final Inflater inflater = new Inflater(true);
    private final CRC32 dataCrc = new CRC32();
    private final CRC32 headerCrc = new CRC32();
    private final byte[] buf;
    /** Next unread byte of {@link #buf}. While the inflater owns the buffer's content it equals {@link #limit}. */
    private int pos;
    private int limit;
    /** Compressed bytes read from {@link #in} before the current fill of {@link #buf}, for error messages. */
    private long consumedBeforeBuf;
    private boolean eof;
    private boolean closed;

    MultiMemberGzipInputStream(InputStream in, int bufferSize) throws IOException {
        if (bufferSize <= 0) {
            throw new IllegalArgumentException("buffer size must be positive: " + bufferSize);
        }
        this.in = Objects.requireNonNull(in);
        this.buf = new byte[bufferSize];
        try {
            int first = readByte();
            if (first == -1) {
                throw new EOFException();
            }
            readHeader(first);
        } catch (IOException | RuntimeException e) {
            inflater.end();
            throw e;
        }
    }

    @Override
    public int read() throws IOException {
        byte[] one = new byte[1];
        return read(one, 0, 1) == -1 ? -1 : one[0] & 0xff;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        Objects.checkFromIndexSize(off, len, b.length);
        ensureOpen();
        if (len == 0) {
            return 0;
        }
        while (eof == false) {
            // Must be checked before feeding: a finished inflater with an empty input would otherwise swallow the trailer.
            if (inflater.finished()) {
                finishMember();
                continue;
            }
            if (inflater.needsInput()) {
                if (pos == limit && fill() == false) {
                    throw new EOFException("Unexpected end of ZLIB input stream");
                }
                inflater.setInput(buf, pos, limit - pos);
                pos = limit;
            }
            int n;
            try {
                n = inflater.inflate(b, off, len);
            } catch (DataFormatException e) {
                throw new ZipException(e.getMessage() != null ? e.getMessage() : "Invalid ZLIB data format");
            }
            if (n > 0) {
                dataCrc.update(b, off, n);
                return n;
            }
            if (inflater.needsDictionary()) {
                throw new ZipException("Preset dictionaries are not supported in gzip data");
            }
        }
        return -1;
    }

    @Override
    public int available() throws IOException {
        ensureOpen();
        return eof ? 0 : 1;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        inflater.end();
        in.close();
    }

    private void ensureOpen() throws IOException {
        if (closed) {
            throw new IOException("Stream closed");
        }
    }

    /** Reads the trailer of the member the inflater just finished, then positions on the next member or at the end. */
    private void finishMember() throws IOException {
        // The inflater was handed everything up to limit; give back what it did not consume.
        pos = limit - inflater.getRemaining();
        long crc = readTrailerInt();
        long size = readTrailerInt();
        if (crc != dataCrc.getValue() || size != (inflater.getBytesWritten() & 0xffffffffL)) {
            throw new ZipException("Corrupt GZIP trailer");
        }
        int next = readByte();
        if (next == -1) {
            eof = true;
        } else if (next == MAGIC_1) {
            readHeader(next);
            inflater.reset();
            dataCrc.reset();
        } else if (next == 0) {
            skipZeroPadding();
            eof = true;
        } else {
            throw new ZipException("Trailing garbage after gzip member at compressed offset " + (offset() - 1));
        }
    }

    /** Zero bytes after the last member are tolerated; anything else is not a gzip member. */
    private void skipZeroPadding() throws IOException {
        int b;
        while ((b = readByte()) != -1) {
            if (b != 0) {
                throw new ZipException("Trailing garbage after gzip member at compressed offset " + (offset() - 1));
            }
        }
    }

    private long readTrailerInt() throws IOException {
        long v = 0;
        for (int i = 0; i < TRAILER_BYTES / 2; i++) {
            v |= (long) readRequired() << (8 * i);
        }
        return v;
    }

    /** Parses a member header whose first byte, already known to be {@code 0x1f}, has been consumed. */
    private void readHeader(int first) throws IOException {
        headerCrc.reset();
        headerCrc.update(first);
        if (first != MAGIC_1 || readHeaderByte() != MAGIC_2) {
            throw new ZipException("Not in GZIP format");
        }
        if (readHeaderByte() != METHOD_DEFLATE) {
            throw new ZipException("Unsupported compression method");
        }
        int flags = readHeaderByte();
        if ((flags & RESERVED_FLAGS) != 0) {
            throw new ZipException("Unsupported GZIP header flags: 0x" + Integer.toHexString(flags));
        }
        // MTIME (4), XFL (1), OS (1)
        for (int i = 0; i < 6; i++) {
            readHeaderByte();
        }
        if ((flags & FEXTRA) != 0) {
            int extraLength = readHeaderByte() | (readHeaderByte() << 8);
            for (int i = 0; i < extraLength; i++) {
                readHeaderByte();
            }
        }
        if ((flags & FNAME) != 0) {
            skipZeroTerminated();
        }
        if ((flags & FCOMMENT) != 0) {
            skipZeroTerminated();
        }
        if ((flags & FHCRC) != 0) {
            int expected = (int) headerCrc.getValue() & 0xffff;
            int actual = readRequired() | (readRequired() << 8);
            if (actual != expected) {
                throw new ZipException("Corrupt GZIP header");
            }
        }
    }

    private void skipZeroTerminated() throws IOException {
        while (readHeaderByte() != 0) {
            // skipping
        }
    }

    private int readHeaderByte() throws IOException {
        int b = readRequired();
        headerCrc.update(b);
        return b;
    }

    private int readRequired() throws IOException {
        int b = readByte();
        if (b == -1) {
            throw new EOFException("Unexpected end of GZIP data at compressed offset " + offset());
        }
        return b;
    }

    /** Next compressed byte, or -1 at the end of the raw stream. Blocks on the raw stream when the buffer is empty. */
    private int readByte() throws IOException {
        if (pos == limit && fill() == false) {
            return -1;
        }
        return buf[pos++] & 0xff;
    }

    private boolean fill() throws IOException {
        int n;
        do {
            n = in.read(buf, 0, buf.length);
        } while (n == 0);
        if (n < 0) {
            return false;
        }
        consumedBeforeBuf += limit;
        pos = 0;
        limit = n;
        return true;
    }

    private long offset() {
        return consumedBeforeBuf + pos;
    }
}
