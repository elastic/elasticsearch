/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.unit.ByteSizeUnit;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;
import java.util.zip.GZIPOutputStream;

/**
 * An {@link OutputStream} which Gzip-compresses the written data, Base64-encodes it, and writes it in fixed-size chunks to a logger. This
 * is useful for debugging information that may be too large for a single log message and/or which may include data which cannot be
 * recorded faithfully in plain-text (e.g. binary data or data with significant whitespace).
 */
public class ChunkedLoggingStream extends OutputStream {

    static final int CHUNK_SIZE = ByteSizeUnit.KB.toIntBytes(2);

    /**
     * Create an {@link OutputStream} which Gzip-compresses the written data, Base64-encodes it, and writes it in fixed-size (2kiB) chunks
     * to the given logger. If the data fits into a single chunk then the output looks like this:
     *
     * <pre>
     * $PREFIX (gzip compressed and base64-encoded; for details see ...): H4sIAAAAA...
     * </pre>
     *
     * If there are multiple chunks then they are written like this:
     *
     * <pre>
     * $PREFIX [part 1]: H4sIAAAAA...
     * $PREFIX [part 2]: r38c4MBHO...
     * $PREFIX [part 3]: ECyRFONaL...
     * $PREFIX [part 4]: kTgm+Qswm...
     * $PREFIX (gzip compressed, base64-encoded, and split into 4 parts on preceding log lines; for details see ...)
     * </pre>
     *
     * @param logger        The logger to receive the chunks of data.
     * @param level         The log level to use for the logging.
     * @param prefix        A prefix for each chunk, which should be reasonably unique to allow for reconstruction of the original message
     *                      even if multiple such streams are used concurrently.
     * @param referenceDocs A link to the relevant reference docs to help users interpret the output. Relevant reference docs are required
     *                      because the output is rather human-unfriendly and we need somewhere to describe how to decode it.
     */
    public static OutputStream create(Logger logger, Level level, String prefix, ReferenceDocs referenceDocs) throws IOException {
        return new GZIPOutputStream(Base64.getEncoder().wrap(new ChunkedLoggingStream(logger, level, prefix, referenceDocs)));
    }

    private final Logger logger;
    private final Level level;
    private final String prefix;
    private final ReferenceDocs referenceDocs;

    private int chunk;
    private int offset;
    private boolean closed;
    private final byte[] buffer = new byte[CHUNK_SIZE];

    ChunkedLoggingStream(Logger logger, Level level, String prefix, ReferenceDocs referenceDocs) {
        this.logger = Objects.requireNonNull(logger);
        this.level = Objects.requireNonNull(level);
        this.prefix = Objects.requireNonNull(prefix);
        this.referenceDocs = Objects.requireNonNull(referenceDocs);
    }

    private void flushBuffer() {
        assert closed || offset == CHUNK_SIZE : offset;
        assert offset >= 0 && offset <= CHUNK_SIZE : offset;
        chunk += 1;

        final var chunkString = new String(buffer, 0, offset, StandardCharsets.ISO_8859_1);
        offset = 0;

        if (closed && chunk == 1) {
            logger.log(level, "{} (gzip compressed and base64-encoded; for details see {}): {}", prefix, referenceDocs, chunkString);
        } else {
            logger.log(level, "{} [part {}]: {}", prefix, chunk, chunkString);
        }
    }

    @Override
    public void write(int b) throws IOException {
        assert closed == false;
        if (offset == CHUNK_SIZE) {
            flushBuffer();
        }
        buffer[offset] = (byte) b;
        assert assertSafeByte(buffer[offset]);
        offset += 1;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        assert closed == false;
        assert assertSafeBytes(b, off, len);
        while (len > 0) {
            if (offset == CHUNK_SIZE) {
                flushBuffer();
            }
            var copyLen = Math.min(len, CHUNK_SIZE - offset);
            System.arraycopy(b, off, buffer, offset, copyLen);
            offset += copyLen;
            off += copyLen;
            len -= copyLen;
        }
    }

    @Override
    public void close() throws IOException {
        if (closed == false) {
            closed = true;
            flushBuffer();
            if (chunk > 1) {
                logger.log(
                    level,
                    "{} (gzip compressed, base64-encoded, and split into {} parts on preceding log lines; for details see {})",
                    prefix,
                    chunk,
                    referenceDocs
                );
            }
        }
    }

    private static boolean assertSafeBytes(byte[] b, int off, int len) {
        for (int i = off; i < off + len; i++) {
            assertSafeByte(b[i]);
        }
        return true;
    }

    private static boolean assertSafeByte(byte b) {
        assert 0x20 <= b && b < 0x7f;
        return true;
    }
}
