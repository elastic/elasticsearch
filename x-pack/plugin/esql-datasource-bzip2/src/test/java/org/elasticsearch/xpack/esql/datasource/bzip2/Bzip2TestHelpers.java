/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.bzip2;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * Shared helpers for the split-reassembly contract between {@link Bzip2DecompressionCodec},
 * {@code FileSplitProvider}, and the NDJSON reader.
 *
 * <p>The split-decompression protocol is:
 * <ul>
 *   <li>Each range starts at a block boundary and the codec's left split finishes the current line
 *       past the boundary.</li>
 *   <li>Every non-first split drops its first (partial) line because the previous split already
 *       emitted it.</li>
 * </ul>
 *
 * <p>Applying both rules end-to-end must yield the original file byte-for-byte; this class
 * encapsulates that reassembly so multiple test classes agree on the same protocol.
 */
final class Bzip2TestHelpers {

    private Bzip2TestHelpers() {}

    /**
     * Decompresses each block-aligned range in {@code boundaries} and reassembles them using the
     * line-alignment protocol (skip the first partial line on every non-first range). Returns the
     * concatenated UTF-8 text, which must equal the original uncompressed payload.
     */
    static String reassembleLineAligned(Bzip2DecompressionCodec codec, ByteArrayStorageObject object, long[] boundaries, long fileLength)
        throws IOException {
        ByteArrayOutputStream reassembled = new ByteArrayOutputStream();
        for (int i = 0; i < boundaries.length; i++) {
            long start = boundaries[i];
            long end = (i + 1 < boundaries.length) ? boundaries[i + 1] : fileLength;
            try (InputStream stream = codec.decompressRange(object, start, end)) {
                byte[] bytes = stream.readAllBytes();
                byte[] aligned = i == 0 ? bytes : skipFirstLine(bytes);
                reassembled.write(aligned);
            }
        }
        return reassembled.toString(StandardCharsets.UTF_8);
    }

    /**
     * Drops bytes up to and including the first {@code \n}. Returns an empty array if the input
     * contains no newline (the whole buffer was a partial line from the prior split).
     */
    static byte[] skipFirstLine(byte[] bytes) {
        for (int i = 0; i < bytes.length; i++) {
            if (bytes[i] == '\n') {
                byte[] out = new byte[bytes.length - i - 1];
                System.arraycopy(bytes, i + 1, out, 0, out.length);
                return out;
            }
        }
        return new byte[0];
    }
}
