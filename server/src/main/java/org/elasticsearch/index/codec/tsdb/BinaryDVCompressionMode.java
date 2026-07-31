/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.codecs.compressing.CompressionMode;
import org.elasticsearch.index.codec.zstd.ZstdCompressionMode;

public enum BinaryDVCompressionMode {

    NO_COMPRESS((byte) 0, null),
    COMPRESSED_ZSTD_LEVEL_1((byte) 1, new ZstdCompressionMode(1));

    public final byte code;
    private final CompressionMode compressionMode;

    private static final BinaryDVCompressionMode[] values = new BinaryDVCompressionMode[values().length];
    static {
        for (BinaryDVCompressionMode mode : values()) {
            values[mode.code] = mode;
        }
    }

    BinaryDVCompressionMode(byte code, CompressionMode compressionMode) {
        this.code = code;
        this.compressionMode = compressionMode;
    }

    public static BinaryDVCompressionMode fromMode(byte code) {
        if (code < 0 || code >= values.length) {
            throw new IllegalStateException("unknown compression mode [" + code + "]");
        }
        return values[code];
    }

    public CompressionMode compressionMode() {
        if (compressionMode == null) {
            throw new UnsupportedOperationException("BinaryDVCompressionMode [" + code + "] does not support compression");
        }
        return compressionMode;
    }

    public record BlockHeader(boolean isCompressed, boolean isColumnar, boolean isSubchunked) {
        static final byte IS_COMPRESSED = 0x1;
        /**
         * Set when the block uses the columnar layout for flattened {@code ._keyed} fields:
         * an uncompressed key dictionary followed by a (optionally compressed) values region.
         * When set, the row-oriented doc-offsets array is absent.
         */
        static final byte IS_COLUMNAR = 0x2;
        /**
         * Set (together with {@link #IS_COLUMNAR}) when each key's value run is stored as an
         * individually compressed chunk. Enables single-key decompression without touching the
         * runs for other keys.
         */
        static final byte IS_SUBCHUNKED = 0x4;

        /** Convenience constructor for row-oriented blocks (back-compat). */
        BlockHeader(boolean isCompressed) {
            this(isCompressed, false, false);
        }

        /** Convenience constructor for columnar blocks without sub-chunking (back-compat). */
        BlockHeader(boolean isCompressed, boolean isColumnar) {
            this(isCompressed, isColumnar, false);
        }

        public static BlockHeader fromByte(byte header) {
            boolean isCompressed = (header & IS_COMPRESSED) != 0;
            boolean isColumnar = (header & IS_COLUMNAR) != 0;
            boolean isSubchunked = (header & IS_SUBCHUNKED) != 0;
            return new BlockHeader(isCompressed, isColumnar, isSubchunked);
        }

        public byte toByte() {
            byte header = 0;
            if (isCompressed) {
                header |= IS_COMPRESSED;
            }
            if (isColumnar) {
                header |= IS_COLUMNAR;
            }
            if (isSubchunked) {
                header |= IS_SUBCHUNKED;
            }
            return header;
        }
    }
}
