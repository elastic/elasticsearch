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

    public record BlockHeader(boolean isCompressed) {
        static final byte IS_COMPRESSED = 0x1;

        public static BlockHeader fromByte(byte header) {
            boolean isCompressed = (header & IS_COMPRESSED) != 0;
            return new BlockHeader(isCompressed);
        }

        public byte toByte() {
            byte header = 0;
            if (isCompressed) {
                header |= IS_COMPRESSED;
            }
            return header;
        }
    }
}
