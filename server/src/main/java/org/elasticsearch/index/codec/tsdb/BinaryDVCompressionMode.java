/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.codecs.compressing.Compressor;
import org.apache.lucene.codecs.compressing.Decompressor;
import org.elasticsearch.index.codec.zstd.ZstdCompressor;
import org.elasticsearch.index.codec.zstd.ZstdDecompressor;

public enum BinaryDVCompressionMode {

    NO_COMPRESS((byte) 0, null, null),
    COMPRESSED_WITH_ZSTD_1((byte) 1, new ZstdCompressor(1), new ZstdDecompressor());

    public final byte code;
    public final Compressor compressor;
    public final Decompressor decompressor;

    BinaryDVCompressionMode(byte code, Compressor compressor, Decompressor decompressor) {
        this.code = code;
        this.compressor = compressor;
        this.decompressor = decompressor;
    }

    public static BinaryDVCompressionMode fromMode(byte mode) {
        return switch (mode) {
            case 0 -> NO_COMPRESS;
            case 1 -> COMPRESSED_WITH_ZSTD_1;
            default -> throw new IllegalStateException("unknown compression mode [" + mode + "]");
        };
    }
}
