/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.zstd;

import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.compressing.Compressor;
import org.apache.lucene.codecs.compressing.Decompressor;

public class ZstdCompressionMode extends CompressionMode {
    private final int level;

    public ZstdCompressionMode(int level) {
        this.level = level;
    }

    @Override
    public Compressor newCompressor() {
        return new ZstdCompressor(level);
    }

    @Override
    public Decompressor newDecompressor() {
        return new ZstdDecompressor();
    }

    @Override
    public String toString() {
        return "ZSTD(level=" + level + ")";
    }
}
