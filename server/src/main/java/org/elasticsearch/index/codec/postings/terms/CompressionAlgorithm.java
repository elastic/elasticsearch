/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.codec.postings.terms;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.util.compress.LowercaseAsciiCompression;

import java.io.IOException;

/** Compression algorithm used for suffixes of a block of terms. */
enum CompressionAlgorithm {
    NO_COMPRESSION(0x00) {

        @Override
        void read(DataInput in, byte[] out, int len) throws IOException {
            in.readBytes(out, 0, len);
        }
    },

    LOWERCASE_ASCII(0x01) {

        @Override
        void read(DataInput in, byte[] out, int len) throws IOException {
            LowercaseAsciiCompression.decompress(in, out, len);
        }
    },

    LZ4(0x02) {

        @Override
        void read(DataInput in, byte[] out, int len) throws IOException {
            org.apache.lucene.util.compress.LZ4.decompress(in, len, out, 0);
        }
    };

    private static final CompressionAlgorithm[] BY_CODE = new CompressionAlgorithm[3];

    static {
        for (CompressionAlgorithm alg : CompressionAlgorithm.values()) {
            BY_CODE[alg.code] = alg;
        }
    }

    /** Look up a {@link CompressionAlgorithm} by its {@link CompressionAlgorithm#code}. */
    static CompressionAlgorithm byCode(int code) {
        if (code < 0 || code >= BY_CODE.length) {
            throw new IllegalArgumentException("Illegal code for a compression algorithm: " + code);
        }
        return BY_CODE[code];
    }

    public final int code;

    CompressionAlgorithm(int code) {
        this.code = code;
    }

    abstract void read(DataInput in, byte[] out, int len) throws IOException;
}
