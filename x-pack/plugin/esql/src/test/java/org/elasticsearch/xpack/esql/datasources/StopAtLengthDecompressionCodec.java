/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.datasources.spi.DecompressionCodec;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * Test codec that passes the first {@code length} raw bytes through unchanged, then reports end-of-stream without
 * reading {@code raw} any further. This is the shape of a decoder that stops at the end of a frame or member, like
 * the JDK gzip decoder on JDK 21, 22 and 27+. Unlike the real gzip decoder, it behaves the same on every JDK, so
 * tests of the release's end-of-body read exercise that read deterministically.
 */
final class StopAtLengthDecompressionCodec implements DecompressionCodec {
    private final String name;
    private final int length;

    StopAtLengthDecompressionCodec(String name, int length) {
        this.name = name;
        this.length = length;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public List<String> extensions() {
        return List.of();
    }

    @Override
    public InputStream decompress(InputStream raw) {
        return new InputStream() {
            private int remaining = length;

            @Override
            public int read() throws IOException {
                byte[] one = new byte[1];
                int n = read(one, 0, 1);
                return n == -1 ? -1 : (one[0] & 0xFF);
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                if (remaining == 0) {
                    return -1;
                }
                int n = raw.read(b, off, Math.min(len, remaining));
                if (n > 0) {
                    remaining -= n;
                }
                return n;
            }
        };
    }
}
