/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.compress;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.transport.Compression;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class LZ4Compressor implements Compressor {

    public static final Compressor INSTANCE = new LZ4Compressor();

    private static final byte[] HEADER = new byte[]{'L', 'Z', '4', '\0'};

    @Override
    public boolean isCompressed(BytesReference bytes) {
        if (bytes.length() < HEADER.length) {
            return false;
        }
        for (int i = 0; i < HEADER.length; ++i) {
            if (bytes.get(i) != HEADER[i]) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int headerLength() {
        return HEADER.length;
    }

    @Override
    public InputStream threadLocalInputStream(InputStream in) throws IOException {
        // Skip the header
        in.read(new byte[headerLength()]);
        return Compression.Scheme.lz4FrameInputStream(in);
    }

    @Override
    public OutputStream threadLocalOutputStream(OutputStream out) throws IOException {
        return Compression.Scheme.lz4FrameOutputStream(out);
    }

    @Override
    public BytesReference uncompress(BytesReference bytesReference) throws IOException {
        // TODO: Will implement
        throw new AssertionError("Unimplemented");
    }

    @Override
    public BytesReference compress(BytesReference bytesReference) throws IOException {
        // TODO: Will implement
        throw new AssertionError("Unimplemented");
    }
}
