/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import net.jpountz.lz4.LZ4BlockOutputStream;

import org.elasticsearch.common.compress.DeflateCompressor;

import java.io.IOException;
import java.io.OutputStream;

public enum CompressionScheme {
    LZ4,
    DEFLATE;

    static byte[] DEFLATE_HEADER = DeflateCompressor.HEADER;
    static byte[] LZ4_HEADER = new byte[]{'L', 'Z', '4', '\0'};
    static int HEADER_LENGTH = 4;

    public static OutputStream lz4OutputStream(OutputStream outputStream) throws IOException {
        outputStream.write(LZ4_HEADER);
        return new LZ4BlockOutputStream(outputStream, 64 * 1024);
    }
}
