/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.compress;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface Compressor {

    boolean isCompressed(BytesReference bytes);

    /**
     * Same as {@link #threadLocalInputStream(InputStream)} but wraps the returned stream as a {@link StreamInput}.
     */
    default StreamInput threadLocalStreamInput(InputStream in) throws IOException {
        // wrap stream in buffer since InputStreamStreamInput doesn't do any buffering itself but does a lot of small reads
        return new InputStreamStreamInput(new BufferedInputStream(threadLocalInputStream(in), DeflateCompressor.BUFFER_SIZE) {
            @Override
            public int read() throws IOException {
                // override read to avoid synchronized single byte reads now that JEP374 removed biased locking
                if (pos >= count) {
                    return super.read();
                }
                return buf[pos++] & 0xFF;
            }
        });
    }

    /**
     * Creates a new input stream that decompresses the contents read from the provided input stream.
     * Closing the returned {@link InputStream} will close the provided stream input.
     * Note: The returned stream may only be used on the thread that created it as it might use thread-local resources and must be safely
     * closed after use
     */
    InputStream threadLocalInputStream(InputStream in) throws IOException;

    /**
     * Creates a new output stream that compresses the contents and writes to the provided output stream.
     * Closing the returned {@link OutputStream} will close the provided output stream.
     * Note: The returned stream may only be used on the thread that created it as it might use thread-local resources and must be safely
     * closed after use
     */
    OutputStream threadLocalOutputStream(OutputStream out) throws IOException;

    /**
     * Decompress bytes into a newly allocated buffer.
     *
     * @param bytesReference bytes to decompress
     * @return decompressed bytes
     */
    BytesReference uncompress(BytesReference bytesReference) throws IOException;

    /**
     * Compress bytes into a newly allocated buffer.
     *
     * @param bytesReference bytes to compress
     * @return compressed bytes
     */
    BytesReference compress(BytesReference bytesReference) throws IOException;
}
