/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.compress;

import org.elasticsearch.common.bytes.BytesReference;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface Compressor {

    boolean isCompressed(BytesReference bytes);

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
