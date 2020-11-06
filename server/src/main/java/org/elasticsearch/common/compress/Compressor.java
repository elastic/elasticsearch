/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.compress;

import org.elasticsearch.common.bytes.BytesReference;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface Compressor {

    boolean isCompressed(BytesReference bytes);

    int headerLength();

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
