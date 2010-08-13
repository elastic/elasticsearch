/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.common.compress.lzf;

import java.io.IOException;
import java.io.InputStream;

public class LZFInputStream extends InputStream {
    public static int EOF_FLAG = -1;

    /* stream to be decompressed */
    private final InputStream inputStream;

    /* the current buffer of compressed bytes */
    private final byte[] compressedBytes = new byte[LZFChunk.MAX_CHUNK_LEN];

    /* the buffer of uncompressed bytes from which */
    private final byte[] uncompressedBytes = new byte[LZFChunk.MAX_CHUNK_LEN];

    /* The current position (next char to output) in the uncompressed bytes buffer. */
    private int bufferPosition = 0;

    /* Length of the current uncompressed bytes buffer */
    private int bufferLength = 0;

    public LZFInputStream(final InputStream inputStream) throws IOException {
        super();
        this.inputStream = inputStream;
    }

    @Override
    public int read() throws IOException {
        int returnValue = EOF_FLAG;
        readyBuffer();
        if (bufferPosition < bufferLength) {
            returnValue = (uncompressedBytes[bufferPosition++] & 255);
        }
        return returnValue;
    }

    public int read(final byte[] buffer) throws IOException {
        return (read(buffer, 0, buffer.length));

    }

    public int read(final byte[] buffer, final int offset, final int length) throws IOException {
        int outputPos = offset;
        readyBuffer();
        if (bufferLength == -1) {
            return -1;
        }

        while (outputPos < buffer.length && bufferPosition < bufferLength) {
            int chunkLength = Math.min(bufferLength - bufferPosition, buffer.length - outputPos);
            System.arraycopy(uncompressedBytes, bufferPosition, buffer, outputPos, chunkLength);
            outputPos += chunkLength;
            bufferPosition += chunkLength;
            readyBuffer();
        }
        return outputPos;
    }

    public void close() throws IOException {
        inputStream.close();
    }

    /**
     * Fill the uncompressed bytes buffer by reading the underlying inputStream.
     *
     * @throws java.io.IOException
     */
    private void readyBuffer() throws IOException {
        if (bufferPosition >= bufferLength) {
            bufferLength = LZFDecoder.decompressChunk(inputStream, compressedBytes, uncompressedBytes);
            bufferPosition = 0;
        }
    }
}
