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

package org.elasticsearch.common.compress.deflate;

import org.apache.lucene.store.IndexInput;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedIndexInput;
import org.elasticsearch.common.compress.Compressor;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.jboss.netty.buffer.ChannelBuffer;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

/**
 * {@link Compressor} implementation based on the DEFLATE compression algorithm.
 */
public class DeflateCompressor implements Compressor {

    // An arbitrary header that we use to identify compressed streams
    // It needs to be different from other compressors and to not be specific
    // enough so that no stream starting with these bytes could be detected as
    // a XContent
    private static final byte[] HEADER = new byte[] { 'D', 'F', 'L', '\0' };
    // 3 is a good trade-off between speed and compression ratio
    private static final int LEVEL = 3;
    // We use buffering on the input and output of in/def-laters in order to
    // limit the number of JNI calls
    private static final int BUFFER_SIZE = 4096;

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
    public boolean isCompressed(ChannelBuffer buffer) {
        if (buffer.readableBytes() < HEADER.length) {
            return false;
        }
        final int offset = buffer.readerIndex();
        for (int i = 0; i < HEADER.length; ++i) {
            if (buffer.getByte(offset + i) != HEADER[i]) {
                return false;
            }
        }
        return true;
    }

    @Override
    public StreamInput streamInput(StreamInput in) throws IOException {
        final byte[] headerBytes = new byte[HEADER.length];
        int len = 0;
        while (len < headerBytes.length) {
            final int read = in.read(headerBytes, len, headerBytes.length - len);
            if (read == -1) {
                break;
            }
            len += read;
        }
        if (len != HEADER.length || Arrays.equals(headerBytes, HEADER) == false) {
            throw new IllegalArgumentException("Input stream is not compressed with DEFLATE!");
        }

        final boolean nowrap = true;
        final Inflater inflater = new Inflater(nowrap);
        InputStream decompressedIn = new InflaterInputStream(in, inflater, BUFFER_SIZE);
        decompressedIn = new BufferedInputStream(decompressedIn, BUFFER_SIZE);
        return new InputStreamStreamInput(decompressedIn) {
            private boolean closed = false;

            public void close() throws IOException {
                try {
                    super.close();
                } finally {
                    if (closed == false) {
                        // important to release native memory
                        inflater.end();
                        closed = true;
                    }
                }
            }
        };
    }

    @Override
    public StreamOutput streamOutput(StreamOutput out) throws IOException {
        out.writeBytes(HEADER);
        final boolean nowrap = true;
        final Deflater deflater = new Deflater(LEVEL, nowrap);
        final boolean syncFlush = true;
        OutputStream compressedOut = new DeflaterOutputStream(out, deflater, BUFFER_SIZE, syncFlush);
        compressedOut = new BufferedOutputStream(compressedOut, BUFFER_SIZE);
        return new OutputStreamStreamOutput(compressedOut) {
            private boolean closed = false;

            public void close() throws IOException {
                try {
                    super.close();
                } finally {
                    if (closed == false) {
                        // important to release native memory
                        deflater.end();
                        closed = true;
                    }
                }
            }
        };
    }

    @Override
    public boolean isCompressed(IndexInput in) throws IOException {
        return false;
    }

    @Override
    public CompressedIndexInput indexInput(IndexInput in) throws IOException {
        throw new UnsupportedOperationException();
    }
}
