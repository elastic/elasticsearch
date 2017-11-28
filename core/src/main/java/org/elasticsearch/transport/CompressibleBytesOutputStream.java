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

package org.elasticsearch.transport;

import org.apache.lucene.util.IOUtils;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.BytesStream;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasable;

import java.io.IOException;
import java.util.zip.DeflaterOutputStream;

/**
 * This class exists to provide a stream with optional compression. This is useful as using compression
 * requires that the underlying {@link DeflaterOutputStream} be closed to write EOS bytes. However, the
 * {@link BytesStream} should not be closed yet, as we have not used the bytes. This class handles these
 * intricacies.
 *
 * {@link CompressibleBytesOutputStream#materializeBytes()} should be called when all the bytes have been
 * written to this stream. If compression is enabled, the proper EOS bytes will be written at that point.
 * The underlying {@link BytesReference} will be returned.
 *
 * {@link CompressibleBytesOutputStream#close()} should be called when the bytes are no longer needed and
 * can be safely released.
 */
final class CompressibleBytesOutputStream extends StreamOutput {

    private final StreamOutput stream;
    private final BytesStream bytesStreamOutput;
    private final boolean shouldCompress;

    CompressibleBytesOutputStream(BytesStream bytesStreamOutput, boolean shouldCompress) throws IOException {
        this.bytesStreamOutput = bytesStreamOutput;
        this.shouldCompress = shouldCompress;
        if (shouldCompress) {
            this.stream = CompressorFactory.COMPRESSOR.streamOutput(Streams.flushOnCloseStream(bytesStreamOutput));
        } else {
            this.stream = bytesStreamOutput;
        }
    }

    /**
     * This method ensures that compression is complete and returns the underlying bytes.
     *
     * @return bytes underlying the stream
     * @throws IOException if an exception occurs when writing or flushing
     */
    BytesReference materializeBytes() throws IOException {
        // If we are using compression the stream needs to be closed to ensure that EOS marker bytes are written.
        // The actual ReleasableBytesStreamOutput will not be closed yet as it is wrapped in flushOnCloseStream when
        // passed to the deflater stream.
        if (shouldCompress) {
            stream.close();
        }

        return bytesStreamOutput.bytes();
    }

    @Override
    public void writeByte(byte b) throws IOException {
        stream.write(b);
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
        stream.writeBytes(b, offset, length);
    }

    @Override
    public void flush() throws IOException {
        stream.flush();
    }

    @Override
    public void close() throws IOException {
        if (stream == bytesStreamOutput) {
            assert shouldCompress == false : "If the streams are the same we should not be compressing";
            IOUtils.close(stream);
        } else {
            assert shouldCompress : "If the streams are different we should be compressing";
            IOUtils.close(stream, bytesStreamOutput);
        }
    }

    @Override
    public void reset() throws IOException {
        throw new UnsupportedOperationException();
    }
}
