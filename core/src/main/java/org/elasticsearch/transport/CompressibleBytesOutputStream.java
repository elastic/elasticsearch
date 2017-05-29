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
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.BytesStream;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasable;

import java.io.IOException;

final class CompressibleBytesOutputStream extends StreamOutput implements Releasable {

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
    BytesStream finishStream() throws IOException {
        // If we are using compression the stream needs to be closed to ensure that EOS marker bytes are written.
        // The actual ReleasableBytesStreamOutput will not be closed yet as it is wrapped in flushOnCloseStream when
        // passed to the deflater stream.
        if (shouldCompress) {
            stream.close();
        }

        return bytesStreamOutput;
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
    public void close() {
        if (stream == bytesStreamOutput) {
            IOUtils.closeWhileHandlingException(stream);
        } else {
            IOUtils.closeWhileHandlingException(stream, bytesStreamOutput);
        }
    }

    @Override
    public void reset() throws IOException {
        stream.reset();
    }
}
