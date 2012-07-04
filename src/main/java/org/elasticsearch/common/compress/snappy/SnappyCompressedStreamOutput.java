/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.common.compress.snappy;

import com.ning.compress.BufferRecycler;
import org.elasticsearch.common.compress.CompressedStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 */
public abstract class SnappyCompressedStreamOutput extends CompressedStreamOutput<SnappyCompressorContext> {

    protected final BufferRecycler recycler;

    protected byte[] compressedBuffer;

    public SnappyCompressedStreamOutput(StreamOutput out, SnappyCompressorContext context) throws IOException {
        super(out, context);
        this.recycler = BufferRecycler.instance();
        this.uncompressed = this.recycler.allocOutputBuffer(context.compressChunkLength());
        this.uncompressedLength = context.compressChunkLength();
        this.compressedBuffer = recycler.allocEncodingBuffer(context.compressMaxCompressedChunkLength());
    }

    @Override
    public void writeHeader(StreamOutput out) throws IOException {
        out.writeBytes(SnappyCompressor.HEADER);
        out.writeVInt(context.compressChunkLength());
        out.writeVInt(context.compressMaxCompressedChunkLength());
    }

    @Override
    protected void doClose() throws IOException {
        byte[] buf = uncompressed;
        if (buf != null) {
            uncompressed = null;
            recycler.releaseOutputBuffer(buf);
        }
        buf = compressedBuffer;
        if (buf != null) {
            compressedBuffer = null;
            recycler.releaseEncodeBuffer(buf);
        }
    }
}
