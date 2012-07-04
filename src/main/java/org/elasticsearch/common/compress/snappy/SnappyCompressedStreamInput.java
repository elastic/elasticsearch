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
import org.elasticsearch.common.compress.CompressedStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;
import java.util.Arrays;

/**
 */
public abstract class SnappyCompressedStreamInput extends CompressedStreamInput<SnappyCompressorContext> {

    protected final BufferRecycler recycler;

    protected int chunkSize;

    protected int maxCompressedChunkLength;

    protected byte[] inputBuffer;

    public SnappyCompressedStreamInput(StreamInput in, SnappyCompressorContext context) throws IOException {
        super(in, context);
        this.recycler = BufferRecycler.instance();
        this.uncompressed = recycler.allocDecodeBuffer(Math.max(chunkSize, maxCompressedChunkLength));
        this.inputBuffer = recycler.allocInputBuffer(Math.max(chunkSize, maxCompressedChunkLength));
    }

    @Override
    public void readHeader(StreamInput in) throws IOException {
        byte[] header = new byte[SnappyCompressor.HEADER.length];
        in.readBytes(header, 0, header.length);
        if (!Arrays.equals(header, SnappyCompressor.HEADER)) {
            throw new IOException("wrong snappy compressed header [" + Arrays.toString(header) + "]");
        }
        this.chunkSize = in.readVInt();
        this.maxCompressedChunkLength = in.readVInt();
    }

    @Override
    protected void doClose() throws IOException {
        byte[] buf = uncompressed;
        if (buf != null) {
            uncompressed = null;
            recycler.releaseDecodeBuffer(uncompressed);
        }
        buf = inputBuffer;
        if (buf != null) {
            inputBuffer = null;
            recycler.releaseInputBuffer(inputBuffer);
        }
    }
}
