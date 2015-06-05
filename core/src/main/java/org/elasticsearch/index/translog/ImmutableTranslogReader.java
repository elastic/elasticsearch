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

package org.elasticsearch.index.translog;

import org.elasticsearch.common.io.Channels;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * a translog reader which is fixed in length
 */
public class ImmutableTranslogReader extends TranslogReader {

    private final int totalOperations;
    protected final long length;

    /**
     * Create a snapshot of translog file channel. The length parameter should be consistent with totalOperations and point
     * at the end of the last operation in this snapshot.
     */
    public ImmutableTranslogReader(long generation, ChannelReference channelReference, long firstOperationOffset, long length, int totalOperations) {
        super(generation, channelReference, firstOperationOffset);
        this.length = length;
        this.totalOperations = totalOperations;
    }

    @Override
    public final TranslogReader clone() {
        if (channelReference.tryIncRef()) {
            try {
                ImmutableTranslogReader reader = newReader(generation, channelReference, firstOperationOffset, length, totalOperations);
                channelReference.incRef(); // for the new object
                return reader;
            } finally {
                channelReference.decRef();
            }
        } else {
            throw new IllegalStateException("can't increment translog [" + generation + "] channel ref count");
        }
    }


    protected ImmutableTranslogReader newReader(long generation, ChannelReference channelReference, long offset, long length, int totalOperations) {
        return new ImmutableTranslogReader(generation, channelReference, offset, length, totalOperations);
    }

    public long sizeInBytes() {
        return length;
    }

    public int totalOperations() {
        return totalOperations;
    }

    /**
     * reads an operation at the given position into the given buffer.
     */
    protected void readBytes(ByteBuffer buffer, long position) throws IOException {
        if (position >= length) {
            throw new EOFException("read requested past EOF. pos [" + position + "] end: [" + length + "]");
        }
        if (position < firstOperationOffset) {
            throw new IOException("read requested before position of first ops. pos [" + position + "] first op on: [" + firstOperationOffset + "]");
        }
        Channels.readFromFileChannelWithEofException(channel, position, buffer);
    }

    public Checkpoint getInfo() {
        return new Checkpoint(length, totalOperations, getGeneration());
    }

}
