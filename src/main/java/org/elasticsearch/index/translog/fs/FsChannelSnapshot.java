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

package org.elasticsearch.index.translog.fs;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.index.translog.TranslogStreams;
import org.elasticsearch.index.translog.Translog;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 */
public class FsChannelSnapshot implements Translog.Snapshot {

    private final long id;

    private final int totalOperations;

    private final ChannelReference channelReference;

    private final FileChannel channel;

    private final long length;

    private Translog.Operation lastOperationRead = null;

    private long position = 0;

    private ByteBuffer cacheBuffer;

    private AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Create a snapshot of translog file channel. The length parameter should be consistent with totalOperations and point
     * at the end of the last operation in this snapshot.
     */
    public FsChannelSnapshot(long id, ChannelReference channelReference, long length, int totalOperations) throws FileNotFoundException {
        this.id = id;
        this.channelReference = channelReference;
        this.channel = channelReference.channel();
        this.length = length;
        this.totalOperations = totalOperations;
    }

    @Override
    public long translogId() {
        return this.id;
    }

    @Override
    public long position() {
        return this.position;
    }

    @Override
    public long length() {
        return this.length;
    }

    @Override
    public int estimatedTotalOperations() {
        return this.totalOperations;
    }

    @Override
    public long lengthInBytes() {
        return length - position;
    }

    @Override
    public Translog.Operation next() {
        try {
            if (position >= length) {
                return null;
            }
            if (cacheBuffer == null) {
                cacheBuffer = ByteBuffer.allocate(1024);
            }
            cacheBuffer.limit(4);
            int bytesRead = Channels.readFromFileChannel(channel, position, cacheBuffer);
            if (bytesRead < 0) {
                // the snapshot is acquired under a write lock. we should never
                // read beyond the EOF, must be an abrupt EOF
                throw new EOFException("read past EOF. pos [" + position + "] length: [" + cacheBuffer.limit() + "] end: [" + channel.size() + "]");
            }
            assert bytesRead == 4;
            cacheBuffer.flip();
            // Add an extra 4 to account for the operation size integer itself
            int opSize = cacheBuffer.getInt() + 4;
            if ((position + opSize) > length) {
                // the snapshot is acquired under a write lock. we should never
                // read beyond the EOF, must be an abrupt EOF
                throw new EOFException("opSize of [" + opSize + "] pointed beyond EOF. position [" + position + "] length [" + length + "]");
            }
            if (cacheBuffer.capacity() < opSize) {
                cacheBuffer = ByteBuffer.allocate(opSize);
            }
            cacheBuffer.clear();
            cacheBuffer.limit(opSize);
            bytesRead = Channels.readFromFileChannel(channel, position, cacheBuffer);
            if (bytesRead < 0) {
                // the snapshot is acquired under a write lock. we should never
                // read beyond the EOF, must be an abrupt EOF
                throw new EOFException("tried to read past EOF. opSize [" + opSize + "] position [" + position + "] length [" + length + "]");
            }
            cacheBuffer.flip();
            position += opSize;
            BytesArray bytesArray = new BytesArray(cacheBuffer.array(), 0, opSize);
            return TranslogStreams.readTranslogOperation(new BytesStreamInput(bytesArray.copyBytesArray()));
        } catch (IOException e) {
            throw new ElasticsearchException("unexpected exception reading from translog snapshot of " + this.channelReference.file(), e);
        }
    }

    @Override
    public void seekTo(long position) {
        this.position = position;
    }

    @Override
    public void close() throws ElasticsearchException {
        if (closed.compareAndSet(false, true)) {
            channelReference.decRef();
        }
    }
}
