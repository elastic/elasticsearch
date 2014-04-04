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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.FileChannelInputStream;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogStreams;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 *
 */
public class FsChannelSnapshot implements Translog.Snapshot {

    private final long id;

    private final int totalOperations;

    private final RafReference raf;

    private final FileChannel channel;

    private final long length;

    private Translog.Operation lastOperationRead = null;

    private int position = 0;

    private ByteBuffer cacheBuffer;

    public FsChannelSnapshot(long id, RafReference raf, long length, int totalOperations) throws FileNotFoundException {
        this.id = id;
        this.raf = raf;
        this.channel = raf.raf().getChannel();
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
    public InputStream stream() throws IOException {
        return new FileChannelInputStream(channel, position, lengthInBytes());
    }

    @Override
    public long lengthInBytes() {
        return length - position;
    }

    @Override
    public boolean hasNext() {
        try {
            if (position > length) {
                return false;
            }
            if (cacheBuffer == null) {
                cacheBuffer = ByteBuffer.allocate(1024);
            }
            cacheBuffer.limit(4);
            int bytesRead = channel.read(cacheBuffer, position);
            if (bytesRead < 4) {
                return false;
            }
            cacheBuffer.flip();
            int opSize = cacheBuffer.getInt();
            position += 4;
            if ((position + opSize) > length) {
                // restore the position to before we read the opSize
                position -= 4;
                return false;
            }
            if (cacheBuffer.capacity() < opSize) {
                cacheBuffer = ByteBuffer.allocate(opSize);
            }
            cacheBuffer.clear();
            cacheBuffer.limit(opSize);
            channel.read(cacheBuffer, position);
            cacheBuffer.flip();
            position += opSize;
            lastOperationRead = TranslogStreams.readTranslogOperation(new BytesStreamInput(cacheBuffer.array(), 0, opSize, true));
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public Translog.Operation next() {
        return this.lastOperationRead;
    }

    @Override
    public void seekForward(long length) {
        this.position += length;
    }

    @Override
    public void close() throws ElasticsearchException {
        raf.decreaseRefCount(true);
    }
}
