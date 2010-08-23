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

package org.elasticsearch.index.translog.fs;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogException;
import org.elasticsearch.index.translog.TranslogStreams;

import java.io.*;

/**
 * @author kimchy (shay.banon)
 */
public class FsStreamSnapshot implements Translog.Snapshot {

    private final ShardId shardId;

    private final long id;

    private final int totalOperations;

    private final int snapshotOperations;

    private final RafReference raf;

    private final long length;

    private final DataInputStream dis;

    private Translog.Operation lastOperationRead = null;

    private int position = 0;

    private byte[] cachedData;

    public FsStreamSnapshot(ShardId shardId, long id, RafReference raf, long length, int totalOperations, int snapshotOperations) throws FileNotFoundException {
        this.shardId = shardId;
        this.id = id;
        this.raf = raf;
        this.length = length;
        this.totalOperations = totalOperations;
        this.snapshotOperations = snapshotOperations;
        this.dis = new DataInputStream(new FileInputStream(raf.file()));
    }

    @Override public long translogId() {
        return this.id;
    }

    @Override public long position() {
        return this.position;
    }

    @Override public long length() {
        return this.length;
    }

    @Override public int totalOperations() {
        return this.totalOperations;
    }

    @Override public int snapshotOperations() {
        return this.snapshotOperations;
    }

    @Override public InputStream stream() throws IOException {
        return dis;
    }

    @Override public long lengthInBytes() {
        return length - position;
    }

    @Override public boolean hasNext() {
        try {
            if (position > length) {
                return false;
            }
            int opSize = dis.readInt();
            position += 4;
            if ((position + opSize) > length) {
                // restore the position to before we read the opSize
                position -= 4;
                return false;
            }
            position += opSize;
            if (cachedData == null) {
                cachedData = new byte[opSize];
            } else if (cachedData.length < opSize) {
                cachedData = new byte[opSize];
            }
            dis.readFully(cachedData, 0, opSize);
            lastOperationRead = TranslogStreams.readTranslogOperation(new BytesStreamInput(cachedData, 0, opSize));
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override public Translog.Operation next() {
        return this.lastOperationRead;
    }

    @Override public void seekForward(long length) {
        this.position += length;
        try {
            this.dis.skip(length);
        } catch (IOException e) {
            throw new TranslogException(shardId, "failed to seek forward", e);
        }
    }

    @Override public boolean release() throws ElasticSearchException {
        try {
            dis.close();
        } catch (IOException e) {
            // ignore
        }
        raf.decreaseRefCount();
        return true;
    }
}
