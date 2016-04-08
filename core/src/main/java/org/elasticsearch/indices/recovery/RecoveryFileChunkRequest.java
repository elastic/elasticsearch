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

package org.elasticsearch.indices.recovery;

import org.apache.lucene.util.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;

/**
 *
 */
public final class RecoveryFileChunkRequest extends TransportRequest {
    private boolean lastChunk;
    private long recoveryId;
    private ShardId shardId;
    private long position;
    private BytesReference content;
    private StoreFileMetaData metaData;
    private long sourceThrottleTimeInNanos;

    private int totalTranslogOps;

    public RecoveryFileChunkRequest() {
    }

    public RecoveryFileChunkRequest(long recoveryId, ShardId shardId, StoreFileMetaData metaData, long position, BytesReference content,
                                    boolean lastChunk, int totalTranslogOps, long sourceThrottleTimeInNanos) {
        this.recoveryId = recoveryId;
        this.shardId = shardId;
        this.metaData = metaData;
        this.position = position;
        this.content = content;
        this.lastChunk = lastChunk;
        this.totalTranslogOps = totalTranslogOps;
        this.sourceThrottleTimeInNanos = sourceThrottleTimeInNanos;
    }

    public long recoveryId() {
        return this.recoveryId;
    }

    public ShardId shardId() {
        return shardId;
    }

    public String name() {
        return metaData.name();
    }

    public long position() {
        return position;
    }

    @Nullable
    public String checksum() {
        return metaData.checksum();
    }

    public long length() {
        return metaData.length();
    }

    public BytesReference content() {
        return content;
    }

    public int totalTranslogOps() {
        return totalTranslogOps;
    }

    public long sourceThrottleTimeInNanos() {
        return sourceThrottleTimeInNanos;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        recoveryId = in.readLong();
        shardId = ShardId.readShardId(in);
        String name = in.readString();
        position = in.readVLong();
        long length = in.readVLong();
        String checksum = in.readOptionalString();
        content = in.readBytesReference();
        Version writtenBy = null;
        String versionString = in.readOptionalString();
        writtenBy = Lucene.parseVersionLenient(versionString, null);
        metaData = new StoreFileMetaData(name, length, checksum, writtenBy);
        lastChunk = in.readBoolean();
        totalTranslogOps = in.readVInt();
        sourceThrottleTimeInNanos = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(recoveryId);
        shardId.writeTo(out);
        out.writeString(metaData.name());
        out.writeVLong(position);
        out.writeVLong(metaData.length());
        out.writeOptionalString(metaData.checksum());
        out.writeBytesReference(content);
        out.writeOptionalString(metaData.writtenBy() == null ? null : metaData.writtenBy().toString());
        out.writeBoolean(lastChunk);
        out.writeVInt(totalTranslogOps);
        out.writeLong(sourceThrottleTimeInNanos);
    }

    @Override
    public String toString() {
        return shardId + ": name='" + name() + '\'' +
                ", position=" + position +
                ", length=" + length();
    }

    public StoreFileMetaData metadata() {
        return metaData;
    }

    /**
     * Returns <code>true</code> if this chunk is the last chunk in the stream.
     */
    public boolean lastChunk() {
        return lastChunk;
    }
}
