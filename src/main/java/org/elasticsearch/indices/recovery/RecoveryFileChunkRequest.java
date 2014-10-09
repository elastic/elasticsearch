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
public final class RecoveryFileChunkRequest extends TransportRequest {  // public for testing
    private boolean lastChunk;
    private long recoveryId;
    private ShardId shardId;
    private long position;
    private BytesReference content;
    private StoreFileMetaData metaData;

    RecoveryFileChunkRequest() {
    }

    public RecoveryFileChunkRequest(long recoveryId, ShardId shardId, StoreFileMetaData metaData, long position, BytesReference content, boolean lastChunk) {
        this.recoveryId = recoveryId;
        this.shardId = shardId;
        this.metaData = metaData;
        this.position = position;
        this.content = content;
        this.lastChunk = lastChunk;
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
        if (in.getVersion().onOrAfter(org.elasticsearch.Version.V_1_3_0)) {
            String versionString = in.readOptionalString();
            writtenBy = Lucene.parseVersionLenient(versionString, null);
        }
        metaData = new StoreFileMetaData(name, length, checksum, writtenBy);
        if (in.getVersion().onOrAfter(org.elasticsearch.Version.V_1_4_0_Beta1)) {
            lastChunk = in.readBoolean();
        } else {
            lastChunk = false;
        }
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
        if (out.getVersion().onOrAfter(org.elasticsearch.Version.V_1_3_0)) {
            out.writeOptionalString(metaData.writtenBy() == null ? null : metaData.writtenBy().toString());
        }
        if (out.getVersion().onOrAfter(org.elasticsearch.Version.V_1_4_0_Beta1)) {
            out.writeBoolean(lastChunk);
        }
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
