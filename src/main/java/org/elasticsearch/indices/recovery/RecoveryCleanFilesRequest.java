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

import com.google.common.collect.Sets;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.util.Set;

/**
 *
 */
class RecoveryCleanFilesRequest extends TransportRequest {

    private long recoveryId;
    private ShardId shardId;
    private Set<String> legacySnapshotFiles; // legacy - we moved to a real snapshot in 1.5
    private Store.MetadataSnapshot snapshotFiles;
    private int totalTranslogOps = RecoveryState.Translog.UNKNOWN;

    RecoveryCleanFilesRequest() {
    }

    RecoveryCleanFilesRequest(long recoveryId, ShardId shardId, Store.MetadataSnapshot snapshotFiles, int totalTranslogOps) {
        this.recoveryId = recoveryId;
        this.shardId = shardId;
        this.snapshotFiles = snapshotFiles;
        this.totalTranslogOps = totalTranslogOps;
    }

    public long recoveryId() {
        return this.recoveryId;
    }

    public ShardId shardId() {
        return shardId;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        recoveryId = in.readLong();
        shardId = ShardId.readShardId(in);
        if (in.getVersion().onOrAfter(Version.V_1_5_0)) {
            snapshotFiles = Store.MetadataSnapshot.read(in);
            totalTranslogOps = in.readVInt();
        } else {
            int size = in.readVInt();
            legacySnapshotFiles = Sets.newHashSetWithExpectedSize(size);
            for (int i = 0; i < size; i++) {
                legacySnapshotFiles.add(in.readString());
            }
        }

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(recoveryId);
        shardId.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_1_5_0)) {
            snapshotFiles.writeTo(out);
            out.writeVInt(totalTranslogOps);
        } else {
            out.writeVInt(snapshotFiles.size());
            for (StoreFileMetaData snapshotFile : snapshotFiles) {
                out.writeString(snapshotFile.name());
            }
        }

    }

    public Store.MetadataSnapshot sourceMetaSnapshot() {
        return snapshotFiles;
    }

    public Set<String> legacySnapshotFiles() {
        return legacySnapshotFiles;
    }

    public int totalTranslogOps() {
        return totalTranslogOps;
    }
}
