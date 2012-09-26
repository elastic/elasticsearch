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

package org.elasticsearch.indices.recovery;

import com.google.common.collect.Sets;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.util.Set;

/**
 *
 */
class RecoveryCleanFilesRequest extends TransportRequest {

    private long recoveryId;
    private ShardId shardId;

    private Set<String> snapshotFiles;

    RecoveryCleanFilesRequest() {
    }

    RecoveryCleanFilesRequest(long recoveryId, ShardId shardId, Set<String> snapshotFiles) {
        this.recoveryId = recoveryId;
        this.shardId = shardId;
        this.snapshotFiles = snapshotFiles;
    }

    public long recoveryId() {
        return this.recoveryId;
    }

    public ShardId shardId() {
        return shardId;
    }

    public Set<String> snapshotFiles() {
        return snapshotFiles;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        recoveryId = in.readLong();
        shardId = ShardId.readShardId(in);
        int size = in.readVInt();
        snapshotFiles = Sets.newHashSetWithExpectedSize(size);
        for (int i = 0; i < size; i++) {
            snapshotFiles.add(in.readString());
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(recoveryId);
        shardId.writeTo(out);
        out.writeVInt(snapshotFiles.size());
        for (String snapshotFile : snapshotFiles) {
            out.writeString(snapshotFile);
        }
    }
}
