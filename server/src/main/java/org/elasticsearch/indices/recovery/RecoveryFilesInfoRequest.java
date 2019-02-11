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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RecoveryFilesInfoRequest extends TransportRequest {

    private long recoveryId;
    private ShardId shardId;

    List<String> phase1FileNames;
    List<Long> phase1FileSizes;
    List<String> phase1ExistingFileNames;
    List<Long> phase1ExistingFileSizes;

    int totalTranslogOps;

    public RecoveryFilesInfoRequest() {
    }

    RecoveryFilesInfoRequest(long recoveryId, ShardId shardId, List<String> phase1FileNames, List<Long> phase1FileSizes,
                             List<String> phase1ExistingFileNames, List<Long> phase1ExistingFileSizes, int totalTranslogOps) {
        this.recoveryId = recoveryId;
        this.shardId = shardId;
        this.phase1FileNames = phase1FileNames;
        this.phase1FileSizes = phase1FileSizes;
        this.phase1ExistingFileNames = phase1ExistingFileNames;
        this.phase1ExistingFileSizes = phase1ExistingFileSizes;
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
        int size = in.readVInt();
        phase1FileNames = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            phase1FileNames.add(in.readString());
        }

        size = in.readVInt();
        phase1FileSizes = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            phase1FileSizes.add(in.readVLong());
        }

        size = in.readVInt();
        phase1ExistingFileNames = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            phase1ExistingFileNames.add(in.readString());
        }

        size = in.readVInt();
        phase1ExistingFileSizes = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            phase1ExistingFileSizes.add(in.readVLong());
        }
        totalTranslogOps = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(recoveryId);
        shardId.writeTo(out);

        out.writeVInt(phase1FileNames.size());
        for (String phase1FileName : phase1FileNames) {
            out.writeString(phase1FileName);
        }

        out.writeVInt(phase1FileSizes.size());
        for (Long phase1FileSize : phase1FileSizes) {
            out.writeVLong(phase1FileSize);
        }

        out.writeVInt(phase1ExistingFileNames.size());
        for (String phase1ExistingFileName : phase1ExistingFileNames) {
            out.writeString(phase1ExistingFileName);
        }

        out.writeVInt(phase1ExistingFileSizes.size());
        for (Long phase1ExistingFileSize : phase1ExistingFileSizes) {
            out.writeVLong(phase1ExistingFileSize);
        }
        out.writeVInt(totalTranslogOps);
    }
}
