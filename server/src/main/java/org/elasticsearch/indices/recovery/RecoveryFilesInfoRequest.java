/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RecoveryFilesInfoRequest extends RecoveryTransportRequest {

    List<String> phase1FileNames;
    List<Long> phase1FileSizes;
    List<String> phase1ExistingFileNames;
    List<Long> phase1ExistingFileSizes;

    int totalTranslogOps;

    public RecoveryFilesInfoRequest(StreamInput in) throws IOException {
        super(in);
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

    RecoveryFilesInfoRequest(
        long recoveryId,
        long requestSeqNo,
        ShardId shardId,
        List<String> phase1FileNames,
        List<Long> phase1FileSizes,
        List<String> phase1ExistingFileNames,
        List<Long> phase1ExistingFileSizes,
        int totalTranslogOps
    ) {
        super(requestSeqNo, recoveryId, shardId);
        this.phase1FileNames = phase1FileNames;
        this.phase1FileSizes = phase1FileSizes;
        this.phase1ExistingFileNames = phase1ExistingFileNames;
        this.phase1ExistingFileSizes = phase1ExistingFileSizes;
        this.totalTranslogOps = totalTranslogOps;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        out.writeStringCollection(phase1FileNames);
        out.writeCollection(phase1FileSizes, StreamOutput::writeVLong);

        out.writeStringCollection(phase1ExistingFileNames);
        out.writeCollection(phase1ExistingFileSizes, StreamOutput::writeVLong);

        out.writeVInt(totalTranslogOps);
    }
}
