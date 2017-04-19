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

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;

class LegacyRecoveryResponse extends RecoveryResponse {

    private List<String> phase1FileNames = new ArrayList<>();
    private List<Long> phase1FileSizes = new ArrayList<>();
    private List<String> phase1ExistingFileNames = new ArrayList<>();
    private List<Long> phase1ExistingFileSizes = new ArrayList<>();
    private long phase1TotalSize;
    private long phase1ExistingTotalSize;
    private long phase1Time;
    private long phase1ThrottlingWaitTime;

    private long startTime;

    private int phase2Operations;
    private long phase2Time;

    LegacyRecoveryResponse() {
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
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

        phase1TotalSize = in.readVLong();
        phase1ExistingTotalSize = in.readVLong();
        phase1Time = in.readVLong();
        phase1ThrottlingWaitTime = in.readVLong();
        startTime = in.readVLong();
        phase2Operations = in.readVInt();
        phase2Time = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(phase1FileNames.size());
        for (String name : phase1FileNames) {
            out.writeString(name);
        }
        out.writeVInt(phase1FileSizes.size());
        for (long size : phase1FileSizes) {
            out.writeVLong(size);
        }

        out.writeVInt(phase1ExistingFileNames.size());
        for (String name : phase1ExistingFileNames) {
            out.writeString(name);
        }
        out.writeVInt(phase1ExistingFileSizes.size());
        for (long size : phase1ExistingFileSizes) {
            out.writeVLong(size);
        }

        out.writeVLong(phase1TotalSize);
        out.writeVLong(phase1ExistingTotalSize);
        out.writeVLong(phase1Time);
        out.writeVLong(phase1ThrottlingWaitTime);
        out.writeVLong(startTime);
        out.writeVInt(phase2Operations);
        out.writeVLong(phase2Time);
    }

    @Override
    public void logTraceSummary(Logger logger, StartRecoveryRequest request,
                                TimeValue recoveryTime) {
        StringBuilder sb = new StringBuilder();
        sb.append('[').append(request.shardId().getIndex().getName()).append(']').append('[')
            .append(request.shardId().id())
            .append("] ");
        sb.append("recovery completed from ").append(request.sourceNode()).append(", took[")
            .append(recoveryTime).append("]\n");
        sb.append("   phase1: recovered_files [").append(phase1FileNames.size()).append("]").append(" with " +
            "total_size of [").append(new ByteSizeValue(phase1TotalSize)).append("]")
            .append(", took [").append(timeValueMillis(phase1Time)).append("], throttling_wait [").append
            (timeValueMillis(phase1ThrottlingWaitTime)).append(']')
            .append("\n");
        sb.append("         : reusing_files   [").append(phase1ExistingFileNames.size())
            .append("] with total_size of [").append(new ByteSizeValue(phase1ExistingTotalSize))
            .append("]\n");
        sb.append("   phase2: start took [").append(timeValueMillis(startTime)).append("]\n");
        sb.append("         : recovered [").append(phase2Operations).append("]")
            .append(" transaction log operations")
            .append(", took [").append(timeValueMillis(phase2Time)).append("]")
            .append("\n");
        logger.trace("{}", sb);
    }
}
