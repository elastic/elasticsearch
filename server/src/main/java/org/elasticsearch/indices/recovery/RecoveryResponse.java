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
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;
import java.util.List;

public final class RecoveryResponse extends TransportResponse {

    final List<String> phase1FileNames;
    final List<Long> phase1FileSizes;
    final List<String> phase1ExistingFileNames;
    final List<Long> phase1ExistingFileSizes;
    final long phase1TotalSize;
    final long phase1ExistingTotalSize;
    final long phase1Time;
    final long phase1ThrottlingWaitTime;

    final long startTime;

    final int phase2Operations;
    final long phase2Time;

    RecoveryResponse(List<String> phase1FileNames, List<Long> phase1FileSizes, List<String> phase1ExistingFileNames,
                     List<Long> phase1ExistingFileSizes, long phase1TotalSize, long phase1ExistingTotalSize,
                     long phase1Time, long phase1ThrottlingWaitTime, long startTime, int phase2Operations, long phase2Time) {
        this.phase1FileNames = phase1FileNames;
        this.phase1FileSizes = phase1FileSizes;
        this.phase1ExistingFileNames = phase1ExistingFileNames;
        this.phase1ExistingFileSizes = phase1ExistingFileSizes;
        this.phase1TotalSize = phase1TotalSize;
        this.phase1ExistingTotalSize = phase1ExistingTotalSize;
        this.phase1Time = phase1Time;
        this.phase1ThrottlingWaitTime = phase1ThrottlingWaitTime;
        this.startTime = startTime;
        this.phase2Operations = phase2Operations;
        this.phase2Time = phase2Time;
    }

    RecoveryResponse(StreamInput in) throws IOException {
        super(in);
        phase1FileNames = in.readStringList();
        phase1FileSizes = in.readList(StreamInput::readVLong);
        phase1ExistingFileNames = in.readStringList();
        phase1ExistingFileSizes = in.readList(StreamInput::readVLong);
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
        out.writeStringCollection(phase1FileNames);
        out.writeCollection(phase1FileSizes, StreamOutput::writeVLong);
        out.writeStringCollection(phase1ExistingFileNames);
        out.writeCollection(phase1ExistingFileSizes, StreamOutput::writeVLong);
        out.writeVLong(phase1TotalSize);
        out.writeVLong(phase1ExistingTotalSize);
        out.writeVLong(phase1Time);
        out.writeVLong(phase1ThrottlingWaitTime);
        out.writeVLong(startTime);
        out.writeVInt(phase2Operations);
        out.writeVLong(phase2Time);
    }
}
