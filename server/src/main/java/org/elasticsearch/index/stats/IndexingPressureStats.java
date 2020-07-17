/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.stats;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class IndexingPressureStats implements Writeable, ToXContentFragment {


    private final long totalCombinedCoordinatingAndPrimaryBytes;
    private final long totalCoordinatingBytes;
    private final long totalPrimaryBytes;
    private final long totalReplicaBytes;

    private final long currentCombinedCoordinatingAndPrimaryBytes;
    private final long currentCoordinatingBytes;
    private final long currentPrimaryBytes;
    private final long currentReplicaBytes;
    private final long coordinatingRejections;
    private final long primaryRejections;
    private final long replicaRejections;

    public IndexingPressureStats(StreamInput in) throws IOException {
        totalCombinedCoordinatingAndPrimaryBytes = in.readVLong();
        totalCoordinatingBytes = in.readVLong();
        totalPrimaryBytes = in.readVLong();
        totalReplicaBytes = in.readVLong();

        currentCombinedCoordinatingAndPrimaryBytes = in.readVLong();
        currentCoordinatingBytes = in.readVLong();
        currentPrimaryBytes = in.readVLong();
        currentReplicaBytes = in.readVLong();

        coordinatingRejections = in.readVLong();
        primaryRejections = in.readVLong();
        replicaRejections = in.readVLong();
    }

    public IndexingPressureStats(long totalCombinedCoordinatingAndPrimaryBytes, long totalCoordinatingBytes, long totalPrimaryBytes,
                                 long totalReplicaBytes, long currentCombinedCoordinatingAndPrimaryBytes, long currentCoordinatingBytes,
                                 long currentPrimaryBytes, long currentReplicaBytes, long coordinatingRejections, long primaryRejections,
                                 long replicaRejections) {
        this.totalCombinedCoordinatingAndPrimaryBytes = totalCombinedCoordinatingAndPrimaryBytes;
        this.totalCoordinatingBytes = totalCoordinatingBytes;
        this.totalPrimaryBytes = totalPrimaryBytes;
        this.totalReplicaBytes = totalReplicaBytes;
        this.currentCombinedCoordinatingAndPrimaryBytes = currentCombinedCoordinatingAndPrimaryBytes;
        this.currentCoordinatingBytes = currentCoordinatingBytes;
        this.currentPrimaryBytes = currentPrimaryBytes;
        this.currentReplicaBytes = currentReplicaBytes;
        this.coordinatingRejections = coordinatingRejections;
        this.primaryRejections = primaryRejections;
        this.replicaRejections = replicaRejections;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(totalCombinedCoordinatingAndPrimaryBytes);
        out.writeVLong(totalCoordinatingBytes);
        out.writeVLong(totalPrimaryBytes);
        out.writeVLong(totalReplicaBytes);

        out.writeVLong(currentCombinedCoordinatingAndPrimaryBytes);
        out.writeVLong(currentCoordinatingBytes);
        out.writeVLong(currentPrimaryBytes);
        out.writeVLong(currentReplicaBytes);

        out.writeVLong(coordinatingRejections);
        out.writeVLong(primaryRejections);
        out.writeVLong(replicaRejections);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("indexing_pressure");
        builder.startObject("total");
        builder.field("combined_coordinating_and_primary_bytes", totalCombinedCoordinatingAndPrimaryBytes);
        builder.field("coordinating_bytes", totalCoordinatingBytes);
        builder.field("primary_bytes", totalPrimaryBytes);
        builder.field("replica_bytes", totalReplicaBytes);
        builder.field("all_bytes", totalReplicaBytes + totalCombinedCoordinatingAndPrimaryBytes);
        builder.field("coordinating_rejections", coordinatingRejections);
        builder.field("primary_rejections", primaryRejections);
        builder.field("replica_rejections", replicaRejections);
        builder.endObject();
        builder.startObject("current");
        builder.field("combined_coordinating_and_primary_bytes", currentCombinedCoordinatingAndPrimaryBytes);
        builder.field("coordinating_bytes", currentCoordinatingBytes);
        builder.field("primary_bytes", currentPrimaryBytes);
        builder.field("replica_bytes", currentReplicaBytes);
        builder.field("all_bytes", currentCombinedCoordinatingAndPrimaryBytes + currentReplicaBytes);
        builder.endObject();
        return builder.endObject();
    }
}
