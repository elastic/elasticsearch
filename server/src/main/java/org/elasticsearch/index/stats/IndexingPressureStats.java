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

    private final long totalCoordinatingAndPrimaryBytes;
    private final long totalReplicaBytes;
    private final long currentCoordinatingAndPrimaryBytes;
    private final long currentReplicaBytes;
    private final long coordinatingAndPrimaryRejections;
    private final long replicaRejections;

    public IndexingPressureStats(StreamInput in) throws IOException {
        totalCoordinatingAndPrimaryBytes = in.readVLong();
        totalReplicaBytes = in.readVLong();
        currentCoordinatingAndPrimaryBytes = in.readVLong();
        currentReplicaBytes = in.readVLong();
        coordinatingAndPrimaryRejections = in.readVLong();
        replicaRejections = in.readVLong();
    }

    public IndexingPressureStats(long totalCoordinatingAndPrimaryBytes, long totalReplicaBytes, long currentCoordinatingAndPrimaryBytes,
                                 long currentReplicaBytes, long coordinatingAndPrimaryRejections, long replicaRejections) {
        this.totalCoordinatingAndPrimaryBytes = totalCoordinatingAndPrimaryBytes;
        this.totalReplicaBytes = totalReplicaBytes;
        this.currentCoordinatingAndPrimaryBytes = currentCoordinatingAndPrimaryBytes;
        this.currentReplicaBytes = currentReplicaBytes;
        this.coordinatingAndPrimaryRejections = coordinatingAndPrimaryRejections;
        this.replicaRejections = replicaRejections;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(totalCoordinatingAndPrimaryBytes);
        out.writeVLong(totalReplicaBytes);
        out.writeVLong(currentCoordinatingAndPrimaryBytes);
        out.writeVLong(currentReplicaBytes);
        out.writeVLong(coordinatingAndPrimaryRejections);
        out.writeVLong(replicaRejections);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("indexing_pressure");
        builder.startObject("total");
        builder.field("coordinating_and_primary_bytes", totalCoordinatingAndPrimaryBytes);
        builder.field("replica_bytes", totalReplicaBytes);
        builder.field("all_bytes", totalReplicaBytes + totalCoordinatingAndPrimaryBytes);
        builder.field("coordinating_and_primary_memory_limit_rejections", coordinatingAndPrimaryRejections);
        builder.field("replica_memory_limit_rejections", replicaRejections);
        builder.endObject();
        builder.startObject("current");
        builder.field("coordinating_and_primary_bytes", currentCoordinatingAndPrimaryBytes);
        builder.field("replica_bytes", currentReplicaBytes);
        builder.field("all_bytes", currentCoordinatingAndPrimaryBytes + currentReplicaBytes);
        builder.endObject();
        return builder.endObject();
    }
}
