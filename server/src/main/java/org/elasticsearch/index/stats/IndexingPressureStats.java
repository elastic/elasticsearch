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
    private final long pendingCoordinatingAndPrimaryBytes;
    private final long pendingReplicaBytes;

    public IndexingPressureStats(StreamInput in) throws IOException {
        totalCoordinatingAndPrimaryBytes = in.readVLong();
        totalReplicaBytes = in.readVLong();
        pendingCoordinatingAndPrimaryBytes = in.readVLong();
        pendingReplicaBytes = in.readVLong();
    }

    public IndexingPressureStats(long totalCoordinatingAndPrimaryBytes, long totalReplicaBytes, long pendingCoordinatingAndPrimaryBytes,
                                 long pendingReplicaBytes) {
        this.totalCoordinatingAndPrimaryBytes = totalCoordinatingAndPrimaryBytes;
        this.totalReplicaBytes = totalReplicaBytes;
        this.pendingCoordinatingAndPrimaryBytes = pendingCoordinatingAndPrimaryBytes;
        this.pendingReplicaBytes = pendingReplicaBytes;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // TODO: Add total
        out.writeVLong(totalCoordinatingAndPrimaryBytes);
        out.writeVLong(totalReplicaBytes);
        out.writeVLong(pendingCoordinatingAndPrimaryBytes);
        out.writeVLong(pendingReplicaBytes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("indexing_pressure");
        builder.field("total_coordinating_and_primary_bytes", totalCoordinatingAndPrimaryBytes);
        builder.field("total_replica_bytes", totalReplicaBytes);
        builder.field("pending_coordinating_and_primary_bytes", pendingCoordinatingAndPrimaryBytes);
        builder.field("pending_replica_bytes", pendingReplicaBytes);
        return builder.endObject();
    }
}
