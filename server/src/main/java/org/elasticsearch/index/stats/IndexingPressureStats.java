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

    private final long totalPrimaryAndCoordinatingBytes;
    private final long totalReplicaBytes;
    private final long pendingPrimaryAndCoordinatingBytes;
    private final long pendingReplicaBytes;
    private final long primaryAndCoordinatingRejections;
    private final long replicaRejections;

    public IndexingPressureStats(StreamInput in) throws IOException {
        totalPrimaryAndCoordinatingBytes = in.readVLong();
        totalReplicaBytes = in.readVLong();
        pendingPrimaryAndCoordinatingBytes = in.readVLong();
        pendingReplicaBytes = in.readVLong();
        primaryAndCoordinatingRejections = in.readVLong();
        replicaRejections = in.readVLong();
    }

    public IndexingPressureStats(long totalPrimaryAndCoordinatingBytes, long totalReplicaBytes, long pendingPrimaryAndCoordinatingBytes,
                                 long pendingReplicaBytes, long primaryAndCoordinatingRejections, long replicaRejections) {
        this.totalPrimaryAndCoordinatingBytes = totalPrimaryAndCoordinatingBytes;
        this.totalReplicaBytes = totalReplicaBytes;
        this.pendingPrimaryAndCoordinatingBytes = pendingPrimaryAndCoordinatingBytes;
        this.pendingReplicaBytes = pendingReplicaBytes;
        this.primaryAndCoordinatingRejections = primaryAndCoordinatingRejections;
        this.replicaRejections = replicaRejections;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(totalPrimaryAndCoordinatingBytes);
        out.writeVLong(totalReplicaBytes);
        out.writeVLong(pendingPrimaryAndCoordinatingBytes);
        out.writeVLong(pendingReplicaBytes);
        out.writeVLong(primaryAndCoordinatingRejections);
        out.writeVLong(replicaRejections);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("indexing_pressure");
        builder.field("total_primary_and_coordinating_bytes", totalPrimaryAndCoordinatingBytes);
        builder.field("total_replica_bytes", totalReplicaBytes);
        builder.field("pending_primary_and_coordinating_bytes", pendingPrimaryAndCoordinatingBytes);
        builder.field("pending_replica_bytes", pendingReplicaBytes);
        builder.field("primary_and_coordinating_rejections", primaryAndCoordinatingRejections);
        builder.field("replica_rejections", replicaRejections);
        return builder.endObject();
    }
}
