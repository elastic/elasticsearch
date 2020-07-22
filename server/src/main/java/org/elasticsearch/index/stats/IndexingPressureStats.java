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
import org.elasticsearch.common.unit.ByteSizeValue;
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

    private static final String COMBINED = "combined_coordinating_and_primary";
    private static final String COMBINED_IN_BYTES = "combined_coordinating_and_primary_in_bytes";
    private static final String COORDINATING = "coordinating";
    private static final String COORDINATING_IN_BYTES = "coordinating_in_bytes";
    private static final String PRIMARY = "primary";
    private static final String PRIMARY_IN_BYTES = "primary_in_bytes";
    private static final String REPLICA = "replica";
    private static final String REPLICA_IN_BYTES = "replica_in_bytes";
    private static final String ALL = "all";
    private static final String ALL_IN_BYTES = "all_in_bytes";
    private static final String COORDINATING_REJECTIONS = "coordinating_rejections";
    private static final String PRIMARY_REJECTIONS = "primary_rejections";
    private static final String REPLICA_REJECTIONS = "replica_rejections";

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("indexing_pressure");
        builder.startObject("total");
        builder.startObject("memory");
        builder.humanReadableField(COMBINED_IN_BYTES, COMBINED, new ByteSizeValue(totalCombinedCoordinatingAndPrimaryBytes));
        builder.humanReadableField(COORDINATING_IN_BYTES, COORDINATING, new ByteSizeValue(totalCoordinatingBytes));
        builder.humanReadableField(PRIMARY_IN_BYTES, PRIMARY, new ByteSizeValue(totalPrimaryBytes));
        builder.humanReadableField(REPLICA_IN_BYTES, REPLICA, new ByteSizeValue(totalReplicaBytes));
        builder.humanReadableField(ALL_IN_BYTES, ALL, new ByteSizeValue(totalReplicaBytes + totalCombinedCoordinatingAndPrimaryBytes));
        builder.endObject();
        builder.field(COORDINATING_REJECTIONS, coordinatingRejections);
        builder.field(PRIMARY_REJECTIONS, primaryRejections);
        builder.field(REPLICA_REJECTIONS, replicaRejections);
        builder.endObject();
        builder.startObject("current");
        builder.startObject("memory");
        builder.humanReadableField(COMBINED_IN_BYTES, COMBINED, new ByteSizeValue(currentCombinedCoordinatingAndPrimaryBytes));
        builder.humanReadableField(COORDINATING_IN_BYTES, COORDINATING, new ByteSizeValue(currentCoordinatingBytes));
        builder.humanReadableField(PRIMARY_IN_BYTES, PRIMARY, new ByteSizeValue(currentPrimaryBytes));
        builder.humanReadableField(REPLICA_IN_BYTES, REPLICA, new ByteSizeValue(currentReplicaBytes));
        builder.humanReadableField(ALL_IN_BYTES, ALL, new ByteSizeValue(currentReplicaBytes + currentCombinedCoordinatingAndPrimaryBytes));
        builder.endObject();
        builder.endObject();
        return builder.endObject();
    }
}

