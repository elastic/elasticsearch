/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.stats;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

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
    private final long primaryDocumentRejections;
    private final long memoryLimit;

    /* Count number of splits due to SPLIT_BULK_LOW_WATERMARK and SPLIT_BULK_HIGH_WATERMARK
       These 2 stats are not serialized via X content yet.
     */
    private final long lowWaterMarkSplits;
    private final long highWaterMarkSplits;
    private final long largeOpsRejections;
    private final long totalLargeRejectedOpsBytes;

    // These fields will be used for additional back-pressure and metrics in the future
    private final long totalCoordinatingOps;
    private final long totalCoordinatingRequests;
    private final long totalPrimaryOps;
    private final long totalReplicaOps;
    private final long currentCoordinatingOps;
    private final long currentPrimaryOps;
    private final long currentReplicaOps;

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

        memoryLimit = in.readVLong();

        // These are not currently propagated across the network yet
        this.totalCoordinatingOps = 0;
        this.totalPrimaryOps = 0;
        this.totalReplicaOps = 0;
        this.currentCoordinatingOps = 0;
        this.currentPrimaryOps = 0;
        this.currentReplicaOps = 0;

        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0)) {
            primaryDocumentRejections = in.readVLong();
        } else {
            primaryDocumentRejections = -1L;
        }

        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
            totalCoordinatingRequests = in.readVLong();
        } else {
            totalCoordinatingRequests = -1L;
        }

        if (in.getTransportVersion().onOrAfter(TransportVersions.INDEXING_PRESSURE_THROTTLING_STATS)) {
            lowWaterMarkSplits = in.readVLong();
            highWaterMarkSplits = in.readVLong();
        } else {
            lowWaterMarkSplits = -1L;
            highWaterMarkSplits = -1L;
        }

        if (in.getTransportVersion().onOrAfter(TransportVersions.MAX_OPERATION_SIZE_REJECTIONS_ADDED)) {
            largeOpsRejections = in.readVLong();
            totalLargeRejectedOpsBytes = in.readVLong();
        } else {
            largeOpsRejections = -1L;
            totalLargeRejectedOpsBytes = -1L;
        }
    }

    public IndexingPressureStats(
        long totalCombinedCoordinatingAndPrimaryBytes,
        long totalCoordinatingBytes,
        long totalPrimaryBytes,
        long totalReplicaBytes,
        long currentCombinedCoordinatingAndPrimaryBytes,
        long currentCoordinatingBytes,
        long currentPrimaryBytes,
        long currentReplicaBytes,
        long coordinatingRejections,
        long primaryRejections,
        long replicaRejections,
        long memoryLimit,
        long totalCoordinatingOps,
        long totalPrimaryOps,
        long totalReplicaOps,
        long currentCoordinatingOps,
        long currentPrimaryOps,
        long currentReplicaOps,
        long primaryDocumentRejections,
        long totalCoordinatingRequests,
        long lowWaterMarkSplits,
        long highWaterMarkSplits,
        long largeOpsRejections,
        long totalRejectedLargeOpsBytes
    ) {
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
        this.memoryLimit = memoryLimit;

        this.totalCoordinatingOps = totalCoordinatingOps;
        this.totalPrimaryOps = totalPrimaryOps;
        this.totalReplicaOps = totalReplicaOps;
        this.currentCoordinatingOps = currentCoordinatingOps;
        this.currentPrimaryOps = currentPrimaryOps;
        this.currentReplicaOps = currentReplicaOps;

        this.primaryDocumentRejections = primaryDocumentRejections;
        this.totalCoordinatingRequests = totalCoordinatingRequests;

        this.lowWaterMarkSplits = lowWaterMarkSplits;
        this.highWaterMarkSplits = highWaterMarkSplits;
        this.largeOpsRejections = largeOpsRejections;
        this.totalLargeRejectedOpsBytes = totalRejectedLargeOpsBytes;
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

        out.writeVLong(memoryLimit);

        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0)) {
            out.writeVLong(primaryDocumentRejections);
        }

        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
            out.writeVLong(totalCoordinatingRequests);
        }

        if (out.getTransportVersion().onOrAfter(TransportVersions.INDEXING_PRESSURE_THROTTLING_STATS)) {
            out.writeVLong(lowWaterMarkSplits);
            out.writeVLong(highWaterMarkSplits);
        }

        if (out.getTransportVersion().onOrAfter(TransportVersions.MAX_OPERATION_SIZE_REJECTIONS_ADDED)) {
            out.writeVLong(largeOpsRejections);
            out.writeVLong(totalLargeRejectedOpsBytes);
        }
    }

    public long getTotalCombinedCoordinatingAndPrimaryBytes() {
        return totalCombinedCoordinatingAndPrimaryBytes;
    }

    public long getTotalCoordinatingBytes() {
        return totalCoordinatingBytes;
    }

    public long getTotalPrimaryBytes() {
        return totalPrimaryBytes;
    }

    public long getTotalReplicaBytes() {
        return totalReplicaBytes;
    }

    public long getCurrentCombinedCoordinatingAndPrimaryBytes() {
        return currentCombinedCoordinatingAndPrimaryBytes;
    }

    public long getCurrentCoordinatingBytes() {
        return currentCoordinatingBytes;
    }

    public long getCurrentPrimaryBytes() {
        return currentPrimaryBytes;
    }

    public long getCurrentReplicaBytes() {
        return currentReplicaBytes;
    }

    public long getCoordinatingRejections() {
        return coordinatingRejections;
    }

    public long getPrimaryRejections() {
        return primaryRejections;
    }

    public long getReplicaRejections() {
        return replicaRejections;
    }

    public long getTotalCoordinatingOps() {
        return totalCoordinatingOps;
    }

    public long getTotalPrimaryOps() {
        return totalPrimaryOps;
    }

    public long getTotalReplicaOps() {
        return totalReplicaOps;
    }

    public long getCurrentCoordinatingOps() {
        return currentCoordinatingOps;
    }

    public long getCurrentPrimaryOps() {
        return currentPrimaryOps;
    }

    public long getCurrentReplicaOps() {
        return currentReplicaOps;
    }

    public long getMemoryLimit() {
        return memoryLimit;
    }

    public long getPrimaryDocumentRejections() {
        return primaryDocumentRejections;
    }

    public long getTotalCoordinatingRequests() {
        return totalCoordinatingRequests;
    }

    public long getHighWaterMarkSplits() {
        return highWaterMarkSplits;
    }

    public long getLowWaterMarkSplits() {
        return lowWaterMarkSplits;
    }

    public long getLargeOpsRejections() {
        return largeOpsRejections;
    }

    public long getTotalLargeRejectedOpsBytes() {
        return totalLargeRejectedOpsBytes;
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
    private static final String PRIMARY_DOCUMENT_REJECTIONS = "primary_document_rejections";
    private static final String LIMIT = "limit";
    private static final String LIMIT_IN_BYTES = "limit_in_bytes";
    private static final String LARGE_OPERATION_REJECTIONS = "large_operation_rejections";

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("indexing_pressure");
        builder.startObject("memory");
        builder.startObject("current");
        builder.humanReadableField(COMBINED_IN_BYTES, COMBINED, ByteSizeValue.ofBytes(currentCombinedCoordinatingAndPrimaryBytes));
        builder.humanReadableField(COORDINATING_IN_BYTES, COORDINATING, ByteSizeValue.ofBytes(currentCoordinatingBytes));
        builder.humanReadableField(PRIMARY_IN_BYTES, PRIMARY, ByteSizeValue.ofBytes(currentPrimaryBytes));
        builder.humanReadableField(REPLICA_IN_BYTES, REPLICA, ByteSizeValue.ofBytes(currentReplicaBytes));
        builder.humanReadableField(
            ALL_IN_BYTES,
            ALL,
            ByteSizeValue.ofBytes(currentReplicaBytes + currentCombinedCoordinatingAndPrimaryBytes)
        );
        builder.endObject();
        builder.startObject("total");
        builder.humanReadableField(COMBINED_IN_BYTES, COMBINED, ByteSizeValue.ofBytes(totalCombinedCoordinatingAndPrimaryBytes));
        builder.humanReadableField(COORDINATING_IN_BYTES, COORDINATING, ByteSizeValue.ofBytes(totalCoordinatingBytes));
        builder.humanReadableField(PRIMARY_IN_BYTES, PRIMARY, ByteSizeValue.ofBytes(totalPrimaryBytes));
        builder.humanReadableField(REPLICA_IN_BYTES, REPLICA, ByteSizeValue.ofBytes(totalReplicaBytes));
        builder.humanReadableField(ALL_IN_BYTES, ALL, ByteSizeValue.ofBytes(totalReplicaBytes + totalCombinedCoordinatingAndPrimaryBytes));
        builder.field(COORDINATING_REJECTIONS, coordinatingRejections);
        builder.field(PRIMARY_REJECTIONS, primaryRejections);
        builder.field(REPLICA_REJECTIONS, replicaRejections);
        builder.field(PRIMARY_DOCUMENT_REJECTIONS, primaryDocumentRejections);
        builder.field(LARGE_OPERATION_REJECTIONS, largeOpsRejections);
        builder.endObject();
        builder.humanReadableField(LIMIT_IN_BYTES, LIMIT, ByteSizeValue.ofBytes(memoryLimit));
        builder.endObject();
        return builder.endObject();
    }
}
