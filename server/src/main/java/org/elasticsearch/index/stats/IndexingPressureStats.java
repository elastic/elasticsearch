/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.stats;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public class IndexingPressureStats implements Writeable, ToXContentFragment {

    private static final TransportVersion MAX_OPERATION_SIZE_REJECTIONS_ADDED = TransportVersion.fromName(
        "max_operation_size_rejections_added"
    );

    private static final TransportVersion INGEST_EXPANSION_STATS_ADDED = TransportVersion.fromName(
        "indexing_pressure_ingest_expansion_stats"
    );

    private static final TransportVersion REINDEX_PAGE_STATS_ADDED = TransportVersion.fromName("reindex_page_indexing_pressure_stats");

    // For a reindex page the coordinating totals hold the estimate the page reserved, not its real bytes. The real bytes are the total
    // minus reindexPageBytesBelowEstimate plus reindexPageBytesAboveEstimate. The coordinating ops total includes the final empty page.
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

    // Bytes by which ingest pipelines grew documents of in-flight bulks, only the growth and not the full size of the grown documents.
    // These are part of the coordinating and combined bytes above.
    private final long currentIngestExpansionBytes;
    private final long totalIngestExpansionBytes;
    // Rejections of ingest expansion. These are also counted in coordinatingRejections.
    private final long ingestExpansionRejections;

    // A reindex page reserve that did not fit. Not counted in coordinatingRejections, because no client sees a 429 for it.
    private final long reindexPageReserveRejections;
    // Bytes a built reindex page gave back, or took on, against the estimate it reserved.
    private final long reindexPageBytesBelowEstimate;
    private final long reindexPageBytesAboveEstimate;
    // A built reindex page larger than the coordinating limit. Also counted in coordinatingRejections.
    private final long reindexPageExceedsLimit;

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

        primaryDocumentRejections = in.readVLong();

        totalCoordinatingRequests = in.readVLong();

        lowWaterMarkSplits = in.readVLong();
        highWaterMarkSplits = in.readVLong();

        if (in.getTransportVersion().supports(MAX_OPERATION_SIZE_REJECTIONS_ADDED)) {
            largeOpsRejections = in.readVLong();
            totalLargeRejectedOpsBytes = in.readVLong();
        } else {
            largeOpsRejections = -1L;
            totalLargeRejectedOpsBytes = -1L;
        }

        if (in.getTransportVersion().supports(INGEST_EXPANSION_STATS_ADDED)) {
            currentIngestExpansionBytes = in.readVLong();
            totalIngestExpansionBytes = in.readVLong();
            ingestExpansionRejections = in.readVLong();
        } else {
            currentIngestExpansionBytes = 0L;
            totalIngestExpansionBytes = 0L;
            ingestExpansionRejections = 0L;
        }

        if (in.getTransportVersion().supports(REINDEX_PAGE_STATS_ADDED)) {
            reindexPageReserveRejections = in.readVLong();
            reindexPageBytesBelowEstimate = in.readVLong();
            reindexPageBytesAboveEstimate = in.readVLong();
            reindexPageExceedsLimit = in.readVLong();
        } else {
            reindexPageReserveRejections = 0L;
            reindexPageBytesBelowEstimate = 0L;
            reindexPageBytesAboveEstimate = 0L;
            reindexPageExceedsLimit = 0L;
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
        long totalRejectedLargeOpsBytes,
        long currentIngestExpansionBytes,
        long totalIngestExpansionBytes,
        long ingestExpansionRejections,
        long reindexPageReserveRejections,
        long reindexPageBytesBelowEstimate,
        long reindexPageBytesAboveEstimate,
        long reindexPageExceedsLimit
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
        this.currentIngestExpansionBytes = currentIngestExpansionBytes;
        this.totalIngestExpansionBytes = totalIngestExpansionBytes;
        this.ingestExpansionRejections = ingestExpansionRejections;
        this.reindexPageReserveRejections = reindexPageReserveRejections;
        this.reindexPageBytesBelowEstimate = reindexPageBytesBelowEstimate;
        this.reindexPageBytesAboveEstimate = reindexPageBytesAboveEstimate;
        this.reindexPageExceedsLimit = reindexPageExceedsLimit;
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

        out.writeVLong(primaryDocumentRejections);

        out.writeVLong(totalCoordinatingRequests);

        out.writeVLong(lowWaterMarkSplits);
        out.writeVLong(highWaterMarkSplits);

        if (out.getTransportVersion().supports(MAX_OPERATION_SIZE_REJECTIONS_ADDED)) {
            out.writeVLong(largeOpsRejections);
            out.writeVLong(totalLargeRejectedOpsBytes);
        }

        if (out.getTransportVersion().supports(INGEST_EXPANSION_STATS_ADDED)) {
            out.writeVLong(currentIngestExpansionBytes);
            out.writeVLong(totalIngestExpansionBytes);
            out.writeVLong(ingestExpansionRejections);
        }

        if (out.getTransportVersion().supports(REINDEX_PAGE_STATS_ADDED)) {
            out.writeVLong(reindexPageReserveRejections);
            out.writeVLong(reindexPageBytesBelowEstimate);
            out.writeVLong(reindexPageBytesAboveEstimate);
            out.writeVLong(reindexPageExceedsLimit);
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

    public long getCurrentIngestExpansionBytes() {
        return currentIngestExpansionBytes;
    }

    public long getTotalIngestExpansionBytes() {
        return totalIngestExpansionBytes;
    }

    public long getIngestExpansionRejections() {
        return ingestExpansionRejections;
    }

    public long getReindexPageReserveRejections() {
        return reindexPageReserveRejections;
    }

    public long getReindexPageBytesBelowEstimate() {
        return reindexPageBytesBelowEstimate;
    }

    public long getReindexPageBytesAboveEstimate() {
        return reindexPageBytesAboveEstimate;
    }

    public long getReindexPageExceedsLimit() {
        return reindexPageExceedsLimit;
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
    private static final String INGEST_EXPANSION = "ingest_expansion";
    private static final String INGEST_EXPANSION_IN_BYTES = "ingest_expansion_in_bytes";
    private static final String INGEST_EXPANSION_REJECTIONS = "ingest_expansion_rejections";
    private static final String REINDEX_PAGE_RESERVE_REJECTIONS = "reindex_page_reserve_rejections";
    private static final String REINDEX_PAGE_BYTES_BELOW_ESTIMATE = "reindex_page_bytes_below_estimate";
    private static final String REINDEX_PAGE_BYTES_BELOW_ESTIMATE_IN_BYTES = "reindex_page_bytes_below_estimate_in_bytes";
    private static final String REINDEX_PAGE_BYTES_ABOVE_ESTIMATE = "reindex_page_bytes_above_estimate";
    private static final String REINDEX_PAGE_BYTES_ABOVE_ESTIMATE_IN_BYTES = "reindex_page_bytes_above_estimate_in_bytes";
    private static final String REINDEX_PAGE_EXCEEDS_LIMIT = "reindex_page_exceeds_limit";

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
        builder.humanReadableField(INGEST_EXPANSION_IN_BYTES, INGEST_EXPANSION, ByteSizeValue.ofBytes(currentIngestExpansionBytes));
        builder.endObject();
        builder.startObject("total");
        builder.humanReadableField(COMBINED_IN_BYTES, COMBINED, ByteSizeValue.ofBytes(totalCombinedCoordinatingAndPrimaryBytes));
        builder.humanReadableField(COORDINATING_IN_BYTES, COORDINATING, ByteSizeValue.ofBytes(totalCoordinatingBytes));
        builder.humanReadableField(PRIMARY_IN_BYTES, PRIMARY, ByteSizeValue.ofBytes(totalPrimaryBytes));
        builder.humanReadableField(REPLICA_IN_BYTES, REPLICA, ByteSizeValue.ofBytes(totalReplicaBytes));
        builder.humanReadableField(ALL_IN_BYTES, ALL, ByteSizeValue.ofBytes(totalReplicaBytes + totalCombinedCoordinatingAndPrimaryBytes));
        builder.humanReadableField(INGEST_EXPANSION_IN_BYTES, INGEST_EXPANSION, ByteSizeValue.ofBytes(totalIngestExpansionBytes));
        builder.field(COORDINATING_REJECTIONS, coordinatingRejections);
        builder.field(PRIMARY_REJECTIONS, primaryRejections);
        builder.field(REPLICA_REJECTIONS, replicaRejections);
        builder.field(PRIMARY_DOCUMENT_REJECTIONS, primaryDocumentRejections);
        builder.field(LARGE_OPERATION_REJECTIONS, largeOpsRejections);
        builder.field(INGEST_EXPANSION_REJECTIONS, ingestExpansionRejections);
        builder.field(REINDEX_PAGE_RESERVE_REJECTIONS, reindexPageReserveRejections);
        builder.humanReadableField(
            REINDEX_PAGE_BYTES_BELOW_ESTIMATE_IN_BYTES,
            REINDEX_PAGE_BYTES_BELOW_ESTIMATE,
            ByteSizeValue.ofBytes(reindexPageBytesBelowEstimate)
        );
        builder.humanReadableField(
            REINDEX_PAGE_BYTES_ABOVE_ESTIMATE_IN_BYTES,
            REINDEX_PAGE_BYTES_ABOVE_ESTIMATE,
            ByteSizeValue.ofBytes(reindexPageBytesAboveEstimate)
        );
        builder.field(REINDEX_PAGE_EXCEEDS_LIMIT, reindexPageExceedsLimit);
        builder.endObject();
        builder.humanReadableField(LIMIT_IN_BYTES, LIMIT, ByteSizeValue.ofBytes(memoryLimit));
        builder.endObject();
        return builder.endObject();
    }
}
