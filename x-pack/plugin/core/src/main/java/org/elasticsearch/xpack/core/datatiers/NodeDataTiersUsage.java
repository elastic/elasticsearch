/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.datatiers;

import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Data tier usage statistics on a specific node. The statistics groups the indices, shard sizes, shard counts based
 * on their tier preference.
 */
public class NodeDataTiersUsage extends BaseNodeResponse {

    private final Map<String, UsageStats> usageStatsByTier;

    public static class UsageStats implements Writeable {
        private final List<Long> primaryShardSizes;
        private int totalShardCount;
        private long docCount;
        private long totalSize;

        public UsageStats() {
            this.primaryShardSizes = new ArrayList<>();
            this.totalShardCount = 0;
            this.docCount = 0;
            this.totalSize = 0;
        }

        public UsageStats(List<Long> primaryShardSizes, int totalShardCount, long docCount, long totalSize) {
            this.primaryShardSizes = primaryShardSizes;
            this.totalShardCount = totalShardCount;
            this.docCount = docCount;
            this.totalSize = totalSize;
        }

        static UsageStats read(StreamInput in) throws IOException {
            return new UsageStats(in.readCollectionAsList(StreamInput::readVLong), in.readVInt(), in.readVLong(), in.readVLong());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(primaryShardSizes, StreamOutput::writeVLong);
            out.writeVInt(totalShardCount);
            out.writeVLong(docCount);
            out.writeVLong(totalSize);
        }

        public void addPrimaryShardSize(long primaryShardSize) {
            primaryShardSizes.add(primaryShardSize);
        }

        public void incrementTotalSize(long totalSize) {
            this.totalSize += totalSize;
        }

        public void incrementDocCount(long docCount) {
            this.docCount += docCount;
        }

        public void incrementTotalShardCount(int totalShardCount) {
            this.totalShardCount += totalShardCount;
        }

        public List<Long> getPrimaryShardSizes() {
            return primaryShardSizes;
        }

        public int getTotalShardCount() {
            return totalShardCount;
        }

        public long getDocCount() {
            return docCount;
        }

        public long getTotalSize() {
            return totalSize;
        }
    }

    public NodeDataTiersUsage(StreamInput in) throws IOException {
        super(in);
        usageStatsByTier = in.readMap(UsageStats::read);
    }

    public NodeDataTiersUsage(DiscoveryNode node, Map<String, UsageStats> usageStatsByTier) {
        super(node);
        this.usageStatsByTier = usageStatsByTier;
    }

    public Map<String, UsageStats> getUsageStatsByTier() {
        return Map.copyOf(usageStatsByTier);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeMap(usageStatsByTier, StreamOutput::writeWriteable);
    }
}
