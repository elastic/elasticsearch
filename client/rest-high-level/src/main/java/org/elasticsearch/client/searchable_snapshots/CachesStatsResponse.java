/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.searchable_snapshots;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.util.List;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class CachesStatsResponse {

    private final List<NodeCachesStats> nodeCachesStats;

    private CachesStatsResponse(List<NodeCachesStats> nodeCachesStats) {
        this.nodeCachesStats = nodeCachesStats != null ? nodeCachesStats : List.of();
    }

    public List<NodeCachesStats> getNodeCachesStats() {
        return nodeCachesStats;
    }

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<CachesStatsResponse, Void> PARSER = new ConstructingObjectParser<>(
        "caches_stats_response", true, args -> new CachesStatsResponse((List<NodeCachesStats>) args[0]));
    static {
        PARSER.declareNamedObjects(constructorArg(), (p, c, nodeId) -> NodeCachesStats.PARSER.apply(p, nodeId), new ParseField("nodes"));
    }

    public static CachesStatsResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public static class NodeCachesStats {

        private final String nodeId;
        private final SharedCacheStats sharedCacheStats;

        public NodeCachesStats(String nodeId, SharedCacheStats sharedCacheStats) {
            this.nodeId = nodeId;
            this.sharedCacheStats = sharedCacheStats;
        }

        public String getNodeId() {
            return nodeId;
        }

        public SharedCacheStats getSharedCacheStats() {
            return sharedCacheStats;
        }

        private static final ConstructingObjectParser<NodeCachesStats, String> PARSER = new ConstructingObjectParser<>(
            "node_caches_stats", true, (args, nodeId) -> new NodeCachesStats(nodeId, (SharedCacheStats) args[0]));
        static {
            PARSER.declareObject(constructorArg(), (p, c) -> SharedCacheStats.fromXContent(p), new ParseField("shared_cache"));
        }

        public static NodeCachesStats fromXContent(XContentParser parser, String nodeId) {
            return PARSER.apply(parser, nodeId);
        }
    }

    public static class SharedCacheStats {

        private final int numRegions;
        private final long size;
        private final long regionSize;
        private final long writes;
        private final long bytesWritten;
        private final long reads;
        private final long bytesRead;
        private final long evictions;

        SharedCacheStats(
            int numRegions,
            long size,
            long regionSize,
            long writes,
            long bytesWritten,
            long reads,
            long bytesRead,
            long evictions
        ) {
            this.numRegions = numRegions;
            this.size = size;
            this.regionSize = regionSize;
            this.writes = writes;
            this.bytesWritten = bytesWritten;
            this.reads = reads;
            this.bytesRead = bytesRead;
            this.evictions = evictions;
        }

        public int getNumRegions() {
            return numRegions;
        }

        public long getSize() {
            return size;
        }

        public long getRegionSize() {
            return regionSize;
        }

        public long getWrites() {
            return writes;
        }

        public long getBytesWritten() {
            return bytesWritten;
        }

        public long getReads() {
            return reads;
        }

        public long getBytesRead() {
            return bytesRead;
        }

        public long getEvictions() {
            return evictions;
        }

        private static final ConstructingObjectParser<SharedCacheStats, Void> PARSER = new ConstructingObjectParser<>(
            "shared_cache_stats",
            true,
            args -> new SharedCacheStats(
                (int) args[0],
                (long) args[1],
                (long) args[2],
                (long) args[3],
                (long) args[4],
                (long) args[5],
                (long) args[6],
                (long) args[7]
            )
        );
        static {
            PARSER.declareInt(constructorArg(), new ParseField("num_regions"));
            PARSER.declareLong(constructorArg(), new ParseField("size_in_bytes"));
            PARSER.declareLong(constructorArg(), new ParseField("region_size_in_bytes"));
            PARSER.declareLong(constructorArg(), new ParseField("writes"));
            PARSER.declareLong(constructorArg(), new ParseField("bytes_written_in_bytes"));
            PARSER.declareLong(constructorArg(), new ParseField("reads"));
            PARSER.declareLong(constructorArg(), new ParseField("bytes_read_in_bytes"));
            PARSER.declareLong(constructorArg(), new ParseField("evictions"));
        }

        public static SharedCacheStats fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        @Override
        public String toString() {
            return "SharedCacheStats{" +
                "numRegions=" + numRegions +
                ", size=" + size +
                ", regionSize=" + regionSize +
                ", writes=" + writes +
                ", bytesWritten=" + bytesWritten +
                ", reads=" + reads +
                ", bytesRead=" + bytesRead +
                ", evictions=" + evictions +
                '}';
        }
    }
}
