/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.action.admin.indices.stats.IndexShardStats;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

public record IndexMetadataStats(IndexWriteLoad indexWriteLoad, AverageShardSize averageShardSize)
    implements
        Writeable,
        ToXContentFragment {

    public static final ParseField WRITE_LOAD_FIELD = new ParseField("write_load");
    public static final ParseField AVERAGE_SIZE_FIELD = new ParseField("avg_size");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<IndexMetadataStats, Void> PARSER = new ConstructingObjectParser<>(
        "index_metadata_stats_parser",
        false,
        (args, unused) -> new IndexMetadataStats((IndexWriteLoad) args[0], (AverageShardSize) args[1])
    );

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> IndexWriteLoad.fromXContent(p), WRITE_LOAD_FIELD);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> AverageShardSize.fromXContent(p), AVERAGE_SIZE_FIELD);
    }

    // Visible for testing
    public IndexMetadataStats(IndexWriteLoad indexWriteLoad, long totalSizeInBytes, int numberOfShards) {
        this(indexWriteLoad, new AverageShardSize(totalSizeInBytes, numberOfShards));
    }

    public IndexMetadataStats(StreamInput in) throws IOException {
        this(new IndexWriteLoad(in), new AverageShardSize(in));
    }

    public IndexMetadataStats {
        Objects.requireNonNull(indexWriteLoad, "Expected a non null index write load");
        Objects.requireNonNull(averageShardSize, "Expected a non null average shard size");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        indexWriteLoad.writeTo(out);
        averageShardSize.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(WRITE_LOAD_FIELD.getPreferredName());
        indexWriteLoad.toXContent(builder, params);
        builder.endObject();

        builder.startObject(AVERAGE_SIZE_FIELD.getPreferredName());
        averageShardSize.toXContent(builder, params);
        builder.endObject();
        return builder;
    }

    public static IndexMetadataStats fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Nullable
    public static IndexMetadataStats fromStatsResponse(IndexMetadata indexMetadata, @Nullable IndicesStatsResponse indicesStatsResponse) {
        if (indicesStatsResponse == null) {
            return null;
        }
        final IndexStats indexStats = indicesStatsResponse.getIndex(indexMetadata.getIndex().getName());
        if (indexStats == null) {
            return null;
        }

        long totalSizeInBytes = 0;
        int shardsTookIntoAccountForSizeAvg = 0;
        final int numberOfShards = indexMetadata.getNumberOfShards();
        final var indexWriteLoadBuilder = IndexWriteLoad.builder(numberOfShards);
        final var indexShards = indexStats.getIndexShards();
        for (IndexShardStats indexShardsStats : indexShards.values()) {
            final var shardStats = Arrays.stream(indexShardsStats.getShards())
                .filter(stats -> stats.getShardRouting().primary())
                .findFirst()
                // Fallback to a replica if for some reason we couldn't find the primary stats
                .orElse(indexShardsStats.getAt(0));
            final var commonStats = shardStats.getStats();
            final var indexingShardStats = commonStats.getIndexing().getTotal();
            indexWriteLoadBuilder.withShardWriteLoad(
                shardStats.getShardRouting().id(),
                indexingShardStats.getWriteLoad(),
                indexingShardStats.getRecentWriteLoad(),
                indexingShardStats.getPeakWriteLoad(),
                indexingShardStats.getTotalActiveTimeInMillis()
            );
            totalSizeInBytes += commonStats.getDocs().getTotalSizeInBytes();
            shardsTookIntoAccountForSizeAvg++;
        }

        return new IndexMetadataStats(
            indexWriteLoadBuilder.build(),
            new AverageShardSize(totalSizeInBytes, shardsTookIntoAccountForSizeAvg)
        );
    }

    public IndexWriteLoad writeLoad() {
        return indexWriteLoad;
    }

    public record AverageShardSize(long totalSizeInBytes, int numberOfShards) implements Writeable, ToXContentFragment {

        public static final ParseField TOTAL_SIZE_IN_BYTES_FIELD = new ParseField("total_size_in_bytes");
        public static final ParseField SHARD_COUNT_FIELD = new ParseField("shard_count");

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<AverageShardSize, Void> PARSER = new ConstructingObjectParser<>(
            "average_shard_size",
            false,
            (args, unused) -> new AverageShardSize((long) args[0], (int) args[1])
        );

        static {
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), TOTAL_SIZE_IN_BYTES_FIELD);
            PARSER.declareInt(ConstructingObjectParser.constructorArg(), SHARD_COUNT_FIELD);
        }

        public AverageShardSize {
            assert numberOfShards > 0;
        }

        AverageShardSize(StreamInput in) throws IOException {
            this(in.readLong(), in.readInt());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(totalSizeInBytes);
            out.writeInt(numberOfShards);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(TOTAL_SIZE_IN_BYTES_FIELD.getPreferredName(), totalSizeInBytes);
            builder.field(SHARD_COUNT_FIELD.getPreferredName(), numberOfShards);
            return builder;
        }

        static AverageShardSize fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        public long getAverageSizeInBytes() {
            return totalSizeInBytes / numberOfShards;
        }
    }
}
