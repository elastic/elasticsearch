/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.searchablesnapshots.action;

import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.Index;
import org.elasticsearch.xpack.core.searchablesnapshots.SearchableSnapshotShardStats;
import org.elasticsearch.xpack.core.searchablesnapshots.SearchableSnapshotShardStats.CacheIndexInputStats;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

public class SearchableSnapshotsStatsResponse extends BroadcastResponse {

    private List<SearchableSnapshotShardStats> stats;
    private volatile List<CacheIndexInputStats> total;

    SearchableSnapshotsStatsResponse(StreamInput in) throws IOException {
        super(in);
        this.stats = in.readList(SearchableSnapshotShardStats::new);
    }

    SearchableSnapshotsStatsResponse(
        List<SearchableSnapshotShardStats> stats,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<DefaultShardOperationFailedException> shardFailures
    ) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.stats = Objects.requireNonNull(stats);
    }

    public List<SearchableSnapshotShardStats> getStats() {
        return stats;
    }

    private List<CacheIndexInputStats> getTotal() {
        if (total != null) {
            return total;
        }
        return total = computeCompound(getStats().stream().flatMap(stat -> stat.getStats().stream()));
    }

    private static List<CacheIndexInputStats> computeCompound(Stream<CacheIndexInputStats> statsStream) {
        return statsStream.collect(groupingBy(CacheIndexInputStats::getFileExt, Collectors.reducing(CacheIndexInputStats::combine)))
            .values()
            .stream()
            .filter(o -> o.isEmpty() == false)
            .map(Optional::get)
            .sorted(Comparator.comparing(CacheIndexInputStats::getFileExt))
            .collect(toList());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeList(stats);
    }

    @Override
    protected void addCustomXContentFields(XContentBuilder builder, Params params) throws IOException {
        final String level = params.param("level", "indices");
        final boolean isLevelValid = "cluster".equalsIgnoreCase(level)
            || "indices".equalsIgnoreCase(level)
            || "shards".equalsIgnoreCase(level);
        if (isLevelValid == false) {
            throw new IllegalArgumentException("level parameter must be one of [cluster] or [indices] or [shards] but was [" + level + "]");
        }

        builder.startArray("total");
        for (CacheIndexInputStats cis : getTotal()) {
            cis.toXContent(builder, params);
        }
        builder.endArray();

        if ("indices".equalsIgnoreCase(level) || "shards".equalsIgnoreCase(level)) {
            builder.startObject("indices");
            final List<Index> indices = getStats().stream()
                .filter(stats -> stats.getStats().isEmpty() == false)
                .map(SearchableSnapshotShardStats::getShardRouting)
                .map(ShardRouting::index)
                .sorted(Comparator.comparing(Index::getName))
                .distinct()
                .collect(toList());

            for (Index index : indices) {
                builder.startObject(index.getName());
                {
                    builder.startArray("total");
                    List<CacheIndexInputStats> indexStats = computeCompound(
                        getStats().stream()
                            .filter(dirStats -> dirStats.getShardRouting().index().equals(index))
                            .flatMap(dirStats -> dirStats.getStats().stream())
                    );

                    for (CacheIndexInputStats cis : indexStats) {
                        cis.toXContent(builder, params);
                    }
                    builder.endArray();

                    if ("shards".equalsIgnoreCase(level)) {
                        builder.startObject("shards");
                        {
                            List<SearchableSnapshotShardStats> listOfStats = getStats().stream()
                                .filter(dirStats -> dirStats.getShardRouting().index().equals(index))
                                .sorted(Comparator.comparingInt(dir -> dir.getShardRouting().getId()))
                                .collect(Collectors.toList());

                            int minShard = listOfStats.stream()
                                .map(stat -> stat.getShardRouting().getId())
                                .min(Integer::compareTo)
                                .orElse(0);
                            int maxShard = listOfStats.stream()
                                .map(stat -> stat.getShardRouting().getId())
                                .max(Integer::compareTo)
                                .orElse(0);

                            for (int i = minShard; i <= maxShard; i++) {
                                builder.startArray(Integer.toString(i));
                                for (SearchableSnapshotShardStats stat : listOfStats) {
                                    if (stat.getShardRouting().getId() == i) {
                                        stat.toXContent(builder, params);
                                    }
                                }
                                builder.endArray();
                            }
                        }
                        builder.endObject();
                    }
                }
                builder.endObject();
            }
            builder.endObject();
        }
    }
}
