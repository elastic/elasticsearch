/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots.action;

import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.Index;
import org.elasticsearch.xpack.core.searchablesnapshots.SearchableSnapshotStats;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

public class SearchableSnapshotsStatsResponse extends BroadcastResponse {

    private List<SearchableSnapshotStats> stats;

    SearchableSnapshotsStatsResponse(StreamInput in) throws IOException {
        super(in);
        this.stats = in.readList(SearchableSnapshotStats::new);
    }

    SearchableSnapshotsStatsResponse(List<SearchableSnapshotStats> stats, int totalShards, int successfulShards, int failedShards,
                                     List<DefaultShardOperationFailedException> shardFailures) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.stats = Objects.requireNonNull(stats);
    }

    public List<SearchableSnapshotStats> getStats() {
        return stats;
    }

    @Override
    protected void addCustomXContentFields(XContentBuilder builder, Params params) throws IOException {
        final List<Index> indices = getStats().stream()
            .filter(stats -> stats.getStats().isEmpty() == false)
            .map(SearchableSnapshotStats::getShardRouting)
            .map(ShardRouting::index)
            .sorted(Comparator.comparing(Index::getName))
            .collect(toList());

        builder.startObject("indices");
        for (Index index : indices) {
            builder.startObject(index.getName());
            {
                builder.startObject("shards");
                {
                    List<SearchableSnapshotStats> listOfStats = getStats().stream()
                        .filter(dirStats -> dirStats.getShardRouting().index().equals(index))
                        .sorted(Comparator.comparingInt(dir -> dir.getShardRouting().getId()))
                        .collect(Collectors.toList());

                    int minShard = listOfStats.stream().map(stat -> stat.getShardRouting().getId()).min(Integer::compareTo).orElse(0);
                    int maxShard = listOfStats.stream().map(stat -> stat.getShardRouting().getId()).max(Integer::compareTo).orElse(0);

                    for (int i = minShard; i <= maxShard; i++) {
                        builder.startArray(Integer.toString(i));
                        for (SearchableSnapshotStats stat : listOfStats) {
                            if (stat.getShardRouting().getId() == i) {
                                stat.toXContent(builder, params);
                            }
                        }
                        builder.endArray();
                    }
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();
    }
}
