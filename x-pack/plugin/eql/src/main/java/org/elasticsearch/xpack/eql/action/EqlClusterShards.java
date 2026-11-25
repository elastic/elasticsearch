/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.action;

import org.elasticsearch.action.search.OpenPointInTimeResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.transport.RemoteClusterAware;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public record EqlClusterShards(String clusterAlias, int total, int successful, int skipped, int failed) implements Writeable {

    public EqlClusterShards(StreamInput in) throws IOException {
        this(in.readString(), in.readVInt(), in.readVInt(), in.readVInt(), in.readVInt());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(clusterAlias);
        out.writeVInt(total);
        out.writeVInt(successful);
        out.writeVInt(skipped);
        out.writeVInt(failed);
    }

    public static List<EqlClusterShards> from(SearchResponse response) {
        return from(
            response.getTotalShards(),
            response.getSuccessfulShards(),
            response.getSkippedShards(),
            response.getFailedShards(),
            response.getClusters()
        );
    }

    public static List<EqlClusterShards> from(OpenPointInTimeResponse response) {
        return from(
            response.getTotalShards(),
            response.getSuccessfulShards(),
            response.getSkippedShards(),
            response.getFailedShards(),
            response.getClusters()
        );
    }

    static List<EqlClusterShards> from(int total, int successful, int skipped, int failed, SearchResponse.Clusters clusters) {
        if (clusters != null && clusters.hasClusterObjects()) {
            List<EqlClusterShards> recorded = new ArrayList<>();
            for (String alias : clusters.getClusterAliases()) {
                SearchResponse.Cluster cluster = clusters.getCluster(alias);
                Integer clusterTotal = cluster.getTotalShards();
                if (clusterTotal == null) {
                    continue;
                }
                int skipped = zero(cluster.getSkippedShards());
                recorded.add(
                    new EqlClusterShards(
                        alias,
                        clusterTotal,
                        withoutSkipped(zero(cluster.getSuccessfulShards()), skipped),
                        skipped,
                        zero(cluster.getFailedShards())
                    )
                );
            }
            return List.copyOf(recorded);
        }
        return List.of(
            new EqlClusterShards(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, total, withoutSkipped(successful, skipped), skipped, failed)
        );
    }

    public static List<EqlClusterShards> withSearchFailures(List<EqlClusterShards> counts, Collection<ShardSearchFailure> failures) {
        if (counts.isEmpty() || failures == null || failures.isEmpty()) {
            return counts;
        }
        Map<String, Set<String>> failedShardIds = new HashMap<>();
        for (ShardSearchFailure failure : failures) {
            if (failure.shard() == null || failure.shard().getShardId() == null) {
                continue;
            }
            String alias = failure.shard().getClusterAlias();
            if (alias == null) {
                alias = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
            }
            failedShardIds.computeIfAbsent(alias, ignored -> new HashSet<>()).add(failure.shard().getShardId().toString());
        }
        if (failedShardIds.isEmpty()) {
            return counts;
        }
        List<EqlClusterShards> adjusted = new ArrayList<>(counts.size());
        for (EqlClusterShards entry : counts) {
            int extra = failedShardIds.getOrDefault(entry.clusterAlias(), Set.of()).size();
            int successful = entry.successful() - extra;
            int failed = entry.failed() + extra;
            if (successful < 0) {
                failed += successful;
                successful = 0;
            }
            adjusted.add(new EqlClusterShards(entry.clusterAlias(), entry.total(), successful, entry.skipped(), failed));
        }
        return List.copyOf(adjusted);
    }

    private static int withoutSkipped(int successful, int skipped) {
        return successful > skipped ? successful - skipped : 0;
    }

    private static int zero(Integer value) {
        return value == null ? 0 : value;
    }
}
