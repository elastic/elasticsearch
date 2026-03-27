/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.querylog;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.activity.ActivityLoggerContext;
import org.elasticsearch.common.logging.activity.QueryLoggerContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.action.EsqlQueryProfile;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class EsqlLogContext extends ActivityLoggerContext implements QueryLoggerContext {
    public static final String TYPE = "esql";
    private final EsqlQueryRequest request;
    private final @Nullable EsqlQueryResponse response;

    EsqlLogContext(Task task, EsqlQueryRequest request, EsqlQueryResponse response) {
        super(task, TYPE, response.getExecutionInfo().overallTook().nanos());
        this.request = request;
        this.response = response;
    }

    EsqlLogContext(Task task, EsqlQueryRequest request, long tookInNanos, Exception error) {
        super(task, TYPE, tookInNanos, error);
        this.request = request;
        this.response = null;
    }

    @Override
    public String getQuery() {
        return request.queryDescription();
    }

    public Optional<ShardInfo> shardInfo() {
        return Optional.ofNullable(response).map(EsqlQueryResponse::getExecutionInfo).map(EsqlLogContext::getShardInfo);
    }

    static ShardInfo getShardInfo(EsqlExecutionInfo info) {
        AtomicInteger successShards = new AtomicInteger(0);
        AtomicInteger skippedShards = new AtomicInteger(0);
        AtomicInteger failedShards = new AtomicInteger(0);
        info.getClusters().forEach((alias, clusterInfo) -> {
            successShards.addAndGet(clusterInfo.getSuccessfulShards());
            skippedShards.addAndGet(clusterInfo.getSkippedShards());
            failedShards.addAndGet(clusterInfo.getFailedShards());
        });
        return new ShardInfo(successShards.get(), skippedShards.get(), failedShards.get());
    }

    @Override
    public int getResultCount() {
        if (response == null) {
            return 0;
        }
        return Math.clamp(response.getRowCount(), 0, Integer.MAX_VALUE);
    }

    Optional<EsqlQueryProfile> getQueryProfile() {
        return Optional.ofNullable(response).map(it -> it.getExecutionInfo().queryProfile());
    }

    @Override
    public String[] getIndices() {
        if (response == null) {
            return null;
        }
        return response.getExecutionInfo()
            .getClusters()
            .values()
            .stream()
            .flatMap(
                cluster -> Arrays.stream(Strings.splitStringByCommaToArray(cluster.getIndexExpression()))
                    .map(ind -> RemoteClusterAware.buildRemoteIndexName(cluster.getClusterAlias(), ind))
            )
            .toArray(String[]::new);
    }

    // CCS stuff

    @Override
    public Map<String, String> getClusters() {
        if (response == null) {
            return Map.of();
        }
        return response.getExecutionInfo()
            .getClusters()
            .entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getStatus().toString()));
    }
}
