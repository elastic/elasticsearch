/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.logging;

import org.elasticsearch.action.ResolvedIndexExpressions;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.logging.activity.ActivityLoggerContext;
import org.elasticsearch.common.logging.activity.QueryLoggerContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.eql.action.EqlSearchRequest;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse;

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class EqlLogContext extends ActivityLoggerContext implements QueryLoggerContext {
    public static final String TYPE = "eql";
    private final EqlSearchRequest request;
    private final EqlSearchResponse response;

    EqlLogContext(Task task, EqlSearchRequest request, EqlSearchResponse response) {
        super(task, TYPE, TimeUnit.MILLISECONDS.toNanos(response.took()));
        this.request = request;
        this.response = response;
    }

    EqlLogContext(Task task, EqlSearchRequest request, long tookInNanos, Exception error) {
        super(task, TYPE, tookInNanos, error);
        this.request = request;
        this.response = null;
    }

    @Override
    public String getQuery() {
        return request.query();
    }

    @Override
    public String[] getIndices() {
        return request.indices();
    }

    @Override
    public boolean isTimedOut() {
        return response != null && response.isTimeout();
    }

    @Override
    public int getResultCount() {
        if (response == null || response.hits() == null || response.hits().totalHits() == null) {
            return 0;
        }
        return Math.clamp(response.hits().totalHits().value(), 0, Integer.MAX_VALUE);
    }

    @Override
    public Optional<ShardInfo> shardInfo() {
        // We only know about failed shards in EQL
        return Optional.ofNullable(response).map(r -> new ShardInfo(null, null, getFailedShards(response)));
    }

    private static int getFailedShards(EqlSearchResponse response) {
        long failedShards = Arrays.stream(response.shardFailures()).map(ShardSearchFailure::shard).distinct().count();
        return Math.clamp(failedShards, 0, Integer.MAX_VALUE);
    }

    // CCS stuff
    public Collection<String> remoteClusterAliases() {
        ResolvedIndexExpressions resolved = request.getResolvedIndexExpressions();
        Stream<String> indices = resolved != null ? resolved.getRemoteIndicesList().stream() : Arrays.stream(request.indices());
        return indices.filter(RemoteClusterAware::isRemoteIndexName)
            .map(i -> RemoteClusterAware.splitIndexName(i)[0])
            .collect(Collectors.toSet());
    }
}
