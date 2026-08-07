/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.querylog;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.activity.QueryLoggerContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.action.EsqlQueryProfile;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.action.PreparedEsqlQueryRequest;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class EsqlLogContext extends QueryLoggerContext {
    public static final String TYPE = "esql";
    private final EsqlQueryRequest request;
    private final @Nullable EsqlQueryResponse response;
    // Cached index names
    private String[] indexNames = null;

    EsqlLogContext(Task task, EsqlQueryRequest request, EsqlQueryResponse response) {
        super(task, queryType(request), response.getExecutionInfo().overallTook().nanos());
        this.request = request;
        this.response = response;
    }

    EsqlLogContext(Task task, EsqlQueryRequest request, long tookInNanos, Exception error) {
        super(task, queryType(request), tookInNanos, error);
        this.request = request;
        this.response = null;
    }

    private static String queryType(EsqlQueryRequest request) {
        if (request instanceof PreparedEsqlQueryRequest prepared && prepared.getType() != null) {
            return prepared.getType();
        }
        return TYPE;
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
            successShards.addAndGet(Objects.requireNonNullElse(clusterInfo.getSuccessfulShards(), 0));
            skippedShards.addAndGet(Objects.requireNonNullElse(clusterInfo.getSkippedShards(), 0));
            failedShards.addAndGet(Objects.requireNonNullElse(clusterInfo.getFailedShards(), 0));
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

    /**
     * Returns the response-root rollup counters for the slow-log walk, or {@link Optional#empty()}
     * when no response is available (failure paths). These values mirror what appears under
     * {@code profile.*} when {@code profile=true}, but are surfaced unconditionally so the slow log
     * carries the same per-query cost signal regardless of whether the caller asked for a profile.
     */
    Optional<RollupCounters> getRollupCounters() {
        if (response == null) {
            return Optional.empty();
        }
        return Optional.of(
            new RollupCounters(
                response.documentsFound(),
                response.valuesLoaded(),
                response.rowsEmitted(),
                response.bytesRead(),
                response.readNanos(),
                response.cpuNanos()
            )
        );
    }

    /** Snapshot of the query-level rollup counters surfaced into the slow log. */
    record RollupCounters(long documentsFound, long valuesLoaded, long rowsEmitted, long bytesRead, long readNanos, long cpuNanos) {}

    @Override
    public String[] getIndices() {
        if (request instanceof PreparedEsqlQueryRequest prepared) {
            String index = prepared.getIndex();
            if (index != null) {
                return new String[] { index };
            }
        }
        if (response == null) {
            return null;
        }
        if (indexNames != null) {
            return indexNames;
        }
        indexNames = response.getExecutionInfo()
            .getClusters()
            .values()
            .stream()
            .flatMap(
                cluster -> Arrays.stream(Strings.splitStringByCommaToArray(cluster.getIndexExpression()))
                    .map(ind -> RemoteClusterAware.buildRemoteIndexName(cluster.getClusterAlias(), ind))
            )
            .toArray(String[]::new);
        return indexNames;
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

    @Override
    protected QueryBuilder queryFilter() {
        return request.filter();
    }

    public Map<String, String> namedParams() {
        var params = request.params().namedParams();
        if (params.isEmpty()) {
            return Map.of();
        }
        return params.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> String.valueOf(e.getValue().value())));
    }

    public List<String> params() {
        return request.params().params().stream().map(p -> String.valueOf(p.value())).collect(Collectors.toList());
    }
}
