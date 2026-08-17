/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.querylog;

import org.elasticsearch.common.Strings;
import org.elasticsearch.compute.operator.PageStreamPublisher;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.action.EsqlQueryProfile;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.session.Result;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * {@link EsqlLogContext} for streaming queries ({@code POST /_query/stream}), which never build an
 * {@link org.elasticsearch.xpack.esql.action.EsqlQueryResponse}. Response-derived fields are sourced
 * instead from the {@link EsqlExecutionInfo} (mutated in place during execution), the final
 * {@link Result}, and the {@link PageStreamPublisher} row counter.
 */
class EsqlStreamLogContext extends EsqlLogContext {

    private final EsqlExecutionInfo executionInfo;
    private final PageStreamPublisher publisher;
    private final Result result;

    EsqlStreamLogContext(
        Task task,
        EsqlQueryRequest request,
        long tookInNanos,
        EsqlExecutionInfo executionInfo,
        PageStreamPublisher publisher,
        Result result
    ) {
        super(task, request, tookInNanos);
        this.executionInfo = executionInfo;
        this.publisher = publisher;
        this.result = result;
    }

    /**
     * Returns rows accepted into the publisher buffer. On a cancelled stream this can exceed rows
     * the client actually consumed, since buffered-but-undelivered rows are still counted.
     */
    @Override
    public int getResultCount() {
        return Math.clamp(publisher.rowsPublished(), 0, Integer.MAX_VALUE);
    }

    @Override
    public String[] getIndices() {
        return executionInfo.getClusters()
            .values()
            .stream()
            .flatMap(
                cluster -> Arrays.stream(Strings.splitStringByCommaToArray(cluster.getIndexExpression()))
                    .map(ind -> RemoteClusterAware.buildRemoteIndexName(cluster.getClusterAlias(), ind))
            )
            .toArray(String[]::new);
    }

    @Override
    public Map<String, String> getClusters() {
        return executionInfo.getClusters()
            .entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getStatus().toString()));
    }

    @Override
    public Optional<ShardInfo> shardInfo() {
        return Optional.of(EsqlLogContext.getShardInfo(executionInfo));
    }

    @Override
    Optional<EsqlQueryProfile> getQueryProfile() {
        return Optional.ofNullable(executionInfo.queryProfile());
    }

    @Override
    Optional<RollupCounters> getRollupCounters() {
        var ci = result.completionInfo();
        return Optional.of(
            new RollupCounters(ci.documentsFound(), ci.valuesLoaded(), ci.rowsEmitted(), ci.bytesRead(), ci.readNanos(), ci.cpuNanos())
        );
    }
}
