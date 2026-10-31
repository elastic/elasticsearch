/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.logging.activity;

import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Captured context for {@link QueryInterceptor}: the information captured about a completed query,
 * extended by specific contexts for each search action.
 */
public abstract class QueryCaptureContext {
    public static final ToXContent.Params FORMAT_PARAMS = new ToXContent.MapParams(Collections.singletonMap("pretty", "false"));

    private final long tookInNanos;
    private final String type;
    private final @Nullable Exception error;
    private final Task task;
    // Cached "isSystem" flag
    private Boolean isSystemSearch = null;

    protected QueryCaptureContext(Task task, String type, long tookInNanos, @Nullable Exception error) {
        this.type = type;
        this.error = error;
        this.tookInNanos = tookInNanos;
        this.task = task;
    }

    protected QueryCaptureContext(Task task, String type, long tookInNanos) {
        this(task, type, tookInNanos, null);
    }

    public long getTookInNanos() {
        return tookInNanos;
    }

    public boolean isSuccess() {
        return error == null;
    }

    public boolean isTimedOut() {
        return false;
    }

    public String getType() {
        return type;
    }

    public String getErrorMessage() {
        return error == null ? "" : error.getMessage();
    }

    public String getErrorType() {
        return error == null ? "" : error.getClass().getName();
    }

    public String getOpaqueId() {
        return task.getHeader(Task.X_OPAQUE_ID_HTTP_HEADER);
    }

    public String getTraceId() {
        return task.getHeader(Task.TRACE_ID);
    }

    public long getTaskId() {
        return task.getId();
    }

    public Optional<TaskId> getParentTaskId() {
        return task.getParentTaskId().isSet() ? Optional.of(task.getParentTaskId()) : Optional.empty();
    }

    /**
     * Map cluster->status (for now).
     */
    public Map<String, String> getClusters() {
        return Map.of();
    }

    /**
     * Shard stats information. All fields are nullable.
     */
    public record ShardInfo(Integer successfulShards, Integer skippedShards, Integer failedShards) {}

    public Optional<ShardInfo> shardInfo() {
        return Optional.empty();
    }

    /**
     * Get query text.
     */
    public abstract String getQuery();

    /**
     * Number of results.
     */
    public abstract int getResultCount();

    /**
     * Indices used in the query.
     */
    public abstract String[] getIndices();

    public boolean isSystemSearch(Predicate<String> systemChecker) {
        if (systemChecker == null) {
            return false;
        }
        if (isSystemSearch != null) {
            return isSystemSearch;
        }

        final String[] indices = getIndices();
        // Request that only asks for system indices is system search
        isSystemSearch = indices != null && indices.length > 0 && Arrays.stream(indices).allMatch(systemChecker);
        return isSystemSearch;
    }

    public Collection<String> getRemoteClusterAliases(Map<String, String> clusters) {
        return clusters.keySet().stream().filter(alias -> alias.equals(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY) == false).toList();
    }

    public Map<String, Long> getCountsByStatus(Map<String, String> clusters) {
        // Group by status and count how many statuses of each kind there are
        return clusters.values().stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
    }

    protected QueryBuilder queryFilter() {
        return null;
    }

    public Optional<String> getFilter() {
        return filterToLogString(queryFilter());
    }

    public static Optional<String> filterToLogString(QueryBuilder filter) {
        if (filter == null) {
            return Optional.empty();
        }
        try {
            return Optional.of(XContentHelper.toXContent(filter, XContentType.JSON, FORMAT_PARAMS, true).utf8ToString());
        } catch (IOException e) {
            return Optional.empty();
        }
    }
}
