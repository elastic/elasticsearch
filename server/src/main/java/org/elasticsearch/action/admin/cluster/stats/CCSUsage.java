/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.admin.cluster.stats.CCSUsageTelemetry.Result;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ElasticsearchSecurityException;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.query.SearchTimeoutException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.transport.RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;

/**
 * This is a container for telemetry data from an individual cross-cluster search for _search or _async_search (or
 * other search endpoints that use the {@link TransportSearchAction} such as _msearch).
 */
public class CCSUsage {
    private final long took;
    private final Result status;
    private final Set<String> features;
    private final int remotesCount;

    private final String client;

    private final Set<String> skippedRemotes;
    private final Map<String, PerClusterUsage> perClusterUsage;

    public static class Builder {
        private long took;
        private final Set<String> features;
        private Result status = Result.SUCCESS;
        private int remotesCount;
        private String client;
        private final Set<String> skippedRemotes;
        private final Map<String, PerClusterUsage> perClusterUsage;

        public Builder() {
            features = new HashSet<>();
            skippedRemotes = new HashSet<>();
            perClusterUsage = new HashMap<>();
        }

        public Builder took(long took) {
            this.took = took;
            return this;
        }

        public Builder setFailure(Result failureType) {
            this.status = failureType;
            return this;
        }

        public Builder setFailure(Exception e) {
            return setFailure(getFailureType(e));
        }

        public Builder setFeature(String feature) {
            this.features.add(feature);
            return this;
        }

        public Builder setClient(String client) {
            this.client = client;
            return this;
        }

        public Builder setClientFromTask(Task task) {
            String client = task.getHeader(Task.X_ELASTIC_PRODUCT_ORIGIN_HTTP_HEADER);
            if (client != null) {
                return setClient(client);
            } else {
                return this;
            }
        }

        public Builder skippedRemote(String remote) {
            this.skippedRemotes.add(remote);
            return this;
        }

        public Builder perClusterUsage(String remote, TimeValue took) {
            this.perClusterUsage.put(remote, new PerClusterUsage(took));
            return this;
        }

        public CCSUsage build() {
            return new CCSUsage(took, status, remotesCount, skippedRemotes, features, client, perClusterUsage);
        }

        public Builder setRemotesCount(int remotesCount) {
            this.remotesCount = remotesCount;
            return this;
        }

        public int getRemotesCount() {
            return remotesCount;
        }

        /**
         * Get failure type as {@link Result} from the search failure exception.
         */
        public static Result getFailureType(Exception e) {
            var unwrapped = ExceptionsHelper.unwrapCause(e);
            if (unwrapped instanceof Exception) {
                e = (Exception) unwrapped;
            }
            if (ExceptionsHelper.isRemoteUnavailableException(e)) {
                return Result.REMOTES_UNAVAILABLE;
            }
            if (ExceptionsHelper.unwrap(e, ResourceNotFoundException.class) != null) {
                return Result.NOT_FOUND;
            }
            if (e instanceof TaskCancelledException || (ExceptionsHelper.unwrap(e, TaskCancelledException.class) != null)) {
                return Result.CANCELED;
            }
            if (ExceptionsHelper.unwrap(e, SearchTimeoutException.class) != null) {
                return Result.TIMEOUT;
            }
            if (ExceptionsHelper.unwrap(e, ElasticsearchSecurityException.class) != null) {
                return Result.SECURITY;
            }
            if (ExceptionsHelper.unwrapCorruption(e) != null) {
                return Result.CORRUPTION;
            }
            ElasticsearchStatusException se = (ElasticsearchStatusException) ExceptionsHelper.unwrap(e, ElasticsearchStatusException.class);
            if (se != null && se.getDetailedMessage().contains("license")) {
                return Result.LICENSE;
            }
            // This is kind of last resort check - if we still don't know the reason but all shard failures are remote,
            // we assume it's remote's fault somehow.
            if (e instanceof SearchPhaseExecutionException spe) {
                // If this is a failure that happened because of remote failures only
                var groupedFails = ExceptionsHelper.groupBy(spe.shardFailures());
                if (Arrays.stream(groupedFails).allMatch(Builder::isRemoteFailure)) {
                    return Result.REMOTES_UNAVAILABLE;
                }
            }
            // OK we don't know what happened
            return Result.UNKNOWN;
        }

        /**
         * Is this failure coming from a remote cluster?
         */
        static boolean isRemoteFailure(ShardOperationFailedException failure) {
            if (failure instanceof ShardSearchFailure shardFailure) {
                SearchShardTarget shard = shardFailure.shard();
                return shard != null && shard.getClusterAlias() != null && LOCAL_CLUSTER_GROUP_KEY.equals(shard.getClusterAlias()) == false;
            }
            return false;
        }
    }

    private CCSUsage(
        long took,
        Result status,
        int remotesCount,
        Set<String> skippedRemotes,
        Set<String> features,
        String client,
        Map<String, PerClusterUsage> perClusterUsage
    ) {
        this.status = status;
        this.remotesCount = remotesCount;
        this.features = features;
        this.client = client;
        this.took = took;
        this.skippedRemotes = skippedRemotes;
        this.perClusterUsage = perClusterUsage;
    }

    public Map<String, PerClusterUsage> getPerClusterUsage() {
        return perClusterUsage;
    }

    public Result getStatus() {
        return status;
    }

    public Set<String> getFeatures() {
        return features;
    }

    public long getRemotesCount() {
        return remotesCount;
    }

    public String getClient() {
        return client;
    }

    public long getTook() {
        return took;
    }

    public Set<String> getSkippedRemotes() {
        return skippedRemotes;
    }

    public static class PerClusterUsage {

        // if MRT=true, the took time on the remote cluster (if MRT=true), otherwise the overall took time
        private long took;

        public PerClusterUsage(TimeValue took) {
            if (took != null) {
                this.took = took.millis();
            }
        }

        public long getTook() {
            return took;
        }
    }

}
