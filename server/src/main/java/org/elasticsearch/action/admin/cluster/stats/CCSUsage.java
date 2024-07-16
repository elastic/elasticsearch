/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.action.admin.cluster.stats.CCSUsageTelemetry.Result;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.core.TimeValue;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * This is a snapshot of telemetry from an individual cross-cluster search for _search or _async_search (or
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

        public Builder setFeature(String feature) {
            this.features.add(feature);
            return this;
        }

        public Builder setClient(String client) {
            this.client = client;
            return this;
        }

        public Builder skipRemote(String remote) {
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
