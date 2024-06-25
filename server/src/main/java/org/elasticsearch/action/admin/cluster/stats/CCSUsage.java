/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.core.TimeValue;

import java.util.Map;

/**
 * This is a snapshot of telemetry from an individual cross-cluster search for _search or _async_search (or
 * other search endpoints that use the TransportSearchAction such as _msearch).
 */
public class CCSUsage {
    private final long took;
    private final String failureType;  // TODO: enum?
    private final boolean minimizeRoundTrips;
    private final boolean async;
    private final int skippedRemotes;
    private final Map<String, PerClusterUsage> perClusterUsage;

    public static class Builder {
        private long took;
        private String failureType;  // TODO: enum?
        private boolean minimizeRoundTrips;
        private boolean async;
        private int skippedRemotes;
        private Map<String, PerClusterUsage> perClusterUsage;

        public Builder took(long took) {
            this.took = took;
            return this;
        }

        public Builder failureType(String failureType) {
            this.failureType = failureType;
            return this;
        }

        public Builder minimizeRoundTrips(boolean minimizeRoundTrips) {
            this.minimizeRoundTrips = minimizeRoundTrips;
            return this;
        }

        public Builder async(boolean async) {
            this.async = async;
            return this;
        }

        public Builder numSkippedRemotes(int skippedRemotes) {
            this.skippedRemotes = skippedRemotes;
            return this;
        }

        // TODO: this should probably be a per cluster "add", not a setter that takes map - change later
        public Builder perClusterUsage(Map<String, PerClusterUsage> perClusterUsage) {
            this.perClusterUsage = perClusterUsage;
            return this;
        }

        public CCSUsage build() {
            return new CCSUsage(minimizeRoundTrips, async, took, skippedRemotes, failureType, perClusterUsage);
        }
    }

    private CCSUsage(
        boolean minimizeRoundTrips,
        boolean async,
        long took,
        int skippedRemotes,
        String failureType,
        Map<String, PerClusterUsage> perClusterUsage
    ) {
        this.minimizeRoundTrips = minimizeRoundTrips;
        this.async = async;
        this.took = took;
        this.skippedRemotes = skippedRemotes;
        this.failureType = failureType;
        this.perClusterUsage = perClusterUsage;
    }

    public Map<String, PerClusterUsage> getPerClusterUsage() {
        return perClusterUsage;
    }

    public int getSkippedRemotes() {
        return skippedRemotes;
    }

    public long getTook() {
        return took;
    }

    public String getFailureType() {
        return failureType;
    }

    public boolean isMinimizeRoundTrips() {
        return minimizeRoundTrips;
    }

    public boolean isAsync() {
        return async;
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
