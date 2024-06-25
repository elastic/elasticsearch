/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.HdrHistogram.DoubleHistogram;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Service holding accumulated CCS search usage statistics. Individual cross-cluster searches will pass
 * CCSUsage data here to have it collated and aggregated. Snapshots of the current CCS Telemetry Usage
 * can be obtained by getting CCSTelemetrySnapshot objects (TODO: create that class (and determine the best name)).
 */
public class CCSUsageTelemetry {

    private final LongAdder totalCCSCount;  // TODO: need this? or just sum the successfulSearchTelem and failedSearchTelem ?
    private final SuccessfulCCSTelemetry successfulSearchTelem;
    private final FailedCCSTelemetry failedSearchTelem;
    private final Map<String, PerClusterCCSTelemetry> byRemoteCluster;

    public CCSUsageTelemetry() {
        this.totalCCSCount = new LongAdder();
        this.successfulSearchTelem = new SuccessfulCCSTelemetry();
        this.failedSearchTelem = new FailedCCSTelemetry();
        this.byRemoteCluster = new ConcurrentHashMap<>();
    }

    public void updateUsage(CCSUsage ccsUsage) {
        // TODO: fork this to a background thread? if yes, could just pass in the SearchResponse to parse it off the response thread
        doUpdate(ccsUsage);
    }

    // TODO: what is the best thread-safety model here? Start with locking model in order to get the functionality working.
    private synchronized void doUpdate(CCSUsage ccsUsage) {
        totalCCSCount.increment();
        if (ccsUsage.getFailureType() == null) {
            // handle successful (or partially successful query)
            successfulSearchTelem.update(ccsUsage);
            for (Map.Entry<String, CCSUsage.PerClusterUsage> entry : ccsUsage.getPerClusterUsage().entrySet()) {
                PerClusterCCSTelemetry perClusterCCSTelemetry = byRemoteCluster.get(entry.getKey());
                if (perClusterCCSTelemetry == null) {
                    perClusterCCSTelemetry = new PerClusterCCSTelemetry(entry.getKey());
                    byRemoteCluster.put(entry.getKey(), perClusterCCSTelemetry);
                }
                // TODO: add more fields/data to the perClusterCCSTelemetry
                perClusterCCSTelemetry.update(entry.getValue());
            }

        } else {
            // handle failed query
            failedSearchTelem.update(ccsUsage);
        }
    }

    public long getTotalCCSCount() {
        return totalCCSCount.sum();
    }

    // TODO: the getters below need to create an immutable snapshot of CCS Telemetry and return that - see SearchUsageStats as example
    public SuccessfulCCSTelemetry getSuccessfulSearchTelemetry() {
        return successfulSearchTelem;
    }

    public FailedCCSTelemetry getFailedSearchTelemetry() {
        return failedSearchTelem;
    }

    public Map<String, PerClusterCCSTelemetry> getTelemetryByCluster() {
        return byRemoteCluster;
    }

    /**
     * Telemetry metrics for successful searches (includes searches with partial failures)
     */
    static class SuccessfulCCSTelemetry {
        private long count; // total number of searches
        private long countMinimizeRoundtrips;
        private long countSearchesWithSkippedRemotes;
        private long countAsync;
        private DoubleHistogram latency;

        SuccessfulCCSTelemetry() {
            this.count = 0;
            this.countMinimizeRoundtrips = 0;
            this.countSearchesWithSkippedRemotes = 0;
            this.countAsync = 0;
            this.latency = new DoubleHistogram(2);
        }

        void update(CCSUsage ccsUsage) {
            count++;
            countMinimizeRoundtrips += ccsUsage.isMinimizeRoundTrips() ? 1 : 0;
            countSearchesWithSkippedRemotes += ccsUsage.getSkippedRemotes() > 0 ? 1 : 0;
            countAsync += ccsUsage.isAsync() ? 1 : 0;
            latency.recordValue(ccsUsage.getTook());
        }

        // TODO: remove these getters and replace with a toSuccessfulCCSUsageSnapshot method?
        public long getCount() {
            return count;
        }

        public long getCountMinimizeRoundtrips() {
            return countMinimizeRoundtrips;
        }

        public long getCountSearchesWithSkippedRemotes() {
            return countSearchesWithSkippedRemotes;
        }

        public long getCountAsync() {
            return countAsync;
        }

        public double getMeanLatency() {
            return latency.getMean();
        }
    }

    /**
     * Telemetry metrics for failed searches (no data returned)
     */
    static class FailedCCSTelemetry {
        private long count;
        private Map<String, Integer> causes;

        FailedCCSTelemetry() {
            causes = new HashMap<>();
        }

        void update(CCSUsage ccsUsage) {
            count++;
            causes.compute(ccsUsage.getFailureType(), (k, v) -> (v == null) ? 1 : v + 1);
        }

        public long getCount() {
            return count;
        }
    }

    /**
     * Telemetry of each remote involved in cross cluster searches
     */
    static class PerClusterCCSTelemetry {
        private String clusterAlias;
        private long count;
        private DoubleHistogram latency;

        PerClusterCCSTelemetry(String clusterAlias) {
            this.clusterAlias = clusterAlias;
            this.count = 0;
            // TODO: what should we use for num significant value digits?
            latency = new DoubleHistogram(2);
        }

        void update(CCSUsage.PerClusterUsage remoteUsage) {
            count++;
            latency.recordValue(remoteUsage.getTook()); // TODO: do we need to add count as well using recordValueWithCount?
        }

        public long getCount() {
            return count;
        }

        // TODO: add additional getters around latency (max, p90)
        public double getMeanLatency() {
            return latency.getMean();
        }

        @Override
        public String toString() {
            return "PerClusterCCSTelemetry{"
                + "clusterAlias='"
                + clusterAlias
                + '\''
                + ", count="
                + count
                + ", latency(mean)="
                + latency.getMean()
                + '}';
        }
    }
}
