/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.common.util.Maps;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Service holding accumulated CCS search usage statistics. Individual cross-cluster searches will pass
 * CCSUsage data here to have it collated and aggregated. Snapshots of the current CCS Telemetry Usage
 * can be obtained by getting {@link CCSTelemetrySnapshot} objects.
 * <br>
 * Theory of operation:
 * Each search creates a {@link CCSUsage.Builder}, which can be updated during the progress of the search request,
 * and then it instantiates a {@link CCSUsage} object when the request is finished.
 * That object is passed to {@link #updateUsage(CCSUsage)} on the request processing end (whether successful or not).
 * The {@link #updateUsage(CCSUsage)} method will then update the internal counters and metrics.
 * <br>
 * When we need to return the current state of the telemetry, we can call {@link #getCCSTelemetrySnapshot()} which produces
 * a snapshot of the current state of the telemetry as {@link CCSTelemetrySnapshot}. These snapshots are additive so
 * when collecting the snapshots from multiple nodes, an empty snapshot is created and then all the node's snapshots are added
 * to it to obtain the summary telemetry.
 */
public class CCSUsageTelemetry {

    /**
     * Result of the request execution.
     * Either "success" or a failure reason.
     */
    public enum Result {
        SUCCESS("success"),
        REMOTES_UNAVAILABLE("remotes_unavailable"),
        CANCELED("canceled"),
        UNKNOWN("unknown");

        private final String name;

        Result(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    // Not enum because we won't mind other places adding their own features
    public static final String MRT_FEATURE = "mrt_on";
    public static final String ASYNC_FEATURE = "async";
    public static final String WILDCARD_FEATURE = "wildcards";
    private static final String CLIENT_UNKNOWN = "unknown";

    // TODO: do we need LongAdder here or long is enough? Since updateUsage is synchronized, worst that can happen is
    // we may miss a count on read.
    private final LongAdder totalCount;
    private final LongAdder successCount;
    private final Map<Result, LongAdder> failureReasons;

    private final LongMetric took;
    private final LongMetric tookMrtTrue;
    private final LongMetric tookMrtFalse;
    private final LongMetric remotesPerSearch;
    private final LongAdder skippedRemotes;

    private final Map<String, LongAdder> featureCounts;

    private final Map<String, LongAdder> clientCounts;
    private final Map<String, PerClusterCCSTelemetry> byRemoteCluster;

    public CCSUsageTelemetry() {
        this.byRemoteCluster = new ConcurrentHashMap<>();
        totalCount = new LongAdder();
        successCount = new LongAdder();
        failureReasons = new ConcurrentHashMap<>();
        took = new LongMetric();
        tookMrtTrue = new LongMetric();
        tookMrtFalse = new LongMetric();
        remotesPerSearch = new LongMetric();
        skippedRemotes = new LongAdder();
        featureCounts = new ConcurrentHashMap<>();
        clientCounts = new ConcurrentHashMap<>();
    }

    public void updateUsage(CCSUsage ccsUsage) {
        // TODO: fork this to a background thread? if yes, could just pass in the SearchResponse to parse it off the response thread
        doUpdate(ccsUsage);
    }

    // TODO: what is the best thread-safety model here? Start with locking model in order to get the functionality working.
    private synchronized void doUpdate(CCSUsage ccsUsage) {
        totalCount.increment();
        long searchTook = ccsUsage.getTook();
        if (isSuccess(ccsUsage)) {
            successCount.increment();
            // TODO: do we need to count latencies for failed requests? Right now the code doesn't collect it without SearchResponse
            took.record(searchTook);
            if (isMRT(ccsUsage)) {
                tookMrtTrue.record(searchTook);
            } else {
                tookMrtFalse.record(searchTook);
            }
            // TODO: can we do it on failure? Not sure what "took" values contain in that case if at all.
            ccsUsage.getPerClusterUsage().forEach((r, u) -> byRemoteCluster.computeIfAbsent(r, PerClusterCCSTelemetry::new).update(u));
        } else {
            failureReasons.computeIfAbsent(ccsUsage.getStatus(), k -> new LongAdder()).increment();
        }

        remotesPerSearch.record(ccsUsage.getRemotesCount());
        if (ccsUsage.getSkippedRemotes().isEmpty() == false) {
            skippedRemotes.increment();
            ccsUsage.getSkippedRemotes().forEach(remote -> byRemoteCluster.computeIfAbsent(remote, PerClusterCCSTelemetry::new).skipped());
        }
        ccsUsage.getFeatures().forEach(f -> featureCounts.computeIfAbsent(f, k -> new LongAdder()).increment());
        if (ccsUsage.getClient() != null) {
            clientCounts.computeIfAbsent(ccsUsage.getClient(), k -> new LongAdder()).increment();
        } else {
            clientCounts.computeIfAbsent(CLIENT_UNKNOWN, k -> new LongAdder()).increment();
        }
    }

    private boolean isMRT(CCSUsage ccsUsage) {
        return ccsUsage.getFeatures().contains(MRT_FEATURE);
    }

    private boolean isSuccess(CCSUsage ccsUsage) {
        return ccsUsage.getStatus() == Result.SUCCESS;
    }

    public Map<String, PerClusterCCSTelemetry> getTelemetryByCluster() {
        return byRemoteCluster;
    }

    /**
     * Telemetry of each remote involved in cross cluster searches
     */
    public static class PerClusterCCSTelemetry {
        private final String clusterAlias;
        // TODO: are we OK to use long and not LongAdder here?
        private long count;
        private long skippedCount;
        private final LongMetric took;

        PerClusterCCSTelemetry(String clusterAlias) {
            this.clusterAlias = clusterAlias;
            this.count = 0;
            took = new LongMetric();
            this.skippedCount = 0;
        }

        void update(CCSUsage.PerClusterUsage remoteUsage) {
            count++;
            took.record(remoteUsage.getTook());
        }

        void skipped() {
            skippedCount++;
        }

        public long getCount() {
            return count;
        }

        @Override
        public String toString() {
            return "PerClusterCCSTelemetry{"
                + "clusterAlias='"
                + clusterAlias
                + '\''
                + ", count="
                + count
                + ", latency="
                + took.toString()
                + '}';
        }

        public long getSkippedCount() {
            return skippedCount;
        }

        public CCSTelemetrySnapshot.PerClusterCCSTelemetry getSnapshot() {
            return new CCSTelemetrySnapshot.PerClusterCCSTelemetry(count, skippedCount, took.getValue());
        }

    }

    // TODO: I wonder if it wouldn't be more correct if this lived on the Snapshot side,
    // but SearchUsage does it this way so following the pattern here.
    public CCSTelemetrySnapshot getCCSTelemetrySnapshot() {
        Map<String, Long> reasonsMap = Maps.newMapWithExpectedSize(failureReasons.size());
        failureReasons.forEach((k, v) -> reasonsMap.put(k.getName(), v.longValue()));

        // TODO: should we use immutable maps here?
        // Since we return copies anyway it's no big deal if anybody modifies them, but it may be cleaner to return immutable.
        LongMetric.LongMetricValue remotes = remotesPerSearch.getValue();
        return new CCSTelemetrySnapshot(
            totalCount.longValue(),
            successCount.longValue(),
            reasonsMap,
            took.getValue(),
            tookMrtTrue.getValue(),
            tookMrtFalse.getValue(),
            remotes.max(),
            remotes.avg(),
            skippedRemotes.longValue(),
            Maps.transformValues(featureCounts, LongAdder::longValue),
            Maps.transformValues(clientCounts, LongAdder::longValue),
            Maps.transformValues(byRemoteCluster, PerClusterCCSTelemetry::getSnapshot)
        );
    }
}
