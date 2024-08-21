/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.common.util.Maps;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
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
        NOT_FOUND("not_found"),
        TIMEOUT("timeout"),
        CORRUPTION("corruption"),
        SECURITY("security"),
        // May be helpful if there's a lot of other reasons, and it may be hard to calculate the unknowns for some clients.
        UNKNOWN("other");

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

    // The list of known Elastic clients. May be incomplete.
    public static final Set<String> KNOWN_CLIENTS = Set.of(
        "kibana",
        "cloud",
        "logstash",
        "beats",
        "fleet",
        "ml",
        "security",
        "observability",
        "enterprise-search",
        "elasticsearch",
        "connectors",
        "connectors-cli"
    );

    private final LongAdder totalCount;
    private final LongAdder successCount;
    private final Map<Result, LongAdder> failureReasons;

    /**
     * Latency metrics overall
     */
    private final LongMetric took;
    /**
     * Latency metrics with minimize_roundtrips=true
     */
    private final LongMetric tookMrtTrue;
    /**
     * Latency metrics with minimize_roundtrips=false
     */
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
        assert ccsUsage.getRemotesCount() > 0 : "Expected at least one remote cluster in CCSUsage";
        // TODO: fork this to a background thread?
        doUpdate(ccsUsage);
    }

    // This is not synchronized, instead we ensure that every metric in the class is thread-safe.
    private void doUpdate(CCSUsage ccsUsage) {
        totalCount.increment();
        long searchTook = ccsUsage.getTook();
        if (isSuccess(ccsUsage)) {
            successCount.increment();
            took.record(searchTook);
            if (isMRT(ccsUsage)) {
                tookMrtTrue.record(searchTook);
            } else {
                tookMrtFalse.record(searchTook);
            }
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
        String client = ccsUsage.getClient();
        if (client != null && KNOWN_CLIENTS.contains(client)) {
            // We count only known clients for now
            clientCounts.computeIfAbsent(ccsUsage.getClient(), k -> new LongAdder()).increment();
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
        // The number of successful (not skipped) requests to this cluster.
        private final LongAdder count;
        private final LongAdder skippedCount;
        // This is only over the successful requetss, skipped ones do not count here.
        private final LongMetric took;

        PerClusterCCSTelemetry(String clusterAlias) {
            this.clusterAlias = clusterAlias;
            this.count = new LongAdder();
            took = new LongMetric();
            this.skippedCount = new LongAdder();
        }

        void update(CCSUsage.PerClusterUsage remoteUsage) {
            count.increment();
            took.record(remoteUsage.getTook());
        }

        void skipped() {
            skippedCount.increment();
        }

        public long getCount() {
            return count.longValue();
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
            return skippedCount.longValue();
        }

        public CCSTelemetrySnapshot.PerClusterCCSTelemetry getSnapshot() {
            return new CCSTelemetrySnapshot.PerClusterCCSTelemetry(count.longValue(), skippedCount.longValue(), took.getValue());
        }

    }

    public CCSTelemetrySnapshot getCCSTelemetrySnapshot() {
        Map<String, Long> reasonsMap = Maps.newMapWithExpectedSize(failureReasons.size());
        failureReasons.forEach((k, v) -> reasonsMap.put(k.getName(), v.longValue()));

        LongMetric.LongMetricValue remotes = remotesPerSearch.getValue();

        // Maps returned here are unmodifiable, but the empty ctor produces modifiable maps
        return new CCSTelemetrySnapshot(
            totalCount.longValue(),
            successCount.longValue(),
            Collections.unmodifiableMap(reasonsMap),
            took.getValue(),
            tookMrtTrue.getValue(),
            tookMrtFalse.getValue(),
            remotes.max(),
            remotes.avg(),
            skippedRemotes.longValue(),
            Collections.unmodifiableMap(Maps.transformValues(featureCounts, LongAdder::longValue)),
            Collections.unmodifiableMap(Maps.transformValues(clientCounts, LongAdder::longValue)),
            Collections.unmodifiableMap(Maps.transformValues(byRemoteCluster, PerClusterCCSTelemetry::getSnapshot))
        );
    }
}
