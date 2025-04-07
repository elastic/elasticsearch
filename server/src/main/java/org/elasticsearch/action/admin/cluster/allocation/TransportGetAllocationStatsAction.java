/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.allocation;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.SingleResultDeduplicator;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequestParameters.Metric;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.routing.allocation.AllocationStatsService;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.cluster.routing.allocation.NodeAllocationStats;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class TransportGetAllocationStatsAction extends TransportMasterNodeReadAction<
    TransportGetAllocationStatsAction.Request,
    TransportGetAllocationStatsAction.Response> {

    public static final ActionType<TransportGetAllocationStatsAction.Response> TYPE = new ActionType<>("cluster:monitor/allocation/stats");

    public static final TimeValue DEFAULT_CACHE_TTL = TimeValue.timeValueMinutes(1);
    public static final Setting<TimeValue> CACHE_TTL_SETTING = Setting.timeSetting(
        "cluster.routing.allocation.stats.cache.ttl",
        DEFAULT_CACHE_TTL,
        TimeValue.ZERO,
        TimeValue.timeValueMinutes(10),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private final AllocationStatsCache allocationStatsCache;
    private final SingleResultDeduplicator<Map<String, NodeAllocationStats>> allocationStatsSupplier;
    private final DiskThresholdSettings diskThresholdSettings;

    @Inject
    public TransportGetAllocationStatsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        AllocationStatsService allocationStatsService
    ) {
        super(
            TYPE.name(),
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            TransportGetAllocationStatsAction.Request::new,
            TransportGetAllocationStatsAction.Response::new,
            // DIRECT is ok here because we fork the allocation stats computation onto a MANAGEMENT thread if needed, or else we return
            // very cheaply.
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        final var managementExecutor = threadPool.executor(ThreadPool.Names.MANAGEMENT);
        this.allocationStatsCache = new AllocationStatsCache(threadPool, DEFAULT_CACHE_TTL);
        this.allocationStatsSupplier = new SingleResultDeduplicator<>(threadPool.getThreadContext(), l -> {
            final var cachedStats = allocationStatsCache.get();
            if (cachedStats != null) {
                l.onResponse(cachedStats);
                return;
            }

            managementExecutor.execute(ActionRunnable.supply(l, () -> {
                final var stats = allocationStatsService.stats();
                allocationStatsCache.put(stats);
                return stats;
            }));
        });
        this.diskThresholdSettings = new DiskThresholdSettings(clusterService.getSettings(), clusterService.getClusterSettings());
        clusterService.getClusterSettings().initializeAndWatch(CACHE_TTL_SETTING, this.allocationStatsCache::setTTL);
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        if (clusterService.state().getMinTransportVersion().before(TransportVersions.V_8_14_0)) {
            // The action is not available before ALLOCATION_STATS
            listener.onResponse(new Response(Map.of(), null));
            return;
        }
        super.doExecute(task, request, listener);
    }

    @Override
    protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<Response> listener) throws Exception {
        // NB we are still on a transport thread here - if adding more functionality here make sure to fork to a different pool

        final SubscribableListener<Map<String, NodeAllocationStats>> allocationStatsStep = request.metrics().contains(Metric.ALLOCATIONS)
            ? SubscribableListener.newForked(allocationStatsSupplier::execute)
            : SubscribableListener.newSucceeded(Map.of());

        allocationStatsStep.andThenApply(
            allocationStats -> new Response(allocationStats, request.metrics().contains(Metric.FS) ? diskThresholdSettings : null)
        ).addListener(listener);
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    public static class Request extends MasterNodeReadRequest<Request> {

        private final EnumSet<Metric> metrics;

        @SuppressWarnings("this-escape")
        public Request(TimeValue masterNodeTimeout, TaskId parentTaskId, EnumSet<Metric> metrics) {
            super(masterNodeTimeout);
            setParentTask(parentTaskId);
            this.metrics = metrics;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.metrics = in.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)
                ? in.readEnumSet(Metric.class)
                : EnumSet.of(Metric.ALLOCATIONS, Metric.FS);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            assert out.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0);
            super.writeTo(out);
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
                out.writeEnumSet(metrics);
            }
        }

        public EnumSet<Metric> metrics() {
            return metrics;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class Response extends ActionResponse {

        private final Map<String, NodeAllocationStats> nodeAllocationStats;
        @Nullable // for bwc
        private final DiskThresholdSettings diskThresholdSettings;

        public Response(Map<String, NodeAllocationStats> nodeAllocationStats, DiskThresholdSettings diskThresholdSettings) {
            this.nodeAllocationStats = nodeAllocationStats;
            this.diskThresholdSettings = diskThresholdSettings;
        }

        public Response(StreamInput in) throws IOException {
            this.nodeAllocationStats = in.readImmutableMap(StreamInput::readString, NodeAllocationStats::new);
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
                this.diskThresholdSettings = in.readOptionalWriteable(DiskThresholdSettings::readFrom);
            } else {
                this.diskThresholdSettings = null;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(nodeAllocationStats, StreamOutput::writeString, StreamOutput::writeWriteable);
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
                out.writeOptionalWriteable(diskThresholdSettings);
            } else {
                assert diskThresholdSettings == null;
            }
        }

        public Map<String, NodeAllocationStats> getNodeAllocationStats() {
            return nodeAllocationStats;
        }

        @Nullable // for bwc
        public DiskThresholdSettings getDiskThresholdSettings() {
            return diskThresholdSettings;
        }
    }

    private record CachedAllocationStats(Map<String, NodeAllocationStats> stats, long timestampMillis) {}

    private static class AllocationStatsCache {
        private volatile long ttlMillis;
        private final ThreadPool threadPool;
        private final AtomicReference<CachedAllocationStats> cachedStats;

        AllocationStatsCache(ThreadPool threadPool, TimeValue ttl) {
            this.threadPool = threadPool;
            this.cachedStats = new AtomicReference<>();
            setTTL(ttl);
        }

        void setTTL(TimeValue ttl) {
            ttlMillis = ttl.millis();
            if (ttlMillis == 0L) {
                cachedStats.set(null);
            }
        }

        Map<String, NodeAllocationStats> get() {
            if (ttlMillis == 0L) {
                return null;
            }

            // We don't set the atomic ref to null here upon expiration since we know it is about to be replaced with a fresh instance.
            final var stats = cachedStats.get();
            return stats == null || threadPool.relativeTimeInMillis() - stats.timestampMillis > ttlMillis ? null : stats.stats;
        }

        void put(Map<String, NodeAllocationStats> stats) {
            if (ttlMillis > 0L) {
                cachedStats.set(new CachedAllocationStats(stats, threadPool.relativeTimeInMillis()));
            }
        }
    }
}
