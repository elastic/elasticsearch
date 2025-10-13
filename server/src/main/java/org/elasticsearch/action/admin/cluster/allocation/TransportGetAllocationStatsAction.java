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
import org.elasticsearch.common.util.CancellableSingleObjectCache;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.BooleanSupplier;

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
        this.allocationStatsCache = new AllocationStatsCache(threadPool, allocationStatsService, DEFAULT_CACHE_TTL);
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

        assert task instanceof CancellableTask;
        final var cancellableTask = (CancellableTask) task;

        final SubscribableListener<Map<String, NodeAllocationStats>> allocationStatsStep = request.metrics().contains(Metric.ALLOCATIONS)
            ? SubscribableListener.newForked(l -> allocationStatsCache.get(cancellableTask::isCancelled, l))
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

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "", parentTaskId, headers);
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

    private static class AllocationStatsCache extends CancellableSingleObjectCache<Long, Long, Map<String, NodeAllocationStats>> {
        private volatile long ttlMillis;
        private final ThreadPool threadPool;
        private final Executor executor;
        private final AllocationStatsService allocationStatsService;

        AllocationStatsCache(ThreadPool threadPool, AllocationStatsService allocationStatsService, TimeValue ttl) {
            super(threadPool.getThreadContext());
            this.threadPool = threadPool;
            this.executor = threadPool.executor(ThreadPool.Names.MANAGEMENT);
            this.allocationStatsService = allocationStatsService;
            setTTL(ttl);
        }

        void setTTL(TimeValue ttl) {
            ttlMillis = ttl.millis();
            clearCacheIfDisabled();
        }

        void get(BooleanSupplier isCancelled, ActionListener<Map<String, NodeAllocationStats>> listener) {
            get(threadPool.relativeTimeInMillis(), isCancelled, listener);
        }

        @Override
        protected void refresh(
            Long aLong,
            Runnable ensureNotCancelled,
            BooleanSupplier supersedeIfStale,
            ActionListener<Map<String, NodeAllocationStats>> listener
        ) {
            if (supersedeIfStale.getAsBoolean() == false) {
                executor.execute(
                    ActionRunnable.supply(
                        // If caching is disabled the item is only cached long enough to prevent duplicate concurrent requests.
                        ActionListener.runBefore(listener, this::clearCacheIfDisabled),
                        () -> allocationStatsService.stats(ensureNotCancelled)
                    )
                );
            }
        }

        @Override
        protected Long getKey(Long timestampMillis) {
            return timestampMillis;
        }

        @Override
        protected boolean isFresh(Long currentKey, Long newKey) {
            return ttlMillis == 0 || newKey - currentKey <= ttlMillis;
        }

        private void clearCacheIfDisabled() {
            if (ttlMillis == 0) {
                clearCurrentCachedItem();
            }
        }
    }
}
