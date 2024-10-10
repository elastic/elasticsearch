/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.compute.operator.FailureCollector;
import org.elasticsearch.compute.operator.ResponseHeadersCollector;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A variant of {@link RefCountingListener} with the following differences:
 * 1. Automatically cancels sub tasks on failure.
 * 2. Collects driver profiles from sub tasks.
 * 3. Collects response headers from sub tasks, specifically warnings emitted during compute
 * 4. Collects failures and returns the most appropriate exception to the caller.
 * 5. Updates {@link EsqlExecutionInfo} for display in the response for cross-cluster searches
 */
final class ComputeListener implements Releasable {
    private static final Logger LOGGER = LogManager.getLogger(ComputeService.class);

    private final RefCountingListener refs;
    private final FailureCollector failureCollector = new FailureCollector();
    private final AtomicBoolean cancelled = new AtomicBoolean();
    private final CancellableTask task;
    private final TransportService transportService;
    private final List<DriverProfile> collectedProfiles;
    private final ResponseHeadersCollector responseHeaders;
    private final EsqlExecutionInfo esqlExecutionInfo;
    private final long queryStartTimeNanos;
    // clusterAlias indicating where this ComputeListener is running
    // used by the top level ComputeListener in ComputeService on both local and remote clusters
    private final String whereRunning;

    /**
     * Create a ComputeListener that does not need to gather any metadata in EsqlExecutionInfo
     * (currently that's the ComputeListener in DataNodeRequestHandler).
     */
    public static ComputeListener create(
        TransportService transportService,
        CancellableTask task,
        ActionListener<ComputeResponse> delegate
    ) {
        return new ComputeListener(transportService, task, null, null, -1, delegate);
    }

    /**
     * Create a ComputeListener that gathers metadata in EsqlExecutionInfo
     * (currently that's the top level ComputeListener in ComputeService).
     * @param clusterAlias the clusterAlias where this ComputeListener is running. For the querying cluster, use
     *                     RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY. For remote clusters that are part of a CCS,
     *                     the remote cluster is given its clusterAlias in the request sent to it, so that should be
     *                     passed in here. This gives context to the ComputeListener as to where this listener is running
     *                     and thus how it should behave with respect to the {@link EsqlExecutionInfo} metadata it gathers.
     * @param transportService
     * @param task
     * @param executionInfo {@link EsqlExecutionInfo} to capture execution metadata
     * @param queryStartTimeNanos Start time of the ES|QL query (stored in {@link org.elasticsearch.xpack.esql.session.Configuration})
     * @param delegate
     */
    public static ComputeListener create(
        String clusterAlias,
        TransportService transportService,
        CancellableTask task,
        EsqlExecutionInfo executionInfo,
        long queryStartTimeNanos,
        ActionListener<ComputeResponse> delegate
    ) {
        return new ComputeListener(transportService, task, clusterAlias, executionInfo, queryStartTimeNanos, delegate);
    }

    private ComputeListener(
        TransportService transportService,
        CancellableTask task,
        String clusterAlias,
        EsqlExecutionInfo executionInfo,
        long queryStartTimeNanos,
        ActionListener<ComputeResponse> delegate
    ) {
        this.transportService = transportService;
        this.task = task;
        this.responseHeaders = new ResponseHeadersCollector(transportService.getThreadPool().getThreadContext());
        this.collectedProfiles = Collections.synchronizedList(new ArrayList<>());
        this.esqlExecutionInfo = executionInfo;
        this.queryStartTimeNanos = queryStartTimeNanos;
        this.whereRunning = clusterAlias;
        // for the DataNodeHandler ComputeListener, clusterAlias and executionInfo will be null
        // for the top level ComputeListener in ComputeService both will be non-null
        assert (clusterAlias == null && executionInfo == null) || (clusterAlias != null && executionInfo != null)
            : "clusterAlias and executionInfo must both be null or both non-null";

        // listener that executes after all the sub-listeners refs (created via acquireCompute) have completed
        this.refs = new RefCountingListener(1, ActionListener.wrap(ignored -> {
            responseHeaders.finish();
            ComputeResponse result;

            if (runningOnRemoteCluster()) {
                // for remote executions - this ComputeResponse is created on the remote cluster/node and will be serialized and
                // received by the acquireCompute method callback on the coordinating cluster
                EsqlExecutionInfo.Cluster cluster = esqlExecutionInfo.getCluster(clusterAlias);
                result = new ComputeResponse(
                    collectedProfiles.isEmpty() ? List.of() : collectedProfiles.stream().toList(),
                    cluster.getTook(),
                    cluster.getTotalShards(),
                    cluster.getSuccessfulShards(),
                    cluster.getSkippedShards(),
                    cluster.getFailedShards()
                );
            } else {
                result = new ComputeResponse(collectedProfiles.isEmpty() ? List.of() : collectedProfiles.stream().toList());
                if (coordinatingClusterIsSearchedInCCS()) {
                    // mark local cluster as finished once the coordinator and all data nodes have finished processing
                    executionInfo.swapCluster(
                        RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY,
                        (k, v) -> new EsqlExecutionInfo.Cluster.Builder(v).setStatus(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL).build()
                    );
                }
            }
            delegate.onResponse(result);
        }, e -> delegate.onFailure(failureCollector.getFailure())));
    }

    /**
     * @return true if the "local" querying/coordinator cluster is being searched in a cross-cluster search
     */
    private boolean coordinatingClusterIsSearchedInCCS() {
        return esqlExecutionInfo != null
            && esqlExecutionInfo.isCrossClusterSearch()
            && esqlExecutionInfo.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY) != null;
    }

    /**
     * @return true if this Listener is running on a remote cluster (i.e., not the querying cluster)
     */
    private boolean runningOnRemoteCluster() {
        return whereRunning != null && whereRunning.equals(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY) == false;
    }

    /**
     * @return true if the listener is in a context where the took time needs to be recorded into the EsqlExecutionInfo
     */
    private boolean shouldRecordTookTime() {
        return runningOnRemoteCluster() || coordinatingClusterIsSearchedInCCS();
    }

    /**
     * @param computeClusterAlias the clusterAlias passed to the acquireCompute method
     * @return true if this listener is waiting for a remote response in a CCS search
     */
    private boolean isCCSListener(String computeClusterAlias) {
        return RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY.equals(whereRunning)
            && computeClusterAlias.equals(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY) == false;
    }

    /**
     * Acquires a new listener that doesn't collect result
     */
    ActionListener<Void> acquireAvoid() {
        return refs.acquire().delegateResponse((l, e) -> {
            failureCollector.unwrapAndCollect(e);
            try {
                if (cancelled.compareAndSet(false, true)) {
                    LOGGER.debug("cancelling ESQL task {} on failure", task);
                    transportService.getTaskManager().cancelTaskAndDescendants(task, "cancelled on failure", false, ActionListener.noop());
                }
            } finally {
                l.onFailure(e);
            }
        });
    }

    /**
     * Acquires a new listener that collects compute result. This listener will also collect warnings emitted during compute
     * @param computeClusterAlias The cluster alias where the compute is happening. Used when metadata needs to be gathered
     *                            into the {@link EsqlExecutionInfo} Cluster objects. Callers that do not required execution
     *                            info to be gathered (namely, the DataNodeRequestHandler ComputeListener) should pass in null.
     */
    ActionListener<ComputeResponse> acquireCompute(@Nullable String computeClusterAlias) {
        assert computeClusterAlias == null || (esqlExecutionInfo != null && queryStartTimeNanos > 0)
            : "When clusterAlias is provided to acquireCompute, executionInfo must be non-null and queryStartTimeNanos must be positive";

        return acquireAvoid().map(resp -> {
            responseHeaders.collect();
            var profiles = resp.getProfiles();
            if (profiles != null && profiles.isEmpty() == false) {
                collectedProfiles.addAll(profiles);
            }
            if (computeClusterAlias == null) {
                return null;
            }
            if (isCCSListener(computeClusterAlias)) {
                // this is the callback for the listener to the CCS compute
                esqlExecutionInfo.swapCluster(
                    computeClusterAlias,
                    (k, v) -> new EsqlExecutionInfo.Cluster.Builder(v)
                        // for now ESQL doesn't return partial results, so set status to SUCCESSFUL
                        .setStatus(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL)
                        .setTook(resp.getTook())
                        .setTotalShards(resp.getTotalShards())
                        .setSuccessfulShards(resp.getSuccessfulShards())
                        .setSkippedShards(resp.getSkippedShards())
                        .setFailedShards(resp.getFailedShards())
                        .build()
                );
            } else if (shouldRecordTookTime()) {
                // handler for this cluster's data node and coordinator completion (runs on "local" and remote clusters)
                TimeValue tookTime = new TimeValue(System.nanoTime() - queryStartTimeNanos, TimeUnit.NANOSECONDS);
                esqlExecutionInfo.swapCluster(computeClusterAlias, (k, v) -> {
                    if (v.getTook() == null || v.getTook().nanos() < tookTime.nanos()) {
                        return new EsqlExecutionInfo.Cluster.Builder(v).setTook(tookTime).build();
                    } else {
                        return v;
                    }
                });
            }
            return null;
        });
    }

    /**
     * Use this method when no execution metadata needs to be added to {@link EsqlExecutionInfo}
     */
    ActionListener<ComputeResponse> acquireCompute() {
        return acquireCompute(null);
    }

    @Override
    public void close() {
        refs.close();
    }
}
