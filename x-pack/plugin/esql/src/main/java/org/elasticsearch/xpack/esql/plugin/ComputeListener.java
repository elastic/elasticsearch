/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.compute.operator.FailureCollector;
import org.elasticsearch.compute.operator.ResponseHeadersCollector;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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

    public static ComputeListener createComputeListener(
        TransportService transportService,
        CancellableTask task,
        EsqlExecutionInfo executionInfo,
        ActionListener<ComputeResponse> delegate
    ) {
        return new ComputeListener(transportService, task, null, executionInfo, delegate);
    }

    /**
     * Create a ComputeListener, specifying a clusterAlias. For use on remote clusters in the ComputeService.ClusterRequestHandler.
     * The final ComputeResponse that is sent back to the querying cluster will have metadata about the search in the ComputeResponse:
     * took time, and shard "accounting" (total, successful, skipped, failed),
     * @param clusterAlias alias of the remote cluster on which the remote query is being done
     * @param transportService
     * @param task
     * @param executionInfo to accumulate metadata about the search
     * @param delegate
     */
    public static ComputeListener createOnRemote(
        String clusterAlias,
        TransportService transportService,
        CancellableTask task,
        EsqlExecutionInfo executionInfo,
        ActionListener<ComputeResponse> delegate
    ) {
        return new ComputeListener(transportService, task, clusterAlias, executionInfo, delegate);
    }

    private ComputeListener(
        TransportService transportService,
        CancellableTask task,
        String clusterAlias,
        EsqlExecutionInfo executionInfo,
        ActionListener<ComputeResponse> delegate
    ) {
        this.transportService = transportService;
        this.task = task;
        this.responseHeaders = new ResponseHeadersCollector(transportService.getThreadPool().getThreadContext());
        this.collectedProfiles = Collections.synchronizedList(new ArrayList<>());
        this.esqlExecutionInfo = executionInfo;
        this.refs = new RefCountingListener(1, ActionListener.wrap(ignored -> {
            responseHeaders.finish();
            ComputeResponse result;
            if (clusterAlias == null) {
                result = new ComputeResponse(collectedProfiles.isEmpty() ? List.of() : collectedProfiles.stream().toList());
            } else {
                // for remote executions - this ComputeResponse is created on the remote cluster/node and will be serialized back and
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
            }
            delegate.onResponse(result);
        }, e -> delegate.onFailure(failureCollector.getFailure())));
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
     */
    ActionListener<ComputeResponse> acquireCompute() {
        return acquireAvoid().map(resp -> {
            responseHeaders.collect();
            var profiles = resp.getProfiles();
            if (profiles != null && profiles.isEmpty() == false) {
                collectedProfiles.addAll(profiles);
            }
            return null;
        });
    }

    /**
     * Acts like {@code acquireCompute} handling the response(s) from the runComputeOnDataNodes
     * phase. Per-cluster took time is recorded in the {@link EsqlExecutionInfo} and status is
     * set to SUCCESSFUL when the CountDown reaches zero.
     * @param clusterAlias remote cluster alias the data node compute is running on
     * @param queryStartTimeMillis start time (on coordinating cluster) for computing took time (per cluster)
     * @param countDown counter of number of data nodes to wait for before changing cluster status from RUNNING to SUCCESSFUL
     */
    ActionListener<ComputeResponse> acquireComputeForDataNodes(String clusterAlias, long queryStartTimeMillis, CountDown countDown) {
        assert clusterAlias != null : "Must provide non-null cluster alias to acquireCompute";
        return acquireAvoid().map(resp -> {
            responseHeaders.collect();
            var profiles = resp.getProfiles();
            if (profiles != null && profiles.isEmpty() == false) {
                collectedProfiles.addAll(profiles);
            }
            long tookTimeMillis = System.currentTimeMillis() - queryStartTimeMillis;
            TimeValue tookOnDataNode = new TimeValue(tookTimeMillis);
            esqlExecutionInfo.swapCluster(clusterAlias, (k, v) -> {
                EsqlExecutionInfo.Cluster.Builder builder = new EsqlExecutionInfo.Cluster.Builder(v);
                if (v.getTook() == null || v.getTook().millis() < tookOnDataNode.millis()) {
                    builder.setTook(tookOnDataNode);
                }
                if (countDown.countDown()) {
                    builder.setStatus(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL);
                }
                return builder.build();
            });
            return null;
        });
    }

    /**
     * The ActionListener to be used on the coordinating cluster when sending a cross-cluster
     * compute request to a remote cluster.
     * @param clusterAlias clusterAlias of cluster receiving the remote compute request
     * @return Listener that will fill in all metadata from to remote cluster into the
     *         {@link EsqlExecutionInfo}  for the clusterAlias cluster.
     */
    ActionListener<ComputeResponse> acquireCCSCompute(String clusterAlias) {
        assert clusterAlias != null : "Must provide non-null cluster alias to acquireCompute";
        assert esqlExecutionInfo != null : "When providing cluster alias to acquireCompute, EsqlExecutionInfo must not be null";
        return acquireAvoid().map(resp -> {
            responseHeaders.collect();
            var profiles = resp.getProfiles();
            if (profiles != null && profiles.isEmpty() == false) {
                collectedProfiles.addAll(profiles);
            }
            esqlExecutionInfo.swapCluster(
                clusterAlias,
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
            return null;
        });
    }

    @Override
    public void close() {
        refs.close();
    }
}
