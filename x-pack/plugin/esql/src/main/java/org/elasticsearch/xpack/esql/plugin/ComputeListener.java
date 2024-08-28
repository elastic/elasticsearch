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
import org.elasticsearch.core.Releasable;
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
 *
 * MP TODO: update docs around changes for CCS telemetry collection
 */
final class ComputeListener implements Releasable {
    private static final Logger LOGGER = LogManager.getLogger(ComputeService.class);

    private final RefCountingListener refs;
    // MP TODO: what is this failureCollector for? how can I use it for CCS metadata?
    private final FailureCollector failureCollector = new FailureCollector();
    private final AtomicBoolean cancelled = new AtomicBoolean();
    private final CancellableTask task;
    private final TransportService transportService;
    private final List<DriverProfile> collectedProfiles;
    private final ResponseHeadersCollector responseHeaders;

    ComputeListener(TransportService transportService, CancellableTask task, ActionListener<ComputeResponse> delegate) {
        this.transportService = transportService;
        this.task = task;
        this.responseHeaders = new ResponseHeadersCollector(transportService.getThreadPool().getThreadContext());
        this.collectedProfiles = Collections.synchronizedList(new ArrayList<>());
        this.refs = new RefCountingListener(1, ActionListener.wrap(ignored -> {
            responseHeaders.finish();
            // MP TODO: I don't understand what this ComputeResponse is for, why it is exists, when it is created, nor where it goes ...
            var result = new ComputeResponse(collectedProfiles.isEmpty() ? List.of() : collectedProfiles.stream().toList());
            delegate.onResponse(result);
        }, e -> delegate.onFailure(failureCollector.getFailure())));
        // MP TODO: ^^ do we need to instrument the above onFailure handler?
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
     * Acquires a new listener that collects compute result. This listener will also collects warnings emitted during compute
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
     * Acts like {@code acquireCompute} but also updates {@link EsqlExecutionInfo} with metadata about the search
     * on the remote cluster
     * @param clusterAlias remote cluster alias the compute is running on
     */
    ActionListener<ComputeResponse> acquireCompute(String clusterAlias, EsqlExecutionInfo executionInfo) {
        assert clusterAlias != null : "Must provide non-null cluster alias to acquireCompute";
        assert executionInfo != null : "When providing cluster alias to acquireCompute, EsqlExecutionInfo must not be null";
        return acquireAvoid().map(resp -> {
            responseHeaders.collect();
            var profiles = resp.getProfiles();
            if (profiles != null && profiles.isEmpty() == false) {
                collectedProfiles.addAll(profiles);
            }
            System.err.printf("UUU: swapping-in (in acquireCompute) for cluster: [%s]\n", clusterAlias);
            executionInfo.swapCluster(
                clusterAlias,
                (k, v) -> new EsqlExecutionInfo.Cluster.Builder(v)
                    // MP TODO: if we get here does that mean that the remote search is finished and was SUCCESSFUL?
                    // MP TODO: if yes, where does the failure path go - how do we update the ExecutionInfo with failure info?
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
