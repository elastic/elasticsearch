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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.session.Configuration;

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
 * MP TODO: update javadoc to include changes for CCS telemetry collection
 */
final class ComputeListener implements Releasable {
    private static final Logger LOGGER = LogManager.getLogger(ComputeService.class);

    private final RefCountingListener refs;
    // MP TODO: what is this failureCollector for? should I use it for CCS metadata?
    private final FailureCollector failureCollector = new FailureCollector();
    private final AtomicBoolean cancelled = new AtomicBoolean();
    private final CancellableTask task;
    private final TransportService transportService;
    private final List<DriverProfile> collectedProfiles;
    private final ResponseHeadersCollector responseHeaders;

    private final EsqlExecutionInfo esqlExecutionInfo;

    // for use by top level ComputeListener (spanning all clusters) (???)
    ComputeListener(
        TransportService transportService,
        CancellableTask task,
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
            var result = new ComputeResponse(collectedProfiles.isEmpty() ? List.of() : collectedProfiles.stream().toList());
            delegate.onResponse(result);
        }, e -> delegate.onFailure(failureCollector.getFailure())));
        // MP TODO: ^^ do we need to instrument the above onFailure handler?
    }

    // this ctor is for use on remote nodes only
    ComputeListener(
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
        // for remote executions - this ComputeResponse is created on the remote cluster/node and will be serialized back and
        // received by the acquireCompute method callback on the coordinating cluster
        this.refs = new RefCountingListener(1, ActionListener.wrap(ignored -> {
            responseHeaders.finish();
            EsqlExecutionInfo.Cluster cluster = esqlExecutionInfo.getCluster(clusterAlias);
            System.err.printf("* * * * * * * Creating ComputeResponse in ComputeListener refs for cluster: [%s]\n", cluster);
            var result = new ComputeResponse(
                collectedProfiles.isEmpty() ? List.of() : collectedProfiles.stream().toList(),
                cluster.getTook(),
                cluster.getTotalShards(),
                cluster.getSuccessfulShards(),
                cluster.getSkippedShards(),
                cluster.getFailedShards()
            );
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
    ActionListener<ComputeResponse> acquireComputeForDataNodes() {
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
            System.err.println("CCC acquireCCSCompute: TOTAL SHARDS n acquireCompute: " + resp.getTotalShards());
            System.err.printf(
                "CCC acquireCCSCompute: Cluster [%s] before swapping: %s\n",
                clusterAlias,
                esqlExecutionInfo.getCluster(clusterAlias)
            );
            esqlExecutionInfo.swapCluster(
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
            System.err.printf("CCC acquireCCSCompute:: Cluster AFTER swapping: [%s]\n", esqlExecutionInfo.getCluster(clusterAlias));
            return null;
        });
    }

    /**
     * Acts like {@code acquireCompute} for handling the response(s) form the runComputeOnDataNodes
     * phase. Per-cluster took time is recorded in the {@link EsqlExecutionInfo}.
     * @param clusterAlias remote cluster alias the data node compute is running on
     */
    ActionListener<ComputeResponse> acquireComputeForDataNodes(String clusterAlias, Configuration configuration) {
        assert clusterAlias != null : "Must provide non-null cluster alias to acquireCompute";
        return acquireAvoid().map(resp -> {
            responseHeaders.collect();
            var profiles = resp.getProfiles();
            if (profiles != null && profiles.isEmpty() == false) {
                collectedProfiles.addAll(profiles);
            }
            System.err.println("VVV acquireCompute: TOTAL SHARDS n acquireCompute: " + resp.getTotalShards());
            System.err.printf(
                "VVV acquireCompute: Cluster [%s] before swapping: %s\n",
                clusterAlias,
                esqlExecutionInfo.getCluster(clusterAlias)
            );
            long tookTimeMillis = System.currentTimeMillis() - configuration.getQueryStartTimeMillis();
            TimeValue tookOnDataNode = new TimeValue(tookTimeMillis);
            System.err.println("VVV acquireCompute: tookOnDataNode: " + tookOnDataNode + "  <---------------");
            esqlExecutionInfo.swapCluster(
                clusterAlias,
                // MP TODO: Not sure this is right - is this called only after the can_match? If yes, then this is not done running
                // MP TODO: how do we get a callback that it is finished overall after the DataNodeRequestHandler part has run?
                (k, v) -> {
                    if (v.getTook() == null || v.getTook().millis() < tookOnDataNode.millis()) {
                        return new EsqlExecutionInfo.Cluster.Builder(v).setStatus(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL)
                            .setTook(tookOnDataNode)
                            .build();
                    } else {
                        // other data node had higher took time, so keep the current value
                        return v;
                    }
                }
            );
            System.err.printf("VVV acquireCompute:: Cluster AFTER swapping: [%s]\n", esqlExecutionInfo.getCluster(clusterAlias));
            return null;
        });
    }

    @Override
    public void close() {
        refs.close();
    }
}
