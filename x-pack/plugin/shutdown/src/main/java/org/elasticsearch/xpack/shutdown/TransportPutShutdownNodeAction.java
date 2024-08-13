/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.shutdown;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.shutdown.PutShutdownNodeAction.Request;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

import static org.elasticsearch.cluster.routing.allocation.allocator.AllocationActionListener.rerouteCompletionIsNotRequired;

public class TransportPutShutdownNodeAction extends AcknowledgedTransportMasterNodeAction<Request> {
    private static final Logger logger = LogManager.getLogger(TransportPutShutdownNodeAction.class);

    private final AllocationService allocationService;
    private final MasterServiceTaskQueue<PutShutdownNodeTask> taskQueue;

    private static boolean putShutdownNodeState(
        Map<String, SingleNodeShutdownMetadata> shutdownMetadata,
        Predicate<String> nodeExists,
        Request request
    ) {
        if (isNoop(shutdownMetadata, request)) {
            return false;
        }

        final boolean nodeSeen = nodeExists.test(request.getNodeId());
        SingleNodeShutdownMetadata newNodeMetadata = SingleNodeShutdownMetadata.builder()
            .setNodeId(request.getNodeId())
            .setType(request.getType())
            .setReason(request.getReason())
            .setStartedAtMillis(System.currentTimeMillis())
            .setNodeSeen(nodeSeen)
            .setAllocationDelay(request.getAllocationDelay())
            .setTargetNodeName(request.getTargetNodeName())
            .setGracePeriod(request.getGracePeriod())
            .build();

        // log the update
        SingleNodeShutdownMetadata existingRecord = shutdownMetadata.get(request.getNodeId());
        if (existingRecord != null) {
            logger.info("updating existing shutdown record {} with new record {}", existingRecord, newNodeMetadata);
        } else {
            logger.info("creating shutdown record {}", newNodeMetadata);
        }

        shutdownMetadata.put(request.getNodeId(), newNodeMetadata);
        return true;
    }

    // package private for tests
    record PutShutdownNodeTask(Request request, ActionListener<AcknowledgedResponse> listener) implements ClusterStateTaskListener {
        @Override
        public void onFailure(Exception e) {
            if (MasterService.isPublishFailureException(e)) {
                logger.info(() -> "failed to put shutdown for node [" + request.getNodeId() + "], attempting retry", e);
            } else {
                logger.error(() -> "failed to put shutdown for node [" + request.getNodeId() + "]", e);
            }
            listener.onFailure(e);
        }
    }

    // package private for tests
    class PutShutdownNodeExecutor implements ClusterStateTaskExecutor<PutShutdownNodeTask> {
        @Override
        public ClusterState execute(BatchExecutionContext<PutShutdownNodeTask> batchExecutionContext) throws Exception {
            final var initialState = batchExecutionContext.initialState();
            var shutdownMetadata = new HashMap<>(initialState.metadata().nodeShutdowns().getAll());
            Predicate<String> nodeExistsPredicate = batchExecutionContext.initialState().getNodes()::nodeExists;
            boolean changed = false;
            boolean needsReroute = false;
            for (final var taskContext : batchExecutionContext.taskContexts()) {
                var request = taskContext.getTask().request();
                try (var ignored = taskContext.captureResponseHeaders()) {
                    changed |= putShutdownNodeState(shutdownMetadata, nodeExistsPredicate, request);
                } catch (Exception e) {
                    taskContext.onFailure(e);
                    continue;
                }
                switch (request.getType()) {
                    case REMOVE, SIGTERM, REPLACE -> needsReroute = true;
                }
                taskContext.success(() -> {
                    logger.trace(
                        () -> "finished registering node [" + request.getNodeId() + "] for shutdown of type [" + request.getType() + "]"
                    );
                    taskContext.getTask().listener.onResponse(AcknowledgedResponse.TRUE);
                });
            }
            if (changed == false) {
                return initialState;
            }

            final var updatedState = initialState.copyAndUpdateMetadata(
                b -> b.putCustom(NodesShutdownMetadata.TYPE, new NodesShutdownMetadata(shutdownMetadata))
            );

            if (needsReroute == false) {
                return updatedState;
            }

            // Reroute inline with the update, rather than using the RerouteService, in order to atomically update things like auto-expand
            // replicas to account for the shutdown metadata. If the reroute were separate then the get-shutdown API might observe the
            // intermediate state and report that nodes are ready to shut down prematurely. Even if the client were to wait for the
            // put-shutdown API to complete there's a risk that it gets disconnected and retries, but the retry could well be a no-op which
            // short-circuits past the cluster state update and therefore also doesn't wait for the background reroute.
            return allocationService.reroute(updatedState, "reroute after put-shutdown", rerouteCompletionIsNotRequired());
        }
    }

    @Inject
    public TransportPutShutdownNodeAction(
        TransportService transportService,
        ClusterService clusterService,
        AllocationService allocationService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            PutShutdownNodeAction.NAME,
            false,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            Request::readFrom,
            indexNameExpressionResolver,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.allocationService = allocationService;
        taskQueue = clusterService.createTaskQueue("put-shutdown", Priority.URGENT, new PutShutdownNodeExecutor());
    }

    @Override
    protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<AcknowledgedResponse> listener) {
        if (isNoop(state.getMetadata().nodeShutdowns().getAll(), request)) {
            listener.onResponse(AcknowledgedResponse.TRUE);
            return;
        }
        taskQueue.submitTask(
            "put-node-shutdown-" + request.getNodeId(),
            new PutShutdownNodeTask(request, listener),
            request.masterNodeTimeout()
        );
    }

    private static boolean isNoop(Map<String, SingleNodeShutdownMetadata> shutdownMetadata, Request request) {
        var existing = shutdownMetadata.get(request.getNodeId());
        return existing != null
            && existing.getType().equals(request.getType())
            && existing.getReason().equals(request.getReason())
            && Objects.equals(existing.getAllocationDelay(), request.getAllocationDelay())
            && Objects.equals(existing.getTargetNodeName(), request.getTargetNodeName());
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
