/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.reshard;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.RetryableAction;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

public class TransportReshardSplitAction extends TransportAction<TransportReshardSplitAction.SplitRequest, ActionResponse> {

    public static final ActionType<ActionResponse> TYPE = new ActionType<>("indices:admin/reshard/split");

    public static final String START_SPLIT_ACTION_NAME = TYPE.name() + "/start";
    public static final String SPLIT_HANDOFF_ACTION_NAME = TYPE.name() + "/handoff";

    private final TransportService transportService;
    private final Executor recoveryExecutor;
    private final IndicesService indicesService;
    private final SplitSourceService splitSourceService;
    private final SplitTargetService splitTargetService;

    @Inject
    public TransportReshardSplitAction(
        TransportService transportService,
        ActionFilters actionFilters,
        IndicesService indicesService,
        SplitSourceService splitSourceService,
        SplitTargetService splitTargetService
    ) {
        super(TYPE.name(), actionFilters, transportService.getTaskManager(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.transportService = transportService;
        this.recoveryExecutor = transportService.getThreadPool().generic();
        this.indicesService = indicesService;
        this.splitSourceService = splitSourceService;
        this.splitTargetService = splitTargetService;

        transportService.registerRequestHandler(
            START_SPLIT_ACTION_NAME,
            recoveryExecutor,
            false, // forceExecution
            false, // canTripCircuitBreaker
            Request::new,
            (request, channel, task) -> handleStartSplitOnSource(
                task,
                request,
                new ChannelActionListener<>(channel).map(ignored -> ActionResponse.Empty.INSTANCE)
            )
        );

        transportService.registerRequestHandler(
            SPLIT_HANDOFF_ACTION_NAME,
            recoveryExecutor,
            false, // forceExecution
            false, // canTripCircuitBreaker
            Request::new,
            (request, channel, task) -> handleSplitHandoffOnTarget(
                request,
                new ChannelActionListener<>(channel).map(ignored -> ActionResponse.Empty.INSTANCE)
            )
        );
    }

    @Override
    protected void doExecute(Task task, SplitRequest request, ActionListener<ActionResponse> listener) {
        SplitTargetService.Split split = request.split;
        Request childRequest = new Request(
            split.shardId(),
            split.sourceNode(),
            split.targetNode(),
            split.sourcePrimaryTerm(),
            split.targetPrimaryTerm()
        );
        transportService.sendChildRequest(
            split.sourceNode(),
            START_SPLIT_ACTION_NAME,
            childRequest,
            task,
            TransportRequestOptions.EMPTY,
            new ActionListenerResponseHandler<>(
                listener.map(ignored -> ActionResponse.Empty.INSTANCE),
                in -> ActionResponse.Empty.INSTANCE,
                recoveryExecutor
            )
        );
    }

    private void handleStartSplitOnSource(Task task, Request request, ActionListener<ActionResponse.Empty> listener) {
        assert task instanceof CancellableTask : "not cancellable";

        SubscribableListener.<Releasable>newForked(
            l -> splitSourceService.setupTargetShard(
                (CancellableTask) task,
                request.shardId,
                request.sourcePrimaryTerm,
                request.targetPrimaryTerm,
                l
            )
        ).<ActionResponse>andThen((l, releasable) -> {
            /*
             Install a cluster state observer to observe handoff or source/target shard primary term change and
             complete the listener accordingly. The retryable action below, retries requests for HANDOFF until this
             cluster state observer sets shouldRetry to false.
                ┌─────────────────────┐
                │ RetryableAction     │───► sends HANDOFF RPCs
                │ (transport layer)   │
                └─────────▲───────────┘
                          │ shouldRetry
                ┌─────────┴───────────┐
                │ ClusterStateObserver│───► decides success/failure
                │ (metadata truth)    │
                └─────────────────────┘
             */
            AtomicBoolean shouldRetry = new AtomicBoolean(true);  // should retry until cluster state converges
            splitSourceService.waitForHandoffSuccessOrFailure(
                request.shardId,
                request.targetPrimaryTerm,
                request.sourcePrimaryTerm,
                shouldRetry,
                releasable,
                l
            );

            // retry SPLIT_HANDOFF_ACTION until cluster state converges (shouldRetry == false)
            new RetryableAction<ActionResponse.Empty>(
                logger,
                transportService.getThreadPool(),
                TimeValue.timeValueMillis(10),  // initial delay
                TimeValue.timeValueSeconds(1),  // max delay bound
                TimeValue.MAX_VALUE,      // no timeout
                ActionListener.noop(),
                EsExecutors.DIRECT_EXECUTOR_SERVICE
            ) {
                @Override
                public void tryAction(ActionListener<ActionResponse.Empty> retryListener) {
                    logger.info("Source sending HANDOFF request to target {}", request.shardId);
                    transportService.sendChildRequest(
                        request.targetNode,
                        SPLIT_HANDOFF_ACTION_NAME,
                        request,
                        (CancellableTask) task,
                        TransportRequestOptions.EMPTY,
                        new ActionListenerResponseHandler<>(
                            retryListener,
                            in -> ActionResponse.Empty.INSTANCE,
                            EsExecutors.DIRECT_EXECUTOR_SERVICE
                        )
                    );
                }

                @Override
                public boolean shouldRetry(Exception e) {
                    return shouldRetry.get();
                }

                @Override
                public void onFinished() {
                    logger.debug("Handoff retry action finished for {}", request.shardId);
                }
            }.run();
        })
            .addListener(
                ActionListener.runBefore(
                    listener.map(ignored -> ActionResponse.Empty.INSTANCE),
                    () -> logger.debug("START_SPLIT complete on source")
                )
            );
    }

    private void handleSplitHandoffOnTarget(Request request, ActionListener<Void> listener) {
        var indexService = indicesService.indexServiceSafe(request.shardId.getIndex());
        var indexShard = indexService.getShard(request.shardId.id());
        splitTargetService.acceptHandoff(indexShard, request, listener);
    }

    public static class SplitRequest extends ActionRequest {

        private final SplitTargetService.Split split;

        public SplitRequest(SplitTargetService.Split split) {
            this.split = split;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            TransportAction.localOnly();
        }
    }

    public static class Request extends ActionRequest {

        private final ShardId shardId;
        private final DiscoveryNode sourceNode;
        private final DiscoveryNode targetNode;
        private final long sourcePrimaryTerm;
        private final long targetPrimaryTerm;

        Request(ShardId shardId, DiscoveryNode sourceNode, DiscoveryNode targetNode, long sourcePrimaryTerm, long targetPrimaryTerm) {
            this.shardId = shardId;
            this.sourceNode = sourceNode;
            this.targetNode = targetNode;
            this.sourcePrimaryTerm = sourcePrimaryTerm;
            this.targetPrimaryTerm = targetPrimaryTerm;
        }

        Request(StreamInput in) throws IOException {
            super(in);
            shardId = new ShardId(in);
            sourceNode = new DiscoveryNode(in);
            targetNode = new DiscoveryNode(in);
            sourcePrimaryTerm = in.readVLong();
            targetPrimaryTerm = in.readVLong();
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
            sourceNode.writeTo(out);
            targetNode.writeTo(out);
            out.writeVLong(sourcePrimaryTerm);
            out.writeVLong(targetPrimaryTerm);
        }

        public ShardId shardId() {
            return shardId;
        }

        public DiscoveryNode sourceNode() {
            return sourceNode;
        }

        public DiscoveryNode targetNode() {
            return targetNode;
        }

        public long sourcePrimaryTerm() {
            return sourcePrimaryTerm;
        }

        public long targetPrimaryTerm() {
            return targetPrimaryTerm;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, getDescription(), parentTaskId, headers);
        }

        @Override
        public String getDescription() {
            return Strings.format("start-split-request-%d", shardId.id());
        }
    }
}
