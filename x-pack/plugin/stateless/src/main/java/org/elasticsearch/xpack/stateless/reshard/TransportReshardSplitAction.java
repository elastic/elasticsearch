/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
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
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Releasable;
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
        )
            .<ActionResponse>andThen(
                // Finally, initiate handoff.
                (l, releasable) -> SubscribableListener.<ActionResponse>newForked(afterHandoff -> {
                    logger.info("Source sending HANDOFF request to target {}", request.shardId);
                    transportService.sendChildRequest(
                        request.targetNode,
                        SPLIT_HANDOFF_ACTION_NAME,
                        request,
                        task,
                        TransportRequestOptions.EMPTY,
                        new ActionListenerResponseHandler<>(new ActionListener<>() {

                            @Override
                            public void onResponse(ActionResponse.Empty response) {
                                logger.info("SPLIT_HANDOFF to target {} complete", request.shardId);
                                afterHandoff.onResponse(response);
                            }

                            @Override
                            public void onFailure(Exception e) {
                                // CUSTOM HANDOFF EXCEPTION HANDLING
                                logger.error(Strings.format("SPLIT_HANDOFF to target {} FAILED, release permits", request.shardId), e);
                                releasable.close();
                                // propagate failure back up the async chain
                                afterHandoff.onFailure(e);
                            }
                        }, in -> ActionResponse.Empty.INSTANCE, EsExecutors.DIRECT_EXECUTOR_SERVICE)
                    );
                    // Make sure HANDOFF is observed on the source node before releasing permits. Otherwise,
                    // requests can get routed incorrectly after target is actually in HANDOFF state.
                }).andThen(afterHandoffObserved -> {
                    logger.info("Source waiting to see HANDOFF state for target {}", request.shardId);
                    splitSourceService.waitForTargetShardHandoff(request.shardId, ActionListener.wrap(ignored -> {
                        logger.debug("Cluster state indicates HANDOFF complete; releasing permits for {}", request.shardId);
                        releasable.close();
                        afterHandoffObserved.onResponse(null);
                    }, e -> {
                        logger.error(Strings.format("Failed while waiting to see HANDOFF state for {}", request.shardId), e);
                        releasable.close();
                        afterHandoffObserved.onFailure(e);
                    }));
                }).addListener(l.map(ignored -> ActionResponse.Empty.INSTANCE))
            )
            // we are only interested in success/failure, the response is empty
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
