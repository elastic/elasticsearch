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

package co.elastic.elasticsearch.stateless.reshard;

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
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexReshardingState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.concurrent.Executor;

public class TransportReshardSplitAction extends TransportAction<TransportReshardSplitAction.SplitRequest, ActionResponse> {

    public static final ActionType<ActionResponse> TYPE = new ActionType<>("indices:admin/reshard/split");

    public static final String START_SPLIT_ACTION_NAME = TYPE.name() + "/start";
    public static final String SPLIT_HANDOFF_ACTION_NAME = TYPE.name() + "/handoff";

    private final TransportService transportService;
    private final Client client;
    private final Executor recoveryExecutor;
    private final SplitSourceService splitSourceService;

    @Inject
    public TransportReshardSplitAction(
        Client client,
        TransportService transportService,
        ActionFilters actionFilters,
        SplitSourceService splitSourceService
    ) {
        super(TYPE.name(), actionFilters, transportService.getTaskManager(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.client = client;
        this.transportService = transportService;
        this.recoveryExecutor = transportService.getThreadPool().generic();
        this.splitSourceService = splitSourceService;

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
        SubscribableListener.<Releasable>newForked(
            l -> splitSourceService.setupTargetShard(request.shardId, request.sourcePrimaryTerm, request.targetPrimaryTerm, l)
        )
            .<ActionResponse>andThen(
                // Finally, initiate handoff.
                (l, releasable) -> SubscribableListener.<ActionResponse>newForked(
                    afterHandoff -> transportService.sendChildRequest(
                        request.targetNode,
                        SPLIT_HANDOFF_ACTION_NAME,
                        request,
                        task,
                        TransportRequestOptions.EMPTY,
                        new ActionListenerResponseHandler<>(
                            ActionListener.runBefore(afterHandoff, () -> logger.debug("handoff to target {} complete", request.shardId)),
                            in -> ActionResponse.Empty.INSTANCE,
                            EsExecutors.DIRECT_EXECUTOR_SERVICE
                        )
                    )
                ).addListener(ActionListener.runBefore(l, () -> {
                    logger.debug("handoff attempt completed, releasing permits");
                    releasable.close();
                }))
            )
            // we are only interested in success/failure, the response is empty
            .addListener(listener.map(ignored -> ActionResponse.Empty.INSTANCE));
    }

    private void handleSplitHandoffOnTarget(Request request, ActionListener<Void> listener) {
        SplitStateRequest splitStateRequest = new SplitStateRequest(
            request.shardId,
            IndexReshardingState.Split.TargetShardState.HANDOFF,
            request.sourcePrimaryTerm,
            request.targetPrimaryTerm
        );
        client.execute(TransportUpdateSplitStateAction.TYPE, splitStateRequest, listener.map(r -> null));
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

    static class Request extends ActionRequest {

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
    }
}
