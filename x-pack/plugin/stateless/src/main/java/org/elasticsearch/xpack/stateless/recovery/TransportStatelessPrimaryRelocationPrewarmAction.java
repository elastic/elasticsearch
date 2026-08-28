/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.recovery.StatelessPrimaryRelocationAction;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.concurrent.Executor;

/// [TransportAction] for the prewarm phase of a stateless primary relocation.
///
/// Invoked by [StatelessPrimaryRelocationSourceService] (on the source node), request goes to the target node. The
/// target-side handler delegates to [StatelessPrimaryRelocationTargetService].
public class TransportStatelessPrimaryRelocationPrewarmAction extends TransportAction<
    TransportStatelessPrimaryRelocationPrewarmAction.PrewarmRequest,
    ActionResponse.Empty> {

    public static final ActionType<ActionResponse.Empty> TYPE = new ActionType<>(
        "internal:index/shard/recovery/stateless_primary_relocation/prewarm_action"
    );

    /// Legacy name from before the transport action split for backward compatibility.
    public static final String PREWARM_RELOCATION_ACTION_NAME = StatelessPrimaryRelocationAction.TYPE.name() + "/prewarm";

    private final TransportService transportService;
    private final Executor recoveryExecutor;

    @Inject
    public TransportStatelessPrimaryRelocationPrewarmAction(
        TransportService transportService,
        ActionFilters actionFilters,
        StatelessPrimaryRelocationTargetService primaryRelocationTargetService
    ) {
        super(TYPE.name(), actionFilters, transportService.getTaskManager(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.transportService = transportService;
        this.recoveryExecutor = transportService.getThreadPool().generic();

        transportService.registerRequestHandler(
            PREWARM_RELOCATION_ACTION_NAME,
            recoveryExecutor,
            false, // forceExecution
            false, // canTripCircuitBreaker
            Request::new,
            (request, channel, task) -> primaryRelocationTargetService.handlePrewarmRelocation(
                request,
                new ChannelActionListener<>(channel).map(ignored -> ActionResponse.Empty.INSTANCE)
            )
        );
    }

    /// Runs on the source node. The request already carries the original relocation task as its parent, so forwarding
    /// it directly preserves the expected task linkage on the target node.
    @Override
    protected void doExecute(Task task, PrewarmRequest request, ActionListener<ActionResponse.Empty> listener) {
        final var transportRequest = request.request();
        transportRequest.copyFieldsFrom(request);
        transportService.sendRequest(
            request.targetNode(),
            PREWARM_RELOCATION_ACTION_NAME,
            transportRequest,
            new ActionListenerResponseHandler<>(listener, in -> ActionResponse.Empty.INSTANCE, recoveryExecutor)
        );
    }

    static class PrewarmRequest extends ActionRequest {

        private final DiscoveryNode targetNode;
        private final Request request;

        PrewarmRequest(DiscoveryNode targetNode, Request request) {
            this.targetNode = targetNode;
            this.request = request;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            TransportAction.localOnly();
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public DiscoveryNode targetNode() {
            return targetNode;
        }

        public Request request() {
            return request;
        }
    }

    static class Request extends AbstractTransportRequest {

        private final ShardId shardId;
        private final BlobFileWithLength latestBccBlob;
        private final boolean hasRecentIdLookup;

        Request(ShardId shardId, BlobFileWithLength latestBccBlob, boolean hasRecentIdLookup) {
            this.shardId = shardId;
            this.latestBccBlob = latestBccBlob;
            this.hasRecentIdLookup = hasRecentIdLookup;
        }

        Request(StreamInput in) throws IOException {
            super(in);
            this.shardId = new ShardId(in);
            this.latestBccBlob = new BlobFileWithLength(in);
            this.hasRecentIdLookup = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
            latestBccBlob.writeTo(out);
            out.writeBoolean(hasRecentIdLookup);
        }

        public ShardId shardId() {
            return shardId;
        }

        public BlobFileWithLength latestBccBlob() {
            return latestBccBlob;
        }

        public boolean hasRecentIdLookup() {
            return hasRecentIdLookup;
        }
    }
}
