/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.UntypedActionRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.recovery.StatelessPrimaryRelocationAction;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.concurrent.Executor;

/// [TransportAction] for the prewarm phase of a stateless primary relocation.
///
/// Invoked by [StatelessPrimaryRelocationSourceService] (on the source node), request goes to the target node. The
/// target-side handler delegates to [StatelessPrimaryRelocationTargetService].
public class TransportStatelessPrimaryRelocationPrewarmAction extends TransportAction<
    TransportStatelessPrimaryRelocationPrewarmAction.Request,
    ActionResponse.Empty> {

    private static final Logger logger = LogManager.getLogger(TransportStatelessPrimaryRelocationPrewarmAction.class);

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

    /// Runs on the source node. Forwards the prewarm request to the target node using `sendChildRequest` so that
    /// the prewarm task is correctly linked as a child of the active relocation task.
    @Override
    protected void doExecute(Task task, Request request, ActionListener<ActionResponse.Empty> listener) {
        transportService.sendChildRequest(
            request.targetNode(),
            PREWARM_RELOCATION_ACTION_NAME,
            request,
            task,
            TransportRequestOptions.EMPTY,
            // Prewarm failures are non-fatal, the relocation continues without the benefit of prewarming.
            new ActionListenerResponseHandler<>(listener.delegateResponse((l, e) -> {
                logger.debug(() -> Strings.format("%s ignoring prewarm action failure", request.shardId()), e);
                l.onFailure(e);
            }), in -> ActionResponse.Empty.INSTANCE, recoveryExecutor)
        );
    }

    static class Request extends UntypedActionRequest {

        private final DiscoveryNode targetNode;
        private final ShardId shardId;
        private final BlobFileWithLength latestBccBlob;
        private final boolean hasRecentIdLookup;

        Request(DiscoveryNode targetNode, ShardId shardId, BlobFileWithLength latestBccBlob, boolean hasRecentIdLookup) {
            this.targetNode = targetNode;
            this.shardId = shardId;
            this.latestBccBlob = latestBccBlob;
            this.hasRecentIdLookup = hasRecentIdLookup;
        }

        Request(StreamInput in) throws IOException {
            super(in);
            this.targetNode = null;
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

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public DiscoveryNode targetNode() {
            return targetNode;
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
