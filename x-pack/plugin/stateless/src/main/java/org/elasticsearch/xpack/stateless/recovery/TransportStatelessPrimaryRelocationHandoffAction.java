/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.elasticsearch.TransportVersion;
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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.index.seqno.RetentionLeases;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.StatelessPrimaryRelocationAction;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.stateless.commits.BlobFile;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService.RecoveryInfoFromSource;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

/// [TransportAction] for the primary-context handoff phase of a stateless primary relocation.
///
/// Invoked by [StatelessPrimaryRelocationSourceService] (on the source node), request goes to the target node. The
/// target-side handler delegates to [StatelessPrimaryRelocationTargetService].
public class TransportStatelessPrimaryRelocationHandoffAction extends TransportAction<
    TransportStatelessPrimaryRelocationHandoffAction.Request,
    ActionResponse.Empty> {

    public static final ActionType<ActionResponse.Empty> TYPE = new ActionType<>(
        "internal:index/shard/recovery/stateless_primary_relocation/handoff_action"
    );

    private static final TransportVersion STATELESS_PRIMARY_HANDOFF_LATEST_BLOBS = TransportVersion.fromName(
        "stateless_primary_handoff_latest_blobs"
    );

    /// Legacy name from before the transport action split for backward compatibility.
    public static final String PRIMARY_CONTEXT_HANDOFF_ACTION_NAME = StatelessPrimaryRelocationAction.TYPE.name()
        + "/primary_context_handoff";

    private final TransportService transportService;
    private final Executor recoveryExecutor;

    @Inject
    public TransportStatelessPrimaryRelocationHandoffAction(
        TransportService transportService,
        ActionFilters actionFilters,
        PeerRecoveryTargetService peerRecoveryTargetService,
        StatelessPrimaryRelocationTargetService primaryRelocationTargetService
    ) {
        super(TYPE.name(), actionFilters, transportService.getTaskManager(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.transportService = transportService;
        this.recoveryExecutor = transportService.getThreadPool().generic();

        transportService.registerRequestHandler(
            PRIMARY_CONTEXT_HANDOFF_ACTION_NAME,
            recoveryExecutor,
            false, // forceExecution
            false, // canTripCircuitBreaker
            Request::new,
            (request, channel, task) -> {
                final var recoveryRef = peerRecoveryTargetService.getRecoveryRef(request.recoveryId(), request.shardId());
                boolean listenerOwnsRef = false;
                try {
                    primaryRelocationTargetService.handlePrimaryContextHandoff(
                        request,
                        ActionListener.releaseAfter(
                            new ChannelActionListener<>(channel).map(ignored -> ActionResponse.Empty.INSTANCE),
                            recoveryRef
                        )
                    );
                    listenerOwnsRef = true;
                } finally {
                    if (listenerOwnsRef == false) {
                        recoveryRef.close();
                    }
                }
            }
        );
    }

    /// Runs on the source node. Forwards the handoff request to the target node using `sendChildRequest` so that
    /// the handoff task is correctly linked as a child of the active relocation task.
    @Override
    protected void doExecute(Task task, Request request, ActionListener<ActionResponse.Empty> listener) {
        transportService.sendChildRequest(
            request.targetNode(),
            PRIMARY_CONTEXT_HANDOFF_ACTION_NAME,
            request,
            task,
            TransportRequestOptions.EMPTY,
            new ActionListenerResponseHandler<>(listener, in -> ActionResponse.Empty.INSTANCE, recoveryExecutor)
        );
    }

    static class Request extends UntypedActionRequest {

        private final DiscoveryNode targetNode;

        private final long recoveryId;
        private final ShardId shardId;
        private final ReplicationTracker.PrimaryContext primaryContext;
        private final RetentionLeases retentionLeases;
        private final Map<PrimaryTermAndGeneration, Set<String>> searchNodesPerCommit;
        @Nullable
        private final BlobFileWithLength latestBccBlob;
        private final Set<BlobFile> otherBlobFiles;
        private final boolean hasRecentIdLookup;
        @Nullable
        private final Set<BlobFile> lastCommitBlobs;
        private final boolean lastCommitIsHollow;

        Request(
            DiscoveryNode targetNode,
            long recoveryId,
            ShardId shardId,
            ReplicationTracker.PrimaryContext primaryContext,
            RetentionLeases retentionLeases,
            Map<PrimaryTermAndGeneration, Set<String>> searchNodesPerCommit,
            BlobFileWithLength latestBccBlob,
            Set<BlobFile> otherBlobFiles,
            boolean hasRecentIdLookup,
            Set<BlobFile> lastCommitBlobs,
            boolean lastCommitIsHollow
        ) {
            this.targetNode = targetNode;
            this.recoveryId = recoveryId;
            this.shardId = shardId;
            this.primaryContext = primaryContext;
            this.retentionLeases = retentionLeases;
            this.searchNodesPerCommit = searchNodesPerCommit;
            this.latestBccBlob = latestBccBlob;
            this.otherBlobFiles = otherBlobFiles;
            this.hasRecentIdLookup = hasRecentIdLookup;
            this.lastCommitBlobs = lastCommitBlobs;
            this.lastCommitIsHollow = lastCommitIsHollow;
        }

        Request(StreamInput in) throws IOException {
            super(in);
            this.targetNode = null;
            recoveryId = in.readVLong();
            shardId = new ShardId(in);
            primaryContext = new ReplicationTracker.PrimaryContext(in);
            retentionLeases = new RetentionLeases(in);
            searchNodesPerCommit = in.readMap(PrimaryTermAndGeneration::new, in0 -> in0.readCollectionAsSet(StreamInput::readString));
            latestBccBlob = in.readOptionalWriteable(BlobFileWithLength::new);
            otherBlobFiles = in.readCollectionAsSet(BlobFile::new);
            hasRecentIdLookup = in.readBoolean();
            lastCommitBlobs = in.getTransportVersion().supports(STATELESS_PRIMARY_HANDOFF_LATEST_BLOBS)
                ? in.readCollectionAsImmutableSet(BlobFile::new)
                : Set.of();
            lastCommitIsHollow = in.getTransportVersion().supports(STATELESS_PRIMARY_HANDOFF_LATEST_BLOBS) && in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVLong(recoveryId);
            shardId.writeTo(out);
            primaryContext.writeTo(out);
            retentionLeases.writeTo(out);
            out.writeMap(
                searchNodesPerCommit,
                (out0, v) -> v.writeTo(out0),
                (out0, v) -> out0.writeCollection(v, StreamOutput::writeString)
            );
            out.writeOptionalWriteable(latestBccBlob);
            out.writeCollection(otherBlobFiles);
            out.writeBoolean(hasRecentIdLookup);
            if (out.getTransportVersion().supports(STATELESS_PRIMARY_HANDOFF_LATEST_BLOBS)) {
                out.writeCollection(lastCommitBlobs);
                out.writeBoolean(lastCommitIsHollow);
            }
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public DiscoveryNode targetNode() {
            return targetNode;
        }

        public long recoveryId() {
            return recoveryId;
        }

        public ShardId shardId() {
            return shardId;
        }

        public ReplicationTracker.PrimaryContext primaryContext() {
            return primaryContext;
        }

        public RetentionLeases retentionLeases() {
            return retentionLeases;
        }

        public Map<PrimaryTermAndGeneration, Set<String>> searchNodesPerCommit() {
            return searchNodesPerCommit;
        }

        public Set<BlobFile> otherBlobFiles() {
            return otherBlobFiles;
        }

        @Nullable
        public BlobFileWithLength latestBccBlob() {
            return latestBccBlob;
        }

        public boolean hasRecentIdLookup() {
            return hasRecentIdLookup;
        }

        public RecoveryInfoFromSource recoveryInfoFromSource() {
            if (latestBccBlob == null && hasRecentIdLookup == false) {
                return null;
            }
            StatelessCommitService.SourceBlobsInfo sourceBlobsInfo = null;
            if (latestBccBlob != null) {
                sourceBlobsInfo = new StatelessCommitService.SourceBlobsInfo(
                    latestBccBlob.blobFile(),
                    latestBccBlob.length(),
                    otherBlobFiles
                );
            }
            return new RecoveryInfoFromSource(sourceBlobsInfo, lastCommitBlobs, lastCommitIsHollow, hasRecentIdLookup);
        }
    }
}
