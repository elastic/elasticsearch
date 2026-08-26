/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.index.seqno.RetentionLeases;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.CompositeRecoverySchedulingListener;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.StatelessPrimaryRelocationAction;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.stateless.commits.BlobFile;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService.RecoveryInfoFromSource;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;
import org.elasticsearch.xpack.stateless.recovery.metering.StatelessPrimaryRelocationMetricsCollector;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

import static org.elasticsearch.indices.recovery.StatelessPrimaryRelocationAction.TYPE;

/// Thin transport layer for stateless primary relocation. Registers request handlers and owns [TransportService] sends.
/// Source/target logic live in [StatelessPrimaryRelocationSourceService] and [StatelessPrimaryRelocationTargetService].
public class TransportStatelessPrimaryRelocationAction extends TransportAction<
    StatelessPrimaryRelocationAction.Request,
    ActionResponse.Empty> {

    private static final Logger logger = LogManager.getLogger(TransportStatelessPrimaryRelocationAction.class);

    private static final TransportVersion STATELESS_PRIMARY_HANDOFF_LATEST_BLOBS = TransportVersion.fromName(
        "stateless_primary_handoff_latest_blobs"
    );

    public static final String START_RELOCATION_ACTION_NAME = TYPE.name() + "/start";
    public static final String PREWARM_RELOCATION_ACTION_NAME = TYPE.name() + "/prewarm";
    public static final String PRIMARY_CONTEXT_HANDOFF_ACTION_NAME = TYPE.name() + "/primary_context_handoff";

    public static final Setting<TimeValue> SLOW_RELOCATION_THRESHOLD_SETTING = Setting.timeSetting(
        "stateless.cluster.primary_relocation.slow_handoff_warning_threshold",
        TimeValue.timeValueSeconds(5),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> ID_LOOKUP_RECENCY_THRESHOLD_SETTING = Setting.timeSetting(
        "stateless.cluster.primary_relocation.id_lookup_recency_threshold",
        TimeValue.timeValueMinutes(10),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private final TransportService transportService;
    private final IndicesService indicesService;
    private final PeerRecoveryTargetService peerRecoveryTargetService;
    private final Executor recoveryExecutor;
    private final StatelessPrimaryRelocationMetricsCollector relocationMetricsCollector;

    @Inject
    public TransportStatelessPrimaryRelocationAction(
        Settings settings,
        TransportService transportService,
        ActionFilters actionFilters,
        IndicesService indicesService,
        CompositeRecoverySchedulingListener recoverySchedulingListeners,
        StatelessPrimaryRelocationSourceService primaryRelocationSourceService,
        StatelessPrimaryRelocationTargetService primaryRelocationTargetService,
        PeerRecoveryTargetService peerRecoveryTargetService,
        StatelessPrimaryRelocationMetricsCollector relocationMetricsCollector
    ) {
        super(TYPE.name(), actionFilters, transportService.getTaskManager(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
        assert DiscoveryNode.hasRole(settings, DiscoveryNodeRole.INDEX_ROLE);
        this.transportService = transportService;
        this.indicesService = indicesService;
        this.peerRecoveryTargetService = peerRecoveryTargetService;
        this.relocationMetricsCollector = relocationMetricsCollector;
        this.recoveryExecutor = transportService.getThreadPool().generic();

        primaryRelocationSourceService.registerRecoverySchedulingListeners(recoverySchedulingListeners);
        primaryRelocationSourceService.registerTargetTriggers(this::triggerPrewarmOnTarget, this::triggerPrimaryContextHandoff);

        transportService.registerRequestHandler(
            START_RELOCATION_ACTION_NAME,
            recoveryExecutor,
            false, // forceExecution
            false, // canTripCircuitBreaker
            StatelessPrimaryRelocationAction.Request::new,
            (request, channel, task) -> primaryRelocationSourceService.startRelocation(task, request, new ChannelActionListener<>(channel))
        );

        transportService.registerRequestHandler(
            PREWARM_RELOCATION_ACTION_NAME,
            recoveryExecutor,
            false, // forceExecution
            false, // canTripCircuitBreaker
            PrewarmRelocationRequest::new,
            (request, channel, task) -> primaryRelocationTargetService.handlePrewarmRelocation(
                request,
                new ChannelActionListener<>(channel).map(ignored -> ActionResponse.Empty.INSTANCE)
            )
        );

        transportService.registerRequestHandler(
            PRIMARY_CONTEXT_HANDOFF_ACTION_NAME,
            recoveryExecutor,
            false, // forceExecution
            false, // canTripCircuitBreaker
            PrimaryContextHandoffRequest::new,
            (request, channel, task) -> {
                final var recoveryRef = peerRecoveryTargetService.getRecoveryRef(request.recoveryId(), request.shardId());
                boolean success = false;
                try {
                    primaryRelocationTargetService.handlePrimaryContextHandoff(
                        request,
                        ActionListener.releaseAfter(
                            new ChannelActionListener<>(channel).map(ignored -> ActionResponse.Empty.INSTANCE),
                            recoveryRef
                        )
                    );
                    success = true;
                } finally {
                    if (success == false) {
                        recoveryRef.close();
                    }
                }
            }
        );
    }

    @Override
    protected void doExecute(Task task, StatelessPrimaryRelocationAction.Request request, ActionListener<ActionResponse.Empty> listener) {
        // executed locally by `PeerRecoveryTargetService` (i.e. we are on the target node here)
        logger.trace("{} preparing unsearchable shard for primary relocation", request.shardId());

        try (var recoveryRef = peerRecoveryTargetService.getRecoveryRef(request.recoveryId(), request.shardId())) {
            final var indexService = indicesService.indexServiceSafe(request.shardId().getIndex());
            final var indexShard = indexService.getShard(request.shardId().id());
            indexShard.ensureRecoveryNotCancelled();
            indexShard.prepareForIndexRecovery();

            transportService.sendChildRequest(
                recoveryRef.target().sourceNode(),
                START_RELOCATION_ACTION_NAME,
                request,
                task,
                TransportRequestOptions.EMPTY,
                new ActionListenerResponseHandler<>(listener.map(response -> {
                    // We record the source metrics on the target node because once the source receives a SIGTERM
                    // the metrics agent stops emitting metrics and we lose all that information
                    RelocationSourceMetrics relocationSourceMetrics = response.getRelocationSourceMetrics();
                    if (relocationSourceMetrics != null) {
                        relocationMetricsCollector.recordRelocationSourceMetrics(relocationSourceMetrics);
                    }
                    return ActionResponse.Empty.INSTANCE;
                }), StartRelocationResponse::new, recoveryExecutor)
            );
        }
    }

    /// Called from the source, which sends a [PrewarmRelocationRequest] to the target
    private void triggerPrewarmOnTarget(Task task, DiscoveryNode targetNode, PrewarmRelocationRequest request) {
        transportService.sendChildRequest(
            targetNode,
            PREWARM_RELOCATION_ACTION_NAME,
            request,
            task,
            TransportRequestOptions.EMPTY,
            // The response (whether prewarm succeeded or not) does not affect the relocation listener, so we use a noop listener
            new ActionListenerResponseHandler<>(ActionListener.noop().delegateResponse((l, e) -> {
                logger.debug(() -> Strings.format("%s ignoring prewarm action failure", request.shardId()), e);
                l.onFailure(e);
            }), in -> ActionResponse.Empty.INSTANCE, recoveryExecutor)
        );
    }

    /// Called from the source, which sends a [PrimaryContextHandoffRequest] to the target
    private void triggerPrimaryContextHandoff(
        Task task,
        DiscoveryNode targetNode,
        PrimaryContextHandoffRequest request,
        ActionListener<ActionResponse.Empty> listener
    ) {
        transportService.sendChildRequest(
            targetNode,
            PRIMARY_CONTEXT_HANDOFF_ACTION_NAME,
            request,
            task,
            TransportRequestOptions.EMPTY,
            new ActionListenerResponseHandler<>(listener, in -> ActionResponse.Empty.INSTANCE, recoveryExecutor)
        );
    }

    public record BlobFileWithLength(BlobFile blobFile, long length) implements Writeable {
        public BlobFileWithLength(StreamInput in) throws IOException {
            this(new BlobFile(in), in.readVLong());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            blobFile.writeTo(out);
            out.writeVLong(length);
        }
    }

    public static class PrimaryContextHandoffRequest extends AbstractTransportRequest {

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

        PrimaryContextHandoffRequest(
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

        PrimaryContextHandoffRequest(StreamInput in) throws IOException {
            super(in);
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

    public static class PrewarmRelocationRequest extends AbstractTransportRequest {

        private final ShardId shardId;
        private final BlobFileWithLength latestBccBlob;
        private final boolean hasRecentIdLookup;

        public PrewarmRelocationRequest(ShardId shardId, BlobFileWithLength latestBccBlob, boolean hasRecentIdLookup) {
            this.shardId = shardId;
            this.latestBccBlob = latestBccBlob;
            this.hasRecentIdLookup = hasRecentIdLookup;
        }

        public PrewarmRelocationRequest(StreamInput in) throws IOException {
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
