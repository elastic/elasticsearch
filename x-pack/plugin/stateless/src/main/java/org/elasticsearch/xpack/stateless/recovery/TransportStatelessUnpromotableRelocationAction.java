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

package org.elasticsearch.xpack.stateless.recovery;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionResponse.Empty;
import org.elasticsearch.action.search.SearchContextIdForNode;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine.Searcher;
import org.elasticsearch.index.engine.Engine.SearcherSupplier;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.indices.recovery.StatelessUnpromotableRelocationAction;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.ReaderContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.stateless.commits.BlobLocation;
import org.elasticsearch.xpack.stateless.engine.SearchEngine;
import org.elasticsearch.xpack.stateless.lucene.SearchDirectory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.indices.recovery.StatelessUnpromotableRelocationAction.TYPE;
import static org.elasticsearch.search.SearchService.PIT_RELOCATION_ENABLED;

public class TransportStatelessUnpromotableRelocationAction extends TransportAction<
    StatelessUnpromotableRelocationAction.Request,
    ActionResponse.Empty> {

    static final String START_HANDOFF_ACTION_NAME = TYPE.name() + "/start_handoff";
    public static final Setting<TimeValue> START_HANDOFF_CLUSTER_STATE_CONVERGENCE_TIMEOUT_SETTING = Setting.timeSetting(
        "serverless.cluster.unpromotable_relocation.start_handoff_cluster_state_convergence_timeout",
        TimeValue.timeValueSeconds(30),
        Setting.Property.NodeScope
    );
    public static final Setting<TimeValue> START_HANDOFF_REQUEST_TIMEOUT_SETTING = Setting.timeSetting(
        "serverless.cluster.unpromotable_relocation.start_handoff_request_timeout",
        TimeValue.timeValueSeconds(30),
        Setting.Property.NodeScope
    );
    private static final RelocationHandoffResponse EMPTY_RESPONSE = new RelocationHandoffResponse(
        new PITHandoffResponse(Collections.emptyList())
    );

    private static final TransportVersion PIT_RELOCATION_FEATURE = TransportVersion.fromName("pit_relocation_feature");

    private final TransportService transportService;
    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final PeerRecoveryTargetService peerRecoveryTargetService;
    private final Executor recoveryExecutor;
    private final ThreadContext threadContext;
    private final ProjectResolver projectResolver;
    private final TimeValue clusterStateConvergenceTimeout;
    private final TimeValue startHandoffRequestTimeout;
    private final SearchService searchService;
    private final PITRelocationService pitRelocationService;

    private static final Logger logger = LogManager.getLogger(TransportStatelessUnpromotableRelocationAction.class);

    @Inject
    public TransportStatelessUnpromotableRelocationAction(
        TransportService transportService,
        ClusterService clusterService,
        ActionFilters actionFilters,
        IndicesService indicesService,
        PeerRecoveryTargetService peerRecoveryTargetService,
        ProjectResolver projectResolver,
        SearchService searchService,
        PITRelocationService pitRelocationService
    ) {
        super(TYPE.name(), actionFilters, transportService.getTaskManager(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.searchService = searchService;
        this.pitRelocationService = pitRelocationService;
        this.peerRecoveryTargetService = peerRecoveryTargetService;
        var threadPool = transportService.getThreadPool();
        this.recoveryExecutor = threadPool.generic();
        this.threadContext = threadPool.getThreadContext();
        this.projectResolver = projectResolver;
        var settings = clusterService.getSettings();
        this.clusterStateConvergenceTimeout = START_HANDOFF_CLUSTER_STATE_CONVERGENCE_TIMEOUT_SETTING.get(settings);
        this.startHandoffRequestTimeout = START_HANDOFF_REQUEST_TIMEOUT_SETTING.get(settings);

        transportService.registerRequestHandler(
            START_HANDOFF_ACTION_NAME,
            recoveryExecutor,
            false,
            false,
            StartHandoffRequest::new,
            (request, channel, task) -> handleStartHandoff(request, new ChannelActionListener<>(channel))
        );
        this.pitRelocationService.setActiveReaderContextProvider(searchService::getActivePITContexts);
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(
                PIT_RELOCATION_ENABLED,
                pitRelocationEnabled -> this.pitRelocationService.setPitRelocationEnabled(pitRelocationEnabled)
            );
    }

    @Override
    protected void doExecute(
        Task task,
        StatelessUnpromotableRelocationAction.Request request,
        ActionListener<ActionResponse.Empty> listener
    ) {
        try (var recoveryRef = peerRecoveryTargetService.getRecoveryRef(request.getRecoveryId(), request.getShardId())) {
            final var indexService = indicesService.indexServiceSafe(request.getShardId().getIndex());
            final var indexShard = indexService.getShard(request.getShardId().id());
            final var recoveryTarget = recoveryRef.target();
            final var recoveryState = recoveryTarget.state();

            assert indexShard.indexSettings().getIndexMetadata().isSearchableSnapshot() == false;

            SubscribableListener.newForked(indexShard::preRecovery).andThenApply(unused -> {
                logger.trace("{} preparing unpromotable shard for recovery", recoveryTarget.shardId());
                indexShard.prepareForIndexRecovery();
                // Skip unnecessary intermediate stages
                recoveryState.setStage(RecoveryState.Stage.VERIFY_INDEX);
                recoveryState.setStage(RecoveryState.Stage.TRANSLOG);
                indexShard.openEngineAndSkipTranslogRecovery();
                recoveryState.getIndex().setFileDetailsComplete();
                recoveryState.setStage(RecoveryState.Stage.FINALIZE);
                return null;
            })
                .<RelocationHandoffResponse>andThen(l -> maybeSendStartHandoffRequest(indexShard, task, request, l))
                .<Void>andThen((l, response) -> handlePitHandoffResponse(indexShard, response.pitHandoffResponse, l))
                .andThenApply(unused -> Empty.INSTANCE)
                .addListener(listener);
        }
    }

    private void maybeSendStartHandoffRequest(
        IndexShard indexShard,
        Task task,
        StatelessUnpromotableRelocationAction.Request request,
        ActionListener<RelocationHandoffResponse> listener
    ) {
        final var relocatingNodeId = indexShard.routingEntry().relocatingNodeId();
        final var nodes = clusterService.state().nodes();
        if (relocatingNodeId != null && nodes.nodeExists(relocatingNodeId)) {
            var relocatingNode = nodes.get(relocatingNodeId);
            // check transport version to see if relocating node already supports PIT relocation
            TransportVersion transportVersion = transportService.getConnection(relocatingNode).getTransportVersion();
            if (transportVersion.supports(PIT_RELOCATION_FEATURE)) {
                sendStartHandoffRequest(task, request, relocatingNode, listener.delegateResponse((l, e) -> {
                    // Ensure recovery process continues even if handoff fails
                    logger.warn(format("%s failed to send start handoff request", request.getShardId()), e);
                    listener.onResponse(EMPTY_RESPONSE);
                }));
            } else {
                // relocating node does not support PIT relocation, just return empty response
                listener.onResponse(EMPTY_RESPONSE);
            }
        } else {
            // no shard relocation in progress, just return empty response
            listener.onResponse(EMPTY_RESPONSE);
        }
    }

    private void sendStartHandoffRequest(
        Task task,
        StatelessUnpromotableRelocationAction.Request request,
        DiscoveryNode relocatingNode,
        ActionListener<RelocationHandoffResponse> listener
    ) {
        logger.debug("sending start handoff request from [{}]", clusterService.localNode().getName());
        transportService.sendChildRequest(
            relocatingNode,
            START_HANDOFF_ACTION_NAME,
            new StartHandoffRequest(request.getShardId(), request.getTargetAllocationId(), request.getClusterStateVersion()),
            task,
            TransportRequestOptions.timeout(startHandoffRequestTimeout),
            new ActionListenerResponseHandler<>(listener, RelocationHandoffResponse::new, recoveryExecutor)
        );
    }

    private void handlePitHandoffResponse(IndexShard indexShard, PITHandoffResponse response, ActionListener<Void> listener) {
        if (searchService.isPitRelocationEnabled() == false) {
            listener.onResponse(null);
            return;
        }
        logger.debug("handle PITHandoffResponse for shard {}. Open pit infos: {}", indexShard.shardId(), response.getOpenPITContextInfos());

        try (var refs = new RefCountingListener(listener)) {
            for (OpenPITContextInfo pitContextInfo : response.getOpenPITContextInfos()) {
                openPitAsync(indexShard, pitContextInfo, refs.acquire());
            }
        }
    }

    private void openPitAsync(IndexShard indexShard, OpenPITContextInfo pitContextInfo, ActionListener<Void> listener) {
        ShardId shardId = indexShard.shardId();
        final IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        final IndexShard shard = indexService.getShard(shardId.id());
        try {
            shard.withEngine(engine -> {
                if (engine instanceof SearchEngine == false) {
                    throw new IllegalStateException("Expected SearchEngine but got: " + engine.getClass());
                }
                final String segmentsFileName = pitContextInfo.segmentsFileName;
                Map<String, BlobLocation> metadata = pitContextInfo.metadata();

                // we need to acquire the searcher for the exact commit point that the PIT was opened against
                // the engine does this asynchronously because we need to synchronize with ongoing commit updates
                ((SearchEngine) engine).acquireSearcherForCommit(
                    segmentsFileName,
                    metadata,
                    indexShard::wrapSearcher,
                    new ActionListener<SearcherSupplier>() {
                        @Override
                        public void onResponse(SearcherSupplier searcherSupplier) {
                            IndexShardState shardState = shard.state();
                            Runnable readerContextCreation = () -> {
                                ReaderContext newReaderContext = searchService.createAndPutRelocatedPitContext(
                                    pitContextInfo.contextId.getSearchContextId(),
                                    indexService,
                                    indexShard,
                                    searcherSupplier,
                                    pitContextInfo.keepAlive()
                                );
                                assert newReaderContext != null;
                                logger.debug("adding relocated ReaderContext with id: [{}]", newReaderContext.id());
                            };
                            assert (shardState.equals(IndexShardState.STARTED) == false
                                && shardState.equals(IndexShardState.POST_RECOVERY) == false)
                                : "relocating pit but shard is already in shard state: " + shardState;
                            logger.debug("delaying ReaderContext creation because shard [{}] state is [{}]:", shardId, shardState);
                            pitRelocationService.addRelocatingContext(shardId, readerContextCreation);
                            listener.onResponse(null);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            logger.warn("Failed to create new readerContext after PIT transfer for shard " + shardId, e);
                            // we don't want to fail the whole recovery because of a PIT context relocation issue
                            listener.onResponse(null);
                        }
                    }
                );
                return null;
            });
        } catch (AlreadyClosedException e) {
            logger.warn("Unexpected exception while acquiring searcher after PIT transfer for shard " + shardId, e);
        }
    }

    private void handleStartHandoff(StartHandoffRequest request, ActionListener<RelocationHandoffResponse> listener) {
        var state = clusterService.state();
        var observer = new ClusterStateObserver(state, clusterService, clusterStateConvergenceTimeout, logger, threadContext);
        if (state.version() < request.getClusterStateVersion()) {
            observer.waitForNextChange(new ClusterStateObserver.Listener() {
                @Override
                public void onNewClusterState(ClusterState state) {
                    doHandleStartHandoff(request, listener);
                }

                @Override
                public void onClusterServiceClose() {
                    listener.onFailure(new NodeClosedException(clusterService.localNode()));
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    listener.onFailure(new ElasticsearchException("Cluster state convergence timed out"));
                }
            }, clusterState -> clusterState.version() >= request.getClusterStateVersion());
        } else {
            doHandleStartHandoff(request, listener);
        }
    }

    private void doHandleStartHandoff(StartHandoffRequest request, ActionListener<RelocationHandoffResponse> listener) {
        ActionListener.completeWith(listener, () -> {
            if (searchService.isPitRelocationEnabled()) {

                ShardId shardId = request.getShardId();
                final var indexService = indicesService.indexServiceSafe(shardId.getIndex());
                final var indexShard = indexService.getShard(shardId.id());
                final var shardRouting = indexShard.routingEntry();

                assert shardRouting.isPromotableToPrimary() == false;

                final var targetShardRouting = clusterService.state()
                    .routingTable(projectResolver.getProjectId())
                    .shardRoutingTable(request.getShardId())
                    .getByAllocationId(request.getTargetAllocationId());

                if (targetShardRouting == null || shardRouting.isRelocationSourceOf(targetShardRouting) == false) {
                    throw new IllegalStateException(
                        "Invalid relocation state: expected [" + shardRouting + "] to be relocation source of [" + targetShardRouting + "]"
                    );
                }

                List<ReaderContext> activeContexts = this.searchService.getActivePITContexts(shardId);
                List<OpenPITContextInfo> pitContextInfos = new ArrayList<>();

                for (ReaderContext context : activeContexts) {
                    try (Searcher searcher = context.acquireSearcher("pit_handoff")) {
                        DirectoryReader reader = searcher.getDirectoryReader();
                        IndexCommit indexCommit = reader.getIndexCommit();
                        SearchDirectory searchDirectory = SearchDirectory.unwrapDirectory(reader.directory());
                        Map<String, BlobLocation> metadata = searchDirectory.getBlobLocationForFiles(indexCommit.getFileNames());
                        pitContextInfos.add(
                            new OpenPITContextInfo(
                                shardId,
                                indexCommit.getSegmentsFileName(),
                                context.keepAlive(),
                                new SearchContextIdForNode(null, clusterService.localNode().getId(), context.id()),
                                metadata
                            )
                        );
                    } catch (AlreadyClosedException e) {
                        logger.debug("Context was closed while preparing PIT handoff for shard " + shardId, e);
                    }
                }
                return new RelocationHandoffResponse(new PITHandoffResponse(pitContextInfos));
            }
            return EMPTY_RESPONSE;
        });
    }

    static class RelocationHandoffResponse extends ActionResponse {

        private final PITHandoffResponse pitHandoffResponse;

        RelocationHandoffResponse(PITHandoffResponse pitHandoffResponse) {
            this.pitHandoffResponse = pitHandoffResponse;
        }

        RelocationHandoffResponse(StreamInput in) throws IOException {
            this.pitHandoffResponse = new PITHandoffResponse(in);
        }

        PITHandoffResponse getPitHandoffResponse() {
            return this.pitHandoffResponse;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            pitHandoffResponse.writeTo(out);
        }
    }

    static class PITHandoffResponse implements Writeable {

        private final List<OpenPITContextInfo> openPITContextInfos;

        PITHandoffResponse(List<OpenPITContextInfo> infos) {
            this.openPITContextInfos = infos;
        }

        PITHandoffResponse(StreamInput in) throws IOException {
            this.openPITContextInfos = in.readCollectionAsList(OpenPITContextInfo::new);
        }

        public List<OpenPITContextInfo> getOpenPITContextInfos() {
            return openPITContextInfos;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(openPITContextInfos);
        }
    }

    record OpenPITContextInfo(
        ShardId shardId,
        String segmentsFileName,
        long keepAlive,
        SearchContextIdForNode contextId,
        Map<String, BlobLocation> metadata
    ) implements Writeable {

        OpenPITContextInfo(StreamInput in) throws IOException {
            this(
                new ShardId(in),
                in.readString(),
                in.readVLong(),
                new SearchContextIdForNode(in),
                in.readMap(StreamInput::readString, BlobLocation::readFromTransport)
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            shardId.writeTo(out);
            out.writeString(segmentsFileName);
            out.writeVLong(keepAlive);
            contextId.writeTo(out);
            out.writeMap(metadata, StreamOutput::writeString, StreamOutput::writeWriteable);
        }

        @Override
        public String toString() {
            return "OpenPITContextInfo{"
                + "shardId="
                + shardId
                + ", segmentsFileName='"
                + segmentsFileName
                + '\''
                + ", keepAlive="
                + keepAlive
                + '\''
                + ", contextId="
                + contextId
                + ", metadata="
                + metadata
                + '}';
        }
    }

    static class StartHandoffRequest extends ActionRequest {
        private final ShardId shardId;
        private final String targetAllocationId;
        private final long clusterStateVersion;

        StartHandoffRequest(ShardId shardId, String targetAllocationId, long clusterStateVersion) {
            this.shardId = shardId;
            this.targetAllocationId = targetAllocationId;
            this.clusterStateVersion = clusterStateVersion;
        }

        StartHandoffRequest(StreamInput in) throws IOException {
            super(in);
            this.shardId = new ShardId(in);
            this.targetAllocationId = in.readString();
            this.clusterStateVersion = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
            out.writeString(targetAllocationId);
            out.writeVLong(clusterStateVersion);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public ShardId getShardId() {
            return shardId;
        }

        public String getTargetAllocationId() {
            return targetAllocationId;
        }

        public long getClusterStateVersion() {
            return clusterStateVersion;
        }
    }
}
