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

package co.elastic.elasticsearch.stateless.recovery;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.indices.recovery.StatelessUnpromotableRelocationAction;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.concurrent.Executor;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.indices.recovery.StatelessUnpromotableRelocationAction.TYPE;

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

    private final TransportService transportService;
    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final PeerRecoveryTargetService peerRecoveryTargetService;
    private final Executor recoveryExecutor;
    private final ThreadContext threadContext;
    private final ProjectResolver projectResolver;
    private final TimeValue clusterStateConvergenceTimeout;
    private final TimeValue startHandoffRequestTimeout;

    @Inject
    public TransportStatelessUnpromotableRelocationAction(
        TransportService transportService,
        ClusterService clusterService,
        ActionFilters actionFilters,
        IndicesService indicesService,
        PeerRecoveryTargetService peerRecoveryTargetService,
        ProjectResolver projectResolver
    ) {
        super(TYPE.name(), actionFilters, transportService.getTaskManager(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
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
            (request, channel, task) -> handleStartHandoff(
                request,
                new ChannelActionListener<>(channel).map(ignored -> ActionResponse.Empty.INSTANCE)
            )
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
            }).<ActionResponse.Empty>andThen(l -> maybeSendStartHandoffRequest(indexShard, task, request, l))
                .andThenApply(response -> handleHandoffResponse(indexShard, response))
                .addListener(listener);
        }
    }

    private void maybeSendStartHandoffRequest(
        IndexShard indexShard,
        Task task,
        StatelessUnpromotableRelocationAction.Request request,
        ActionListener<ActionResponse.Empty> listener
    ) {
        final var relocatingNodeId = indexShard.routingEntry().relocatingNodeId();
        final var nodes = clusterService.state().nodes();
        if (relocatingNodeId != null && nodes.nodeExists(relocatingNodeId)) {
            var relocatingNode = nodes.get(relocatingNodeId);
            sendStartHandoffRequest(task, request, relocatingNode, listener.delegateResponse((l, e) -> {
                // Ensure recovery process continues even if handoff fails
                logger.warn(format("%s failed to send start handoff request", request.getShardId()), e);
                l.onResponse(ActionResponse.Empty.INSTANCE);
            }));
        } else {
            listener.onResponse(ActionResponse.Empty.INSTANCE);
        }
    }

    private void sendStartHandoffRequest(
        Task task,
        StatelessUnpromotableRelocationAction.Request request,
        DiscoveryNode relocatingNode,
        ActionListener<ActionResponse.Empty> listener
    ) {
        transportService.sendChildRequest(
            relocatingNode,
            START_HANDOFF_ACTION_NAME,
            new StartHandoffRequest(request.getShardId(), request.getTargetAllocationId(), request.getClusterStateVersion()),
            task,
            TransportRequestOptions.timeout(startHandoffRequestTimeout),
            new ActionListenerResponseHandler<>(listener, in -> ActionResponse.Empty.INSTANCE, recoveryExecutor)
        );
    }

    private ActionResponse.Empty handleHandoffResponse(IndexShard indexShard, ActionResponse.Empty response) {
        return response;
    }

    private void handleStartHandoff(StartHandoffRequest request, ActionListener<Void> listener) {
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

    private void doHandleStartHandoff(StartHandoffRequest request, ActionListener<Void> listener) {
        ActionListener.completeWith(listener, () -> {
            final var indexService = indicesService.indexServiceSafe(request.getShardId().getIndex());
            final var indexShard = indexService.getShard(request.getShardId().id());
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

            return null;
        });
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
