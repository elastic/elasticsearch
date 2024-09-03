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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoveryCommitTooNewException;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

public class TransportSendRecoveryCommitRegistrationAction extends HandledTransportAction<RegisterCommitRequest, RegisterCommitResponse> {

    public static final String NAME = "internal:index/shard/recovery:send_recovery_commit_registration";
    public static final ActionType<RegisterCommitResponse> TYPE = new ActionType<>(NAME);
    private static final Logger logger = LogManager.getLogger(TransportSendRecoveryCommitRegistrationAction.class);

    private final ClusterService clusterService;
    private final TransportService transportService;
    private final IndicesService indicesService;
    private final ThreadPool threadPool;

    @Inject
    public TransportSendRecoveryCommitRegistrationAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        ActionFilters actionFilters
    ) {
        super(NAME, transportService, actionFilters, RegisterCommitRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.indicesService = indicesService;
        this.threadPool = clusterService.threadPool();
    }

    @Override
    protected void doExecute(Task task, RegisterCommitRequest request, ActionListener<RegisterCommitResponse> listener) {
        var state = clusterService.state();
        // The timeout is intentionally kept low to prevent blocking other recoveries since this registration
        // happens during unpromotable recovery).
        var observer = new ClusterStateObserver(state, clusterService, new TimeValue(10000), logger, threadPool.getThreadContext());
        register(task, request, state, observer, listener);
    }

    private void register(
        Task task,
        RegisterCommitRequest request,
        ClusterState state,
        ClusterStateObserver observer,
        ActionListener<RegisterCommitResponse> listener
    ) {
        tryRegistration(task, request, state, listener.delegateResponse((l, e) -> {
            var cause = ExceptionsHelper.unwrapCause(e);
            logger.debug("recovery commit registration failed", cause);
            if (isRetryable(cause, request)) {
                observer.waitForNextChange(new ClusterStateObserver.Listener() {
                    @Override
                    public void onNewClusterState(ClusterState state) {
                        register(task, request, state, observer, l);
                    }

                    @Override
                    public void onClusterServiceClose() {
                        l.onFailure(new ElasticsearchException("cs observer closed"));
                    }

                    @Override
                    public void onTimeout(TimeValue timeout) {
                        l.onFailure(new ElasticsearchException("cs observer timed out", cause));
                    }
                });
            } else {
                l.onFailure(e);
            }
        }));
    }

    private boolean isRetryable(Throwable e, RegisterCommitRequest request) {
        if (e instanceof ShardNotFoundException || e instanceof RecoveryCommitTooNewException) {
            return true;
        } else if (e instanceof IndexNotFoundException) {
            var state = clusterService.state();
            return state.metadata().hasIndex(request.getShardId().getIndexName());
        } else {
            return false;
        }
    }

    private void tryRegistration(
        Task task,
        RegisterCommitRequest request,
        ClusterState state,
        ActionListener<RegisterCommitResponse> listener
    ) {
        try {
            var shardId = request.getShardId();
            assert localShardIsUnpromotableOrNull(shardId)
                : "TransportSendRecoveryCommitRegistrationAction can only be executed on a search shard";
            // Forward the request to the indexing shard
            var shardRoutingTable = state.routingTable().shardRoutingTable(shardId);
            if (shardRoutingTable.primaryShard() == null || shardRoutingTable.primaryShard().active() == false) {
                // TODO: A search shard should be able to continue using the found commit despite not being able to register it.
                // For now, we retry the request to register the commit.
                throw new ShardNotFoundException(shardId, "cannot route request to the indexing shard");
            }
            DiscoveryNode node = state.nodes().get(shardRoutingTable.primaryShard().currentNodeId());
            logger.debug("{} sending recovery commit registration to {}", shardId, node);
            assert node != null;
            final var requestWithClusterStateVersion = request.withClusterStateVersion(state.version());
            transportService.sendChildRequest(
                node,
                TransportRegisterCommitForRecoveryAction.NAME,
                requestWithClusterStateVersion,
                task,
                TransportRequestOptions.EMPTY,
                new ActionListenerResponseHandler<>(listener.safeMap(r -> {
                    logger.debug("{} received registration response {} for {} ", shardId, r, requestWithClusterStateVersion);
                    return r;
                }), in -> new RegisterCommitResponse(in, node.getId()), transportService.getThreadPool().generic())
            );
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private boolean localShardIsUnpromotableOrNull(ShardId shardId) {
        // It is OK if the index or the shard is concurrently removed when performing this check.
        // We do _not_ want to retry on this ShardNotFoundException.
        // The commit registration will proceed and ultimately fail inside PeerRecoveryTargetService
        // which in turn allows a new allocation to happen.
        IndexService indexService = indicesService.indexService(shardId.getIndex());
        if (indexService == null) {
            return true;
        }
        final IndexShard indexShard = indexService.getShardOrNull(shardId.id());
        if (indexShard == null) {
            return true;
        }
        return indexShard.routingEntry().isPromotableToPrimary() == false;
    }
}
