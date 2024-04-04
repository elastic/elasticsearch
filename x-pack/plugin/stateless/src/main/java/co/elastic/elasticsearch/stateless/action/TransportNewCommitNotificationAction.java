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

package co.elastic.elasticsearch.stateless.action;

import co.elastic.elasticsearch.stateless.Stateless;
import co.elastic.elasticsearch.stateless.autoscaling.search.ShardSizeCollector;
import co.elastic.elasticsearch.stateless.engine.SearchEngine;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.broadcast.unpromotable.TransportBroadcastUnpromotableAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

public class TransportNewCommitNotificationAction extends TransportBroadcastUnpromotableAction<
    NewCommitNotificationRequest,
    NewCommitNotificationResponse> {

    private static final Logger logger = LogManager.getLogger(TransportNewCommitNotificationAction.class);
    public static final String NAME = "internal:admin/" + Stateless.NAME + "/search/new/commit";
    public static final ActionType<NewCommitNotificationResponse> TYPE = new ActionType<>(NAME);

    private final IndicesService indicesService;
    private final ShardSizeCollector shardSizeCollector;

    @Inject
    public TransportNewCommitNotificationAction(
        ClusterService clusterService,
        TransportService transportService,
        ShardStateAction shardStateAction,
        ActionFilters actionFilters,
        IndicesService indicesService,
        ShardSizeCollector shardSizeCollector
    ) {
        super(
            NAME,
            clusterService,
            transportService,
            shardStateAction,
            actionFilters,
            NewCommitNotificationRequest::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.indicesService = indicesService;
        this.shardSizeCollector = shardSizeCollector;
    }

    @Override
    protected void unpromotableShardOperation(
        Task task,
        NewCommitNotificationRequest request,
        ActionListener<NewCommitNotificationResponse> listener
    ) {
        if (logger.isTraceEnabled()) {
            logger.trace("received new commit notification request [{}]", request);
        } else {
            logger.debug(
                "received new commit notification request for shard [{}], term [{}], generation [{}]",
                request.shardId(),
                request.getTerm(),
                request.getGeneration()
            );
        }

        final IndexShard shard = indicesService.indexServiceSafe(request.shardId().getIndex()).getShard(request.shardId().id());

        SubscribableListener

            // Step 1: wait to ensure the shard is no longer in the process of starting up: when this step completes the shard either now
            // has an engine, or it closed without ever creating an engine.
            .newForked(shard::waitForEngineOrClosedShard)

            // Step 2: grab the shard's SearchEngine engine, failing if the engine is absent or closed instead
            .andThenApply(ignored -> {
                if (shard.indexSettings().getIndexMetadata().getState() == IndexMetadata.State.CLOSE) {
                    throw new IndexClosedException(request.shardId().getIndex());
                }

                final Engine engineOrNull = shard.getEngineOrNull();
                if (engineOrNull == null) {
                    throw new EngineException(shard.shardId(), "Engine not started.");
                }

                if (engineOrNull instanceof SearchEngine searchEngine) {
                    return searchEngine;
                } else {
                    final var exception = new ElasticsearchException("expecting SearchEngine but got " + engineOrNull);
                    logger.error("unexpected", exception);
                    assert false : exception;
                    throw exception;
                }
            })

            // Step 3: notify the engine of the new commit, and wait for it to finish processing this notification
            .<SearchEngine>andThen((l, searchEngine) -> searchEngine.onCommitNotification(request, l.map(v -> searchEngine)))

            // Step 4: update the things that need updating, and compute the final response containing the commits that are still in use
            .<NewCommitNotificationResponse>andThen((l, searchEngine) -> {
                shard.updateGlobalCheckpointOnReplica(searchEngine.getLastSyncedGlobalCheckpoint(), "new commit notification");
                shardSizeCollector.collectShardSize(shard.shardId());
                l.onResponse(new NewCommitNotificationResponse(searchEngine.getAcquiredPrimaryTermAndGenerations()));
            })

            // Step 5: send the response back to the primary
            .addListener(listener);
    }

    @Override
    protected NewCommitNotificationResponse combineUnpromotableShardResponses(
        List<NewCommitNotificationResponse> newCommitNotificationResponses
    ) {
        return NewCommitNotificationResponse.combine(newCommitNotificationResponses);
    }

    @Override
    protected NewCommitNotificationResponse readResponse(StreamInput in) throws IOException {
        return new NewCommitNotificationResponse(in);
    }

    @Override
    protected NewCommitNotificationResponse emptyResponse() {
        return NewCommitNotificationResponse.EMPTY;
    }
}
