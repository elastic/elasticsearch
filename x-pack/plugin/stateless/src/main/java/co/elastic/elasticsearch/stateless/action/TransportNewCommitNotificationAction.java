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
import co.elastic.elasticsearch.stateless.engine.SearchEngine;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.broadcast.unpromotable.TransportBroadcastUnpromotableAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportNewCommitNotificationAction extends TransportBroadcastUnpromotableAction<NewCommitNotificationRequest> {

    public static final String NAME = "internal:admin/" + Stateless.NAME + "/search/new/commit";
    public static final ActionType<ActionResponse.Empty> TYPE = new ActionType<>(NAME, ignored -> ActionResponse.Empty.INSTANCE);

    private final IndicesService indicesService;

    @Inject
    public TransportNewCommitNotificationAction(
        ClusterService clusterService,
        TransportService transportService,
        ShardStateAction shardStateAction,
        ActionFilters actionFilters,
        IndicesService indicesService
    ) {
        super(
            NAME,
            clusterService,
            transportService,
            shardStateAction,
            actionFilters,
            NewCommitNotificationRequest::new,
            ThreadPool.Names.SAME
        );
        this.indicesService = indicesService;
    }

    @Override
    protected void unpromotableShardOperation(
        Task task,
        NewCommitNotificationRequest request,
        ActionListener<ActionResponse.Empty> responseListener
    ) {
        ActionListener.run(responseListener, (listener) -> {
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
            IndexShard shard = indicesService.indexServiceSafe(request.shardId().getIndex()).getShard(request.shardId().id());
            shard.readAllowed();
            if (shard.indexSettings().getIndexMetadata().getState() == IndexMetadata.State.CLOSE) {
                listener.onFailure(new IndexClosedException(request.shardId().getIndex()));
                return;
            }
            Engine engineOrNull = shard.getEngineOrNull();
            if (engineOrNull == null) {
                throw new EngineException(shard.shardId(), "Engine not started.");
            }
            if (engineOrNull instanceof SearchEngine searchEngine) {
                searchEngine.onCommitNotification(request.getCompoundCommit(), listener.map(ignored -> ActionResponse.Empty.INSTANCE));
            } else {
                assert false : "expecting SearchEngine but got " + engineOrNull;
                throw new ElasticsearchException("Engine not type SearchEngine.");
            }
        });
    }

}
