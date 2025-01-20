/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.refresh;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.broadcast.unpromotable.TransportBroadcastUnpromotableAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;

public class TransportUnpromotableShardRefreshAction extends TransportBroadcastUnpromotableAction<
    UnpromotableShardRefreshRequest,
    ActionResponse.Empty> {

    public static final String NAME = "indices:admin/refresh/unpromotable";

    static {
        // noinspection ConstantValue just for documentation
        assert NAME.equals(RefreshAction.NAME + "/unpromotable");
    }

    public static final Setting<TimeValue> MAX_WAIT_TIME_FOR_REFRESH_UNBLOCK = Setting.timeSetting(
        "stateless.indices.refresh.max_wait_time_for_unblock",
        TimeValue.timeValueSeconds(30),
        Setting.Property.NodeScope
    );

    private final IndicesService indicesService;
    private final ThreadPool threadPool;
    private final TimeValue maxWaitTimeForRefreshUnblock;
    private final boolean useRefreshBlock;

    @Inject
    public TransportUnpromotableShardRefreshAction(
        ClusterService clusterService,
        TransportService transportService,
        ShardStateAction shardStateAction,
        ActionFilters actionFilters,
        IndicesService indicesService,
        ThreadPool threadPool
    ) {
        this(
            clusterService,
            transportService,
            shardStateAction,
            actionFilters,
            indicesService,
            threadPool,
            MAX_WAIT_TIME_FOR_REFRESH_UNBLOCK.get(clusterService.getSettings()),
            MetadataCreateIndexService.useRefreshBlock(clusterService.getSettings())
        );
    }

    TransportUnpromotableShardRefreshAction(
        ClusterService clusterService,
        TransportService transportService,
        ShardStateAction shardStateAction,
        ActionFilters actionFilters,
        IndicesService indicesService,
        ThreadPool threadPool,
        TimeValue maxWaitTimeForRefreshUnblock,
        boolean useRefreshBlock
    ) {
        super(
            NAME,
            clusterService,
            transportService,
            shardStateAction,
            actionFilters,
            UnpromotableShardRefreshRequest::new,
            transportService.getThreadPool().executor(ThreadPool.Names.REFRESH)
        );
        this.indicesService = indicesService;
        this.threadPool = threadPool;
        this.maxWaitTimeForRefreshUnblock = maxWaitTimeForRefreshUnblock;
        this.useRefreshBlock = useRefreshBlock;
    }

    @Override
    protected void beforeDispatchingRequestToUnpromotableShards(UnpromotableShardRefreshRequest request, ActionListener<Void> listener) {
        if (useRefreshBlock == false) {
            ActionListener.completeWith(listener, () -> null);
            return;
        }

        new IndexRefreshUnBlockObserver(request.shardId().getIndexName(), listener).run();
    }

    class IndexRefreshUnBlockObserver extends AbstractRunnable {
        private final String indexName;
        private final ActionListener<Void> listener;
        private final ClusterStateObserver clusterStateObserver;

        IndexRefreshUnBlockObserver(String indexName, ActionListener<Void> listener) {
            this.indexName = indexName;
            this.listener = listener;
            this.clusterStateObserver = new ClusterStateObserver(
                clusterService,
                maxWaitTimeForRefreshUnblock,
                logger,
                threadPool.getThreadContext()
            );
        }

        @Override
        protected void doRun() throws Exception {
            var state = clusterStateObserver.setAndGetObservedState();
            var indexLevelBlockException = isIndexBlockedOnRefresh(state);
            if (indexLevelBlockException == null) {
                ActionListener.completeWith(listener, () -> null);
                return;
            }

            clusterStateObserver.waitForNextChange(new ClusterStateObserver.Listener() {
                @Override
                public void onNewClusterState(ClusterState state) {
                    run();
                }

                @Override
                public void onClusterServiceClose() {
                    listener.onFailure(new NodeClosedException(clusterService.localNode()));
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    listener.onFailure(indexLevelBlockException);
                }
            });
        }

        private ClusterBlockException isIndexBlockedOnRefresh(ClusterState state) {
            return state.blocks().indexBlockedException(ClusterBlockLevel.REFRESH, indexName);
        }

        @Override
        public void onFailure(Exception e) {
            logger.warn("Unexpected error while waiting for index refresh unblock", e);
            listener.onFailure(e);
        }
    }

    @Override
    protected void unpromotableShardOperation(
        Task task,
        UnpromotableShardRefreshRequest request,
        ActionListener<ActionResponse.Empty> responseListener
    ) {
        // In edge cases, the search shard may still in the process of being created when a refresh request arrives.
        // We simply respond OK to the request because when the search shard recovers later it will use the latest
        // commit from the proper indexing shard.
        final var indexService = indicesService.indexService(request.shardId().getIndex());
        final var shard = indexService == null ? null : indexService.getShardOrNull(request.shardId().id());
        if (shard == null) {
            responseListener.onResponse(ActionResponse.Empty.INSTANCE);
            return;
        }

        ActionListener.run(responseListener, listener -> {
            shard.waitForPrimaryTermAndGeneration(
                request.getPrimaryTerm(),
                request.getSegmentGeneration(),
                listener.map(l -> ActionResponse.Empty.INSTANCE)
            );
        });
    }

    @Override
    protected ActionResponse.Empty combineUnpromotableShardResponses(List<ActionResponse.Empty> empties) {
        return ActionResponse.Empty.INSTANCE;
    }

    @Override
    protected ActionResponse.Empty readResponse(StreamInput in) {
        return ActionResponse.Empty.INSTANCE;
    }

    @Override
    protected ActionResponse.Empty emptyResponse() {
        return ActionResponse.Empty.INSTANCE;
    }
}
