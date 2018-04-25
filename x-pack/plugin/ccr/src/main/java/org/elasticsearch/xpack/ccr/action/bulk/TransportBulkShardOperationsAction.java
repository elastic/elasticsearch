/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action.bulk;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.replication.TransportWriteAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.MapperException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

public class TransportBulkShardOperationsAction
        extends TransportWriteAction<BulkShardOperationsRequest, BulkShardOperationsRequest, BulkShardOperationsResponse> {

    @Inject
    public TransportBulkShardOperationsAction(
            final Settings settings,
            final TransportService transportService,
            final ClusterService clusterService,
            final IndicesService indicesService,
            final ThreadPool threadPool,
            final ShardStateAction shardStateAction,
            final ActionFilters actionFilters,
            final IndexNameExpressionResolver indexNameExpressionResolver) {
        super(
                settings,
                BulkShardOperationsAction.NAME,
                transportService,
                clusterService,
                indicesService,
                threadPool,
                shardStateAction,
                actionFilters,
                indexNameExpressionResolver,
                BulkShardOperationsRequest::new,
                BulkShardOperationsRequest::new,
                ThreadPool.Names.WRITE);
    }

    @Override
    protected WritePrimaryResult<BulkShardOperationsRequest, BulkShardOperationsResponse> shardOperationOnPrimary(
            final BulkShardOperationsRequest request, final IndexShard primary) throws Exception {
        final Translog.Location location = applyTranslogOperations(request, primary, Engine.Operation.Origin.PRIMARY);
        return new WritePrimaryResult<>(request, new BulkShardOperationsResponse(), location, null, primary, logger);
    }

    @Override
    protected WriteReplicaResult<BulkShardOperationsRequest> shardOperationOnReplica(
            final BulkShardOperationsRequest request, final IndexShard replica) throws Exception {
        final Translog.Location location = applyTranslogOperations(request, replica, Engine.Operation.Origin.REPLICA);
        return new WriteReplicaResult<>(request, location, null, replica, logger);
    }

    private Translog.Location applyTranslogOperations(
            final BulkShardOperationsRequest request, final IndexShard shard, final Engine.Operation.Origin origin) throws IOException {
        Translog.Location location = null;
        for (final Translog.Operation operation : request.getOperations()) {
            final Engine.Result result = shard.applyTranslogOperation(operation, origin, m -> {
                // TODO: Figure out how to deal best with dynamic mapping updates from the leader side:
                throw new MapperException("dynamic mapping updates are not allowed in follow shards [" + operation + "]");
            });
            assert result.getSeqNo() == operation.seqNo();
            assert result.hasFailure() == false;
            location = locationToSync(location, result.getTranslogLocation());
        }
        assert request.getOperations().length == 0 || location != null;
        return location;
    }

    @Override
    protected BulkShardOperationsResponse newResponseInstance() {
        return new BulkShardOperationsResponse();
    }

}
