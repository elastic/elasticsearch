/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.indices.flush;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.support.replication.TransportBroadcastReplicationAction;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.transport.TransportService;

import java.util.List;

/**
 * Flush Action.
 */
public class TransportFlushAction
        extends TransportBroadcastReplicationAction<FlushRequest, FlushResponse, ShardFlushRequest, ReplicationResponse> {

    @Inject
    public TransportFlushAction(ClusterService clusterService, TransportService transportService, NodeClient client,
                                ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(FlushAction.NAME, FlushRequest::new, clusterService, transportService, client, actionFilters, indexNameExpressionResolver,
            TransportShardFlushAction.TYPE);
    }

    @Override
    protected ReplicationResponse newShardResponse() {
        return new ReplicationResponse();
    }

    @Override
    protected ShardFlushRequest newShardRequest(FlushRequest request, ShardId shardId) {
        return new ShardFlushRequest(request, shardId);
    }

    @Override
    protected FlushResponse newResponse(int successfulShards, int failedShards, int totalNumCopies, List
            <DefaultShardOperationFailedException> shardFailures) {
        return new FlushResponse(totalNumCopies, successfulShards, failedShards, shardFailures);
    }
}
