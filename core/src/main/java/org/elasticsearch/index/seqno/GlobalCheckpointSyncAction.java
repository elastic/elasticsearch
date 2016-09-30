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
package org.elasticsearch.index.seqno;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

public class GlobalCheckpointSyncAction extends TransportReplicationAction<GlobalCheckpointSyncAction.PrimaryRequest,
    GlobalCheckpointSyncAction.ReplicaRequest, ReplicationResponse> {

    public static String ACTION_NAME = "indices:admin/seq_no/global_checkpoint_sync";

    @Inject
    public GlobalCheckpointSyncAction(Settings settings, TransportService transportService,
                                      ClusterService clusterService, IndicesService indicesService,
                                      ThreadPool threadPool, ShardStateAction shardStateAction,
                                      ActionFilters actionFilters,
                                      IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, ACTION_NAME, transportService, clusterService, indicesService, threadPool, shardStateAction,
            actionFilters, indexNameExpressionResolver, PrimaryRequest::new, ReplicaRequest::new,
            ThreadPool.Names.SAME);
    }

    @Override
    protected ReplicationResponse newResponseInstance() {
        return new ReplicationResponse();
    }

    @Override
    protected PrimaryResult shardOperationOnPrimary(PrimaryRequest request) {
        IndexService indexService = indicesService.indexServiceSafe(request.shardId().getIndex());
        IndexShard indexShard = indexService.getShard(request.shardId().id());
        long checkpoint = indexShard.getGlobalCheckpoint();
        return new PrimaryResult(new ReplicaRequest(request, checkpoint), new ReplicationResponse());
    }

    @Override
    protected ReplicaResult shardOperationOnReplica(ReplicaRequest request) {
        IndexService indexService = indicesService.indexServiceSafe(request.shardId().getIndex());
        IndexShard indexShard = indexService.getShard(request.shardId().id());
        indexShard.updateGlobalCheckpointOnReplica(request.checkpoint);
        return new ReplicaResult();
    }

    public void updateCheckpointForShard(ShardId shardId) {
        execute(new PrimaryRequest(shardId), new ActionListener<ReplicationResponse>() {
            @Override
            public void onResponse(ReplicationResponse replicationResponse) {
                if (logger.isTraceEnabled()) {
                    logger.trace("{} global checkpoint successfully updated (shard info [{}])", shardId,
                        replicationResponse.getShardInfo());
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.debug((Supplier<?>) () -> new ParameterizedMessage("{} failed to update global checkpoint", shardId), e);
            }
        });
    }

    public static final class PrimaryRequest extends ReplicationRequest<PrimaryRequest> {

        private PrimaryRequest() {
            super();
        }

        public PrimaryRequest(ShardId shardId) {
            super(shardId);
        }
    }

    public static final class ReplicaRequest extends ReplicationRequest<GlobalCheckpointSyncAction.ReplicaRequest> {

        private long checkpoint;

        private ReplicaRequest() {
        }

        public ReplicaRequest(PrimaryRequest primaryRequest, long checkpoint) {
            super(primaryRequest.shardId());
            this.checkpoint = checkpoint;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            checkpoint = in.readZLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeZLong(checkpoint);
        }

        public long getCheckpoint() {
            return checkpoint;
        }
    }
}
