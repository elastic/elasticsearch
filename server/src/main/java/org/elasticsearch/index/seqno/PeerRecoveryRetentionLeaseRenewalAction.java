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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.replication.ReplicationOperation;
import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.seqno.PeerRecoveryRetentionLeaseRenewalAction.Request;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

/**
 * Background action to renew retention leases held to ensure that enough history is retained to perform a peer recovery if needed. This
 * action renews the leases for each copy of the shard, advancing the corresponding sequence number, and thereby releases any operations
 * that are now contained in a safe commit on every copy since they are no longer needed.
 */
public class PeerRecoveryRetentionLeaseRenewalAction extends TransportReplicationAction<Request, Request, ReplicationResponse> {

    public static final String ACTION_NAME = "indices:admin/seq_no/peer_recovery_retention_lease_renewal";

    @Inject
    public PeerRecoveryRetentionLeaseRenewalAction(
        final Settings settings,
        final TransportService transportService,
        final ClusterService clusterService,
        final IndicesService indicesService,
        final ThreadPool threadPool,
        final ShardStateAction shardStateAction,
        final ActionFilters actionFilters,
        final IndexNameExpressionResolver indexNameExpressionResolver) {

        super(settings, ACTION_NAME, transportService, clusterService, indicesService, threadPool, shardStateAction, actionFilters,
            indexNameExpressionResolver, Request::new, Request::new, Names.MANAGEMENT);
    }

    @Override
    protected ReplicationResponse newResponseInstance() {
        return new ReplicationResponse();
    }

    @Override
    protected PrimaryResult<Request, ReplicationResponse> shardOperationOnPrimary(Request shardRequest, IndexShard primary) {
        primary.renewPeerRecoveryRetentionLeaseForPrimary();
        return new PrimaryResult<>(shardRequest, new ReplicationResponse());
    }

    @Override
    protected ReplicaResponse readReplicaResponse(StreamInput in) throws IOException {
        return new ShardCopyResponse(in);
    }

    @Override
    protected ReplicaResult shardOperationOnReplica(Request shardRequest, IndexShard replica) {
        return new ReplicaResult() {
            @Override
            public ReplicaResponse getReplicaResponse(IndexShard replica) {
                return new ShardCopyResponse(replica.getLocalCheckpoint(), replica.getGlobalCheckpoint(),
                    replica.getLocalCheckpointOfSafeCommit());
            }
        };
    }

    @Override
    protected void handleReplicaResponse(ShardRouting shard, ReplicationOperation.ReplicaResponse response) {
        assert response instanceof ShardCopyResponse : response.getClass();
        final ShardCopyResponse shardCopyResponse = (ShardCopyResponse) response; // TODO introduce type parameter rather than cast here
        indicesService.indexServiceSafe(shard.index()).getShard(shard.id())
            .renewPeerRecoveryRetentionLeaseForReplica(shard, shardCopyResponse.localCheckpointOfSafeCommit);
    }

    public void renewPeerRecoveryRetentionLease(ShardId shardId) {
        execute(new Request(shardId), new ActionListener<ReplicationResponse>() {
            @Override
            public void onResponse(ReplicationResponse response) {
            }

            @Override
            public void onFailure(Exception e) {
            }
        });
    }

    static final class ShardCopyResponse extends ReplicaResponse {
        private long localCheckpointOfSafeCommit;

        ShardCopyResponse(long localCheckpoint, long globalCheckpoint, long localCheckpointOfSafeCommit) {
            super(localCheckpoint, globalCheckpoint);
            this.localCheckpointOfSafeCommit = localCheckpointOfSafeCommit;
        }

        ShardCopyResponse(StreamInput in) throws IOException {
            super();
            super.readFrom(in);
            localCheckpointOfSafeCommit = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeLong(localCheckpointOfSafeCommit);
        }

        @Override
        public void readFrom(StreamInput in) {
            throw new UnsupportedOperationException("use Writable not Streamable");
        }
    }

    static final class Request extends ReplicationRequest<Request> {
        Request() {
        }

        Request(ShardId shardId) {
            super(shardId);
        }

        @Override
        public String toString() {
            return "request for minimum seqno needed for peer recovery for " + shardId;
        }
    }
}
