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
package org.elasticsearch.index.replication;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.replication.ReplicationOperation;
import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.support.replication.TransportWriteAction;
import org.elasticsearch.action.support.replication.TransportWriteActionTestHelper;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingHelper;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.indices.recovery.RecoveryTarget;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.action.index.TransportIndexAction.executeIndexRequestOnPrimary;
import static org.elasticsearch.action.index.TransportIndexAction.executeIndexRequestOnReplica;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public abstract class ESIndexLevelReplicationTestCase extends IndexShardTestCase {

    protected final Index index = new Index("test", "uuid");
    private final ShardId shardId = new ShardId(index, 0);
    private final Map<String, String> indexMapping = Collections.singletonMap("type", "{ \"type\": {} }");

    protected ReplicationGroup createGroup(int replicas) throws IOException {
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, replicas)
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        IndexMetaData.Builder metaData = IndexMetaData.builder(index.getName())
            .settings(settings)
            .primaryTerm(0, 1);
        for (Map.Entry<String, String> typeMapping : indexMapping.entrySet()) {
            metaData.putMapping(typeMapping.getKey(), typeMapping.getValue());
        }
        return new ReplicationGroup(metaData.build());
    }

    protected DiscoveryNode getDiscoveryNode(String id) {
        return new DiscoveryNode(id, id, buildNewFakeTransportAddress(), Collections.emptyMap(),
            Collections.singleton(DiscoveryNode.Role.DATA), Version.CURRENT);
    }


    protected class ReplicationGroup implements AutoCloseable, Iterable<IndexShard> {
        private final IndexShard primary;
        private final List<IndexShard> replicas;
        private final IndexMetaData indexMetaData;
        private final AtomicInteger replicaId = new AtomicInteger();
        private final AtomicInteger docId = new AtomicInteger();
        boolean closed = false;

        ReplicationGroup(final IndexMetaData indexMetaData) throws IOException {
            primary = newShard(shardId, true, "s0", indexMetaData, null);
            replicas = new ArrayList<>();
            this.indexMetaData = indexMetaData;
            for (int i = 0; i < indexMetaData.getNumberOfReplicas(); i++) {
                addReplica();
            }
        }

        public int indexDocs(final int numOfDoc) throws Exception {
            for (int doc = 0; doc < numOfDoc; doc++) {
                final IndexRequest indexRequest = new IndexRequest(index.getName(), "type", Integer.toString(docId.incrementAndGet()))
                    .source("{}");
                final IndexResponse response = index(indexRequest);
                assertEquals(DocWriteResponse.Result.CREATED, response.getResult());
            }
            return numOfDoc;
        }

        public int appendDocs(final int numOfDoc) throws Exception {
            for (int doc = 0; doc < numOfDoc; doc++) {
                final IndexRequest indexRequest = new IndexRequest(index.getName(), "type").source("{}");
                final IndexResponse response = index(indexRequest);
                assertEquals(DocWriteResponse.Result.CREATED, response.getResult());
            }
            return numOfDoc;
        }

        public IndexResponse index(IndexRequest indexRequest) throws Exception {
            PlainActionFuture<IndexResponse> listener = new PlainActionFuture<>();
            new IndexingAction(indexRequest, listener, this).execute();
            return listener.get();
        }

        public synchronized void startAll() throws IOException {
            final DiscoveryNode pNode = getDiscoveryNode(primary.routingEntry().currentNodeId());
            primary.markAsRecovering("store", new RecoveryState(primary.routingEntry(), pNode, null));
            primary.recoverFromStore();
            primary.updateRoutingEntry(ShardRoutingHelper.moveToStarted(primary.routingEntry()));
            for (IndexShard replicaShard : replicas) {
                recoverReplica(replicaShard);
            }
        }

        public synchronized IndexShard addReplica() throws IOException {
            final IndexShard replica = newShard(shardId, false, "s" + replicaId.incrementAndGet(), indexMetaData, null);
            replicas.add(replica);
            return replica;
        }

        public void recoverReplica(IndexShard replica) throws IOException {
            recoverReplica(replica, (r, sourceNode) -> new RecoveryTarget(r, sourceNode, recoveryListener, version -> {}));
        }

        public void recoverReplica(IndexShard replica, BiFunction<IndexShard, DiscoveryNode, RecoveryTarget> targetSupplier)
            throws IOException {
            recoverReplica(replica, targetSupplier, true);
        }

        public void recoverReplica(IndexShard replica, BiFunction<IndexShard, DiscoveryNode, RecoveryTarget> targetSupplier,
                                   boolean markAsRecovering) throws IOException {
            ESIndexLevelReplicationTestCase.this.recoverReplica(replica, primary, targetSupplier, markAsRecovering);
        }

        public synchronized DiscoveryNode getPrimaryNode() {
            return getDiscoveryNode(primary.routingEntry().currentNodeId());
        }

        public Future<Void> asyncRecoverReplica(IndexShard replica, BiFunction<IndexShard, DiscoveryNode, RecoveryTarget> targetSupplier)
            throws IOException {
            FutureTask<Void> task = new FutureTask<>(() -> {
                recoverReplica(replica, targetSupplier);
                return null;
            });
            threadPool.generic().execute(task);
            return task;
        }

        public synchronized void assertAllEqual(int expectedCount) throws IOException {
            Set<Uid> primaryIds = getShardDocUIDs(primary);
            assertThat(primaryIds.size(), equalTo(expectedCount));
            for (IndexShard replica : replicas) {
                Set<Uid> replicaIds = getShardDocUIDs(replica);
                Set<Uid> temp = new HashSet<>(primaryIds);
                temp.removeAll(replicaIds);
                assertThat(replica.routingEntry() + " is missing docs", temp, empty());
                temp = new HashSet<>(replicaIds);
                temp.removeAll(primaryIds);
                assertThat(replica.routingEntry() + " has extra docs", temp, empty());
            }
        }

        public synchronized void refresh(String source) {
            for (IndexShard shard : this) {
                shard.refresh(source);
            }
        }

        public synchronized void flush() {
            final FlushRequest request = new FlushRequest();
            for (IndexShard shard : this) {
                shard.flush(request);
            }
        }

        public synchronized List<ShardRouting> shardRoutings() {
            return StreamSupport.stream(this.spliterator(), false).map(IndexShard::routingEntry).collect(Collectors.toList());
        }

        @Override
        public synchronized void close() throws Exception {
            if (closed == false) {
                closed = true;
                closeShards(this);
            } else {
                throw new AlreadyClosedException("too bad");
            }
        }

        @Override
        public Iterator<IndexShard> iterator() {
            return Iterators.concat(replicas.iterator(), Collections.singleton(primary).iterator());
        }

        public IndexShard getPrimary() {
            return primary;
        }
    }

    abstract class ReplicationAction<Request extends ReplicationRequest<Request>, ReplicaRequest extends ReplicationRequest<ReplicaRequest>,
        Response extends ReplicationResponse> {
        private final Request request;
        private ActionListener<Response> listener;
        private final ReplicationGroup replicationGroup;
        private final String opType;

        public ReplicationAction(Request request, ActionListener<Response> listener,
                                 ReplicationGroup group, String opType) {
            this.request = request;
            this.listener = listener;
            this.replicationGroup = group;
            this.opType = opType;
        }

        public void execute() throws Exception {
            new ReplicationOperation<Request, ReplicaRequest, PrimaryResult>(request, new PrimaryRef(),
                new ActionListener<PrimaryResult>() {
                    @Override
                    public void onResponse(PrimaryResult result) {
                        result.respond(listener);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        listener.onFailure(e);
                    }
                }, true, new ReplicasRef(), () -> null, logger, opType) {
                @Override
                protected List<ShardRouting> getShards(ShardId shardId, ClusterState state) {
                    return replicationGroup.shardRoutings();
                }

                @Override
                protected String checkActiveShardCount() {
                    return null;
                }

                @Override
                protected Set<String> getInSyncAllocationIds(ShardId shardId, ClusterState clusterState) {
                    return replicationGroup.shardRoutings().stream().filter(ShardRouting::active).map(r -> r.allocationId().getId())
                        .collect(Collectors.toSet());
                }
            }.execute();
        }

        protected abstract PrimaryResult performOnPrimary(IndexShard primary, Request request) throws Exception;

        protected abstract void performOnReplica(ReplicaRequest request, IndexShard replica);

        class PrimaryRef implements ReplicationOperation.Primary<Request, ReplicaRequest, PrimaryResult> {

            @Override
            public ShardRouting routingEntry() {
                return replicationGroup.primary.routingEntry();
            }

            @Override
            public void failShard(String message, Exception exception) {
                throw new UnsupportedOperationException();
            }

            @Override
            public PrimaryResult perform(Request request) throws Exception {
                PrimaryResult response = performOnPrimary(replicationGroup.primary, request);
                response.replicaRequest().primaryTerm(replicationGroup.primary.getPrimaryTerm());
                return response;
            }
        }

        class ReplicasRef implements ReplicationOperation.Replicas<ReplicaRequest> {

            @Override
            public void performOn(
                ShardRouting replicaRouting,
                ReplicaRequest request,
                ActionListener<TransportResponse.Empty> listener) {
                try {
                    IndexShard replica = replicationGroup.replicas.stream()
                        .filter(s -> replicaRouting.isSameAllocation(s.routingEntry())).findFirst().get();
                    performOnReplica(request, replica);
                    listener.onResponse(TransportResponse.Empty.INSTANCE);
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            }

            @Override
            public void failShard(ShardRouting replica, long primaryTerm, String message, Exception exception, Runnable onSuccess,
                                  Consumer<Exception> onPrimaryDemoted, Consumer<Exception> onIgnoredFailure) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void markShardCopyAsStale(ShardId shardId, String allocationId, long primaryTerm, Runnable onSuccess,
                                             Consumer<Exception> onPrimaryDemoted, Consumer<Exception> onIgnoredFailure) {
                throw new UnsupportedOperationException();
            }
        }

        class PrimaryResult implements ReplicationOperation.PrimaryResult<ReplicaRequest> {
            final ReplicaRequest replicaRequest;
            final Response finalResponse;

            public PrimaryResult(ReplicaRequest replicaRequest, Response finalResponse) {
                this.replicaRequest = replicaRequest;
                this.finalResponse = finalResponse;
            }

            @Override
            public ReplicaRequest replicaRequest() {
                return replicaRequest;
            }

            @Override
            public void setShardInfo(ReplicationResponse.ShardInfo shardInfo) {
                finalResponse.setShardInfo(shardInfo);
            }

            public void respond(ActionListener<Response> listener) {
                listener.onResponse(finalResponse);
            }
        }
    }

    class IndexingAction extends ReplicationAction<IndexRequest, IndexRequest, IndexResponse> {

        public IndexingAction(IndexRequest request, ActionListener<IndexResponse> listener, ReplicationGroup replicationGroup) {
            super(request, listener, replicationGroup, "indexing");
            request.process(null, true, request.index());
        }

        @Override
        protected PrimaryResult performOnPrimary(IndexShard primary, IndexRequest request) throws Exception {
            TransportWriteAction.PrimaryOperationResult<IndexResponse> result = executeIndexRequestOnPrimary(request, primary,
                null);
            request.primaryTerm(primary.getPrimaryTerm());
            TransportWriteActionTestHelper.performPostWriteActions(primary, request, result.getLocation(), logger);
            return new PrimaryResult(request, result.getResponse());
        }

        @Override
        protected void performOnReplica(IndexRequest request, IndexShard replica) {
            TransportWriteAction.ReplicaOperationResult index = executeIndexRequestOnReplica(request, replica);
            TransportWriteActionTestHelper.performPostWriteActions(replica, request, index.getLocation(), logger);
        }
    }
}
