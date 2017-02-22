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
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.bulk.BulkItemRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.bulk.BulkShardResponse;
import org.elasticsearch.action.bulk.TransportSingleItemBulkWriteAction;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.replication.ReplicationOperation;
import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.support.replication.TransportReplicationAction.ReplicaResponse;
import org.elasticsearch.action.support.replication.TransportWriteActionTestHelper;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingHelper;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.seqno.GlobalCheckpointSyncAction;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.indices.recovery.RecoveryTarget;

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

import static org.elasticsearch.action.bulk.TransportShardBulkAction.executeIndexRequestOnPrimary;
import static org.elasticsearch.action.bulk.TransportShardBulkAction.executeIndexRequestOnReplica;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public abstract class ESIndexLevelReplicationTestCase extends IndexShardTestCase {

    protected final Index index = new Index("test", "uuid");
    private final ShardId shardId = new ShardId(index, 0);
    private final Map<String, String> indexMapping = Collections.singletonMap("type", "{ \"type\": {} }");

    protected ReplicationGroup createGroup(int replicas) throws IOException {
        IndexMetaData metaData = buildIndexMetaData(replicas);
        return new ReplicationGroup(metaData);
    }

    protected IndexMetaData buildIndexMetaData(int replicas) throws IOException {
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
        return metaData.build();
    }

    protected DiscoveryNode getDiscoveryNode(String id) {
        return new DiscoveryNode(id, id, buildNewFakeTransportAddress(), Collections.emptyMap(),
            Collections.singleton(DiscoveryNode.Role.DATA), Version.CURRENT);
    }

    protected class ReplicationGroup implements AutoCloseable, Iterable<IndexShard> {
        private IndexShard primary;
        private IndexMetaData indexMetaData;
        private final List<IndexShard> replicas;
        private final AtomicInteger replicaId = new AtomicInteger();
        private final AtomicInteger docId = new AtomicInteger();
        boolean closed = false;

        ReplicationGroup(final IndexMetaData indexMetaData) throws IOException {
            final ShardRouting primaryRouting = this.createShardRouting("s0", true);
            primary = newShard(primaryRouting, indexMetaData, null, this::syncGlobalCheckpoint, getEngineFactory(primaryRouting));
            replicas = new ArrayList<>();
            this.indexMetaData = indexMetaData;
            updateAllocationIDsOnPrimary();
            for (int i = 0; i < indexMetaData.getNumberOfReplicas(); i++) {
                addReplica();
            }
        }

        private ShardRouting createShardRouting(String nodeId, boolean primary) {
            return TestShardRouting.newShardRouting(shardId, nodeId, primary, ShardRoutingState.INITIALIZING,
                primary ? RecoverySource.StoreRecoverySource.EMPTY_STORE_INSTANCE : RecoverySource.PeerRecoverySource.INSTANCE);
        }

        protected EngineFactory getEngineFactory(ShardRouting routing) {
            return null;
        }

        public int indexDocs(final int numOfDoc) throws Exception {
            for (int doc = 0; doc < numOfDoc; doc++) {
                final IndexRequest indexRequest = new IndexRequest(index.getName(), "type", Integer.toString(docId.incrementAndGet()))
                    .source("{}", XContentType.JSON);
                final IndexResponse response = index(indexRequest);
                assertEquals(DocWriteResponse.Result.CREATED, response.getResult());
            }
            primary.updateGlobalCheckpointOnPrimary();
            return numOfDoc;
        }

        public int appendDocs(final int numOfDoc) throws Exception {
            for (int doc = 0; doc < numOfDoc; doc++) {
                final IndexRequest indexRequest = new IndexRequest(index.getName(), "type").source("{}", XContentType.JSON);
                final IndexResponse response = index(indexRequest);
                assertEquals(DocWriteResponse.Result.CREATED, response.getResult());
            }
            primary.updateGlobalCheckpointOnPrimary();
            return numOfDoc;
        }

        public IndexResponse index(IndexRequest indexRequest) throws Exception {
            PlainActionFuture<IndexResponse> listener = new PlainActionFuture<>();
            final ActionListener<BulkShardResponse> wrapBulkListener = ActionListener.wrap(
                    bulkShardResponse -> listener.onResponse(bulkShardResponse.getResponses()[0].getResponse()),
                    listener::onFailure);
            BulkItemRequest[] items = new BulkItemRequest[1];
            items[0] = new TestBulkItemRequest(0, indexRequest);
            BulkShardRequest request = new BulkShardRequest(shardId, indexRequest.getRefreshPolicy(), items);
            new IndexingAction(request, wrapBulkListener, this).execute();
            return listener.get();
        }

        /** BulkItemRequest exposing get/set primary response */
        public class TestBulkItemRequest extends BulkItemRequest {

            TestBulkItemRequest(int id, DocWriteRequest request) {
                super(id, request);
            }

            @Override
            protected void setPrimaryResponse(BulkItemResponse primaryResponse) {
                super.setPrimaryResponse(primaryResponse);
            }

            @Override
            protected BulkItemResponse getPrimaryResponse() {
                return super.getPrimaryResponse();
            }
        }

        public synchronized void startAll() throws IOException {
            startReplicas(replicas.size());
        }

        public synchronized int startReplicas(int numOfReplicasToStart) throws IOException {
            if (primary.routingEntry().initializing()) {
                startPrimary();
            }
            int started = 0;
            for (IndexShard replicaShard : replicas) {
                if (replicaShard.routingEntry().initializing()) {
                    recoverReplica(replicaShard);
                    started++;
                    if (started > numOfReplicasToStart) {
                        break;
                    }
                }
            }
            return started;
        }

        public void startPrimary() throws IOException {
            final DiscoveryNode pNode = getDiscoveryNode(primary.routingEntry().currentNodeId());
            primary.markAsRecovering("store", new RecoveryState(primary.routingEntry(), pNode, null));
            primary.recoverFromStore();
            primary.updateRoutingEntry(ShardRoutingHelper.moveToStarted(primary.routingEntry()));
            updateAllocationIDsOnPrimary();
        }

        public synchronized IndexShard addReplica() throws IOException {
            final ShardRouting replicaRouting = createShardRouting("s" + replicaId.incrementAndGet(), false);
            final IndexShard replica =
                newShard(replicaRouting, indexMetaData, null, this::syncGlobalCheckpoint, getEngineFactory(replicaRouting));
            replicas.add(replica);
            updateAllocationIDsOnPrimary();
            return replica;
        }

        public synchronized IndexShard addReplicaWithExistingPath(final ShardPath shardPath, final String nodeId) throws IOException {
            final ShardRouting shardRouting = TestShardRouting.newShardRouting(
                shardId,
                nodeId,
                false, ShardRoutingState.INITIALIZING,
                RecoverySource.PeerRecoverySource.INSTANCE);

            final IndexShard newReplica = newShard(shardRouting, shardPath, indexMetaData, null,
                this::syncGlobalCheckpoint, getEngineFactory(shardRouting));
            replicas.add(newReplica);
            updateAllocationIDsOnPrimary();
            return newReplica;
        }

        public synchronized List<IndexShard> getReplicas() {
            return Collections.unmodifiableList(replicas);
        }

        /**
         * promotes the specific replica as the new primary
         */
        public synchronized void promoteReplicaToPrimary(IndexShard replica) throws IOException {
            final long newTerm = indexMetaData.primaryTerm(shardId.id()) + 1;
            IndexMetaData.Builder newMetaData =
                IndexMetaData.builder(indexMetaData).primaryTerm(shardId.id(), newTerm);
            indexMetaData = newMetaData.build();
            for (IndexShard shard: replicas) {
                shard.updatePrimaryTerm(newTerm);
            }
            boolean found = replicas.remove(replica);
            assert found;
            primary = replica;
            replica.updateRoutingEntry(replica.routingEntry().moveActiveReplicaToPrimary());
            updateAllocationIDsOnPrimary();
        }

        synchronized boolean removeReplica(IndexShard replica) {
            final boolean removed = replicas.remove(replica);
            if (removed) {
                updateAllocationIDsOnPrimary();
            }
            return removed;
        }

        public void recoverReplica(IndexShard replica) throws IOException {
            recoverReplica(replica, (r, sourceNode) -> new RecoveryTarget(r, sourceNode, recoveryListener, version -> {}));
        }

        public void recoverReplica(IndexShard replica, BiFunction<IndexShard, DiscoveryNode, RecoveryTarget> targetSupplier)
            throws IOException {
            recoverReplica(replica, targetSupplier, true);
        }

        public void recoverReplica(
            IndexShard replica,
            BiFunction<IndexShard, DiscoveryNode, RecoveryTarget> targetSupplier,
            boolean markAsRecovering) throws IOException {
            ESIndexLevelReplicationTestCase.this.recoverReplica(replica, primary, targetSupplier, markAsRecovering);
            updateAllocationIDsOnPrimary();
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

        private void syncGlobalCheckpoint() {
            PlainActionFuture<ReplicationResponse> listener = new PlainActionFuture<>();
            try {
                new GlobalCheckpointSync(listener, this).execute();
                listener.get();
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }

        private void updateAllocationIDsOnPrimary() {
            Set<String> active = new HashSet<>();
            Set<String> initializing = new HashSet<>();
            for (ShardRouting shard: shardRoutings()) {
                if (shard.active()) {
                    active.add(shard.allocationId().getId());
                } else {
                    initializing.add(shard.allocationId().getId());
                }
            }
            primary.updateAllocationIdsFromMaster(active, initializing);
        }
    }

    abstract class ReplicationAction<Request extends ReplicationRequest<Request>,
        ReplicaRequest extends ReplicationRequest<ReplicaRequest>,
        Response extends ReplicationResponse> {
        private final Request request;
        private ActionListener<Response> listener;
        private final ReplicationGroup replicationGroup;
        private final String opType;

        ReplicationAction(Request request, ActionListener<Response> listener,
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

        protected abstract void performOnReplica(ReplicaRequest request, IndexShard replica) throws IOException;

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

            @Override
            public void updateLocalCheckpointForShard(String allocationId, long checkpoint) {
                replicationGroup.getPrimary().updateLocalCheckpointForShard(allocationId, checkpoint);
            }

            @Override
            public long localCheckpoint() {
                return replicationGroup.getPrimary().getLocalCheckpoint();
            }
        }

        class ReplicasRef implements ReplicationOperation.Replicas<ReplicaRequest> {

            @Override
            public void performOn(
                ShardRouting replicaRouting,
                ReplicaRequest request,
                ActionListener<ReplicationOperation.ReplicaResponse> listener) {
                try {
                    IndexShard replica = replicationGroup.replicas.stream()
                        .filter(s -> replicaRouting.isSameAllocation(s.routingEntry())).findFirst().get();
                    performOnReplica(request, replica);
                    listener.onResponse(new ReplicaResponse(replica.routingEntry().allocationId().getId(), replica.getLocalCheckpoint()));
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            }

            @Override
            public void failShardIfNeeded(ShardRouting replica, long primaryTerm, String message, Exception exception,
                                          Runnable onSuccess, Consumer<Exception> onPrimaryDemoted,
                                          Consumer<Exception> onIgnoredFailure) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void markShardCopyAsStaleIfNeeded(ShardId shardId, String allocationId, long primaryTerm, Runnable onSuccess,
                                                     Consumer<Exception> onPrimaryDemoted, Consumer<Exception> onIgnoredFailure) {
                throw new UnsupportedOperationException();
            }
        }

        class PrimaryResult implements ReplicationOperation.PrimaryResult<ReplicaRequest> {
            final ReplicaRequest replicaRequest;
            final Response finalResponse;

            PrimaryResult(ReplicaRequest replicaRequest, Response finalResponse) {
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

    class IndexingAction extends ReplicationAction<BulkShardRequest, BulkShardRequest, BulkShardResponse> {

        IndexingAction(BulkShardRequest request, ActionListener<BulkShardResponse> listener, ReplicationGroup replicationGroup) {
            super(request, listener, replicationGroup, "indexing");
        }

        @Override
        protected PrimaryResult performOnPrimary(IndexShard primary, BulkShardRequest request) throws Exception {
            final IndexRequest indexRequest = (IndexRequest) request.items()[0].request();
            indexRequest.process(null, request.index());
            final IndexResponse indexResponse = indexOnPrimary(indexRequest, primary);
            BulkItemResponse[] itemResponses = new BulkItemResponse[1];
            itemResponses[0] = new BulkItemResponse(0, indexRequest.opType(), indexResponse);
            ((ReplicationGroup.TestBulkItemRequest) request.items()[0]).setPrimaryResponse(itemResponses[0]);
            return new PrimaryResult(request, new BulkShardResponse(primary.shardId(), itemResponses));
        }

        @Override
        protected void performOnReplica(BulkShardRequest request, IndexShard replica) throws IOException {
            final ReplicationGroup.TestBulkItemRequest bulkItemRequest = ((ReplicationGroup.TestBulkItemRequest) request.items()[0]);
            final DocWriteResponse primaryResponse = bulkItemRequest.getPrimaryResponse().getResponse();
            indexOnReplica(primaryResponse, ((IndexRequest) bulkItemRequest.request()), replica);
        }
    }

    /**
     * indexes the given requests on the supplied primary, modifying it for replicas
     */
    protected IndexResponse indexOnPrimary(IndexRequest request, IndexShard primary) throws Exception {
        final Engine.IndexResult indexResult = executeIndexRequestOnPrimary(request, primary,
                null);
        request.primaryTerm(primary.getPrimaryTerm());
        TransportWriteActionTestHelper.performPostWriteActions(primary, request, indexResult.getTranslogLocation(), logger);
        return new IndexResponse(
            primary.shardId(),
            request.type(),
            request.id(),
            indexResult.getSeqNo(),
            indexResult.getVersion(),
            indexResult.isCreated());
    }

    /**
     * indexes the given requests on the supplied replica shard
     */
    protected void indexOnReplica(DocWriteResponse response, IndexRequest request, IndexShard replica) throws IOException {
        final Engine.IndexResult result = executeIndexRequestOnReplica(response, request, replica);
        TransportWriteActionTestHelper.performPostWriteActions(replica, request, result.getTranslogLocation(), logger);
    }

    class GlobalCheckpointSync extends ReplicationAction<GlobalCheckpointSyncAction.PrimaryRequest,
        GlobalCheckpointSyncAction.ReplicaRequest, ReplicationResponse> {

        GlobalCheckpointSync(ActionListener<ReplicationResponse> listener, ReplicationGroup replicationGroup) {
            super(new GlobalCheckpointSyncAction.PrimaryRequest(replicationGroup.getPrimary().shardId()), listener,
                replicationGroup, "global_ckp");
        }

        @Override
        protected PrimaryResult performOnPrimary(IndexShard primary,
                                                 GlobalCheckpointSyncAction.PrimaryRequest request) throws Exception {
            primary.getTranslog().sync();
            return new PrimaryResult(new GlobalCheckpointSyncAction.ReplicaRequest(request, primary.getGlobalCheckpoint()),
                new ReplicationResponse());
        }

        @Override
        protected void performOnReplica(GlobalCheckpointSyncAction.ReplicaRequest request, IndexShard replica) throws IOException {
            replica.updateGlobalCheckpointOnReplica(request.getCheckpoint());
            replica.getTranslog().sync();
        }
    }

}
