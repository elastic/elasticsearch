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

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexNotFoundException;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.index.TransportIndexAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.replication.ReplicationOperation;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.support.replication.TransportWriteAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingHelper;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.LocalTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MapperTestUtils;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.cache.query.DisabledQueryCache;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.index.store.DirectoryService;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.recovery.RecoveryFailedException;
import org.elasticsearch.indices.recovery.RecoverySourceHandler;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.indices.recovery.RecoveryTarget;
import org.elasticsearch.indices.recovery.RecoveryTargetService;
import org.elasticsearch.indices.recovery.StartRecoveryRequest;
import org.elasticsearch.test.DummyShardLock;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public abstract class ESIndexLevelReplicationTestCase extends ESTestCase {

    protected ThreadPool threadPool;
    private final Index index = new Index("test", "uuid");
    private final ShardId shardId = new ShardId(index, 0);
    private final Map<String, String> indexMapping = Collections.singletonMap("type", "{ \"type\": {} }");
    protected static final RecoveryTargetService.RecoveryListener recoveryListener = new RecoveryTargetService.RecoveryListener() {
        @Override
        public void onRecoveryDone(RecoveryState state) {

        }

        @Override
        public void onRecoveryFailure(RecoveryState state, RecoveryFailedException e, boolean sendShardFailure) {
            fail(ExceptionsHelper.detailedMessage(e));
        }
    };


    @TestLogging("index.shard:TRACE,index.replication:TRACE,indices.recovery:TRACE")
    public void testIndexingDuringFileRecovery() throws Exception {
        try (ReplicationGroup shards = createGroup(randomInt(1))) {
            shards.startAll();
            int docs = shards.indexDocs(randomInt(50));
            shards.flush();
            IndexShard replica = shards.addReplica();
            final CountDownLatch recoveryBlocked = new CountDownLatch(1);
            final CountDownLatch releaseRecovery = new CountDownLatch(1);
            final Future<Void> recoveryFuture = shards.asyncRecoverReplica(replica,
                new BiFunction<IndexShard, DiscoveryNode, RecoveryTarget>() {
                @Override
                public RecoveryTarget apply(IndexShard indexShard, DiscoveryNode node) {
                    return new RecoveryTarget(indexShard, node, recoveryListener, version -> {}) {
                        @Override
                        public void renameAllTempFiles() throws IOException {
                            super.renameAllTempFiles();
                            recoveryBlocked.countDown();
                            try {
                                releaseRecovery.await();
                            } catch (InterruptedException e) {
                                throw new IOException("terminated by interrupt", e);
                            }
                        }
                    };
                }
            });

            recoveryBlocked.await();
            docs += shards.indexDocs(randomInt(20));
            releaseRecovery.countDown();
            recoveryFuture.get();

            shards.assertAllEqual(docs);
        }
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getClass().getName());
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
    }

    private Store createStore(IndexSettings indexSettings, ShardPath shardPath) throws IOException {
        final ShardId shardId = shardPath.getShardId();
        final DirectoryService directoryService = new DirectoryService(shardId, indexSettings) {
            @Override
            public Directory newDirectory() throws IOException {
                return newFSDirectory(shardPath.resolveIndex());
            }

            @Override
            public long throttleTimeInNanos() {
                return 0;
            }
        };
        return new Store(shardId, indexSettings, directoryService, new DummyShardLock(shardId));
    }

    protected ReplicationGroup createGroup(int replicas) throws IOException {
        final Path homePath = createTempDir();
        Settings build = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, replicas)
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        IndexMetaData metaData = IndexMetaData.builder(index.getName()).settings(build).primaryTerm(0, 1).build();
        return new ReplicationGroup(metaData, homePath);
    }

    protected DiscoveryNode getDiscoveryNode(String id) {
        return new DiscoveryNode(id, id, new LocalTransportAddress(id), Collections.emptyMap(),
            Collections.singleton(DiscoveryNode.Role.DATA), Version.CURRENT);
    }

    private IndexShard newShard(boolean primary, DiscoveryNode node, IndexMetaData indexMetaData, Path homePath) throws IOException {
        // add node name to settings for propper logging
        final Settings nodeSettings = Settings.builder().put("node.name", node.getName()).build();
        final IndexSettings indexSettings = new IndexSettings(indexMetaData, nodeSettings);
        ShardRouting shardRouting = TestShardRouting.newShardRouting(shardId, node.getId(), primary, ShardRoutingState.INITIALIZING);
        final Path path = Files.createDirectories(homePath.resolve(node.getId()));
        final NodeEnvironment.NodePath nodePath = new NodeEnvironment.NodePath(path);
        ShardPath shardPath = new ShardPath(false, nodePath.resolve(shardId), nodePath.resolve(shardId), shardId);
        Store store = createStore(indexSettings, shardPath);
        IndexCache indexCache = new IndexCache(indexSettings, new DisabledQueryCache(indexSettings), null);
        MapperService mapperService = MapperTestUtils.newMapperService(homePath, indexSettings.getSettings());
        for (Map.Entry<String, String> type : indexMapping.entrySet()) {
            mapperService.merge(type.getKey(), new CompressedXContent(type.getValue()), MapperService.MergeReason.MAPPING_RECOVERY, true);
        }
        SimilarityService similarityService = new SimilarityService(indexSettings, Collections.emptyMap());
        final IndexEventListener indexEventListener = new IndexEventListener() {
        };
        final Engine.Warmer warmer = searcher -> {
        };
        return new IndexShard(shardRouting, indexSettings, shardPath, store, indexCache, mapperService, similarityService, null, null,
            indexEventListener, null, threadPool, BigArrays.NON_RECYCLING_INSTANCE, warmer, Collections.emptyList(),
            Collections.emptyList());
    }


    protected class ReplicationGroup implements AutoCloseable, Iterable<IndexShard> {
        private final IndexShard primary;
        private final List<IndexShard> replicas;
        private final IndexMetaData indexMetaData;
        private final Path homePath;
        private final AtomicInteger replicaId = new AtomicInteger();
        private final AtomicInteger docId = new AtomicInteger();
        boolean closed = false;

        ReplicationGroup(final IndexMetaData indexMetaData, Path homePath) throws IOException {
            primary = newShard(true, getDiscoveryNode("s0"), indexMetaData, homePath);
            replicas = new ArrayList<>();
            this.indexMetaData = indexMetaData;
            this.homePath = homePath;
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

        public IndexResponse index(IndexRequest indexRequest) throws Exception {
            PlainActionFuture<IndexingResult> listener = new PlainActionFuture<>();
            IndexingOp op = new IndexingOp(indexRequest, listener, this);
            op.execute();
            return listener.get().finalResponse;
        }

        public synchronized void startAll() throws IOException {
            final DiscoveryNode pNode = getDiscoveryNode(primary.routingEntry().currentNodeId());
            primary.markAsRecovering("store", new RecoveryState(primary.shardId(), true, RecoveryState.Type.STORE, pNode, pNode));
            primary.recoverFromStore();
            primary.updateRoutingEntry(ShardRoutingHelper.moveToStarted(primary.routingEntry()));
            for (IndexShard replicaShard : replicas) {
                recoverReplica(replicaShard,
                    (replica, sourceNode) -> new RecoveryTarget(replica, sourceNode, recoveryListener, version -> {}));
            }
        }

        public synchronized IndexShard addReplica() throws IOException {
            final IndexShard replica = newShard(false, getDiscoveryNode("s" + replicaId.incrementAndGet()), indexMetaData, homePath);
            replicas.add(replica);
            return replica;
        }
        public void recoverReplica(IndexShard replica, BiFunction<IndexShard, DiscoveryNode, RecoveryTarget> targetSupplier)
            throws IOException {
            recoverReplica(replica, targetSupplier, true);
        }

        public void recoverReplica(IndexShard replica, BiFunction<IndexShard, DiscoveryNode, RecoveryTarget> targetSupplier,
                                   boolean markAsRecovering)
            throws IOException {
            final DiscoveryNode pNode = getPrimaryNode();
            final DiscoveryNode rNode = getDiscoveryNode(replica.routingEntry().currentNodeId());
            if (markAsRecovering) {
                replica.markAsRecovering("remote", new RecoveryState(replica.shardId(), false, RecoveryState.Type.REPLICA, pNode, rNode));
            } else {
                assertEquals(replica.state(), IndexShardState.RECOVERING);
            }
            replica.prepareForIndexRecovery();
            RecoveryTarget recoveryTarget = targetSupplier.apply(replica, pNode);
            StartRecoveryRequest request = new StartRecoveryRequest(replica.shardId(), pNode, rNode,
                getMetadataSnapshotOrEmpty(replica), RecoveryState.Type.REPLICA, 0);
            RecoverySourceHandler recovery = new RecoverySourceHandler(primary, recoveryTarget, request, () -> 0L, e -> () -> {},
                (int) ByteSizeUnit.MB.toKB(1), logger);
            recovery.recoverToTarget();
            recoveryTarget.markAsDone();
            replica.updateRoutingEntry(ShardRoutingHelper.moveToStarted(replica.routingEntry()));
        }

        private Store.MetadataSnapshot getMetadataSnapshotOrEmpty(IndexShard replica) throws IOException {
            Store.MetadataSnapshot result;
            try {
                result = replica.snapshotStoreMetadata();
            } catch (IndexNotFoundException e) {
                // OK!
                result = Store.MetadataSnapshot.EMPTY;
            } catch (IOException e) {
                logger.warn("failed read store, treating as empty", e);
                result = Store.MetadataSnapshot.EMPTY;
            }
            return result;
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

        private Set<Uid> getShardDocUIDs(final IndexShard shard) throws IOException {
            shard.refresh("get_uids");
            try (Engine.Searcher searcher = shard.acquireSearcher("test")) {
                Set<Uid> ids = new HashSet<>();
                for (LeafReaderContext leafContext : searcher.reader().leaves()) {
                    LeafReader reader = leafContext.reader();
                    Bits liveDocs = reader.getLiveDocs();
                    for (int i = 0; i < reader.maxDoc(); i++) {
                        if (liveDocs == null || liveDocs.get(i)) {
                            Document uuid = reader.document(i, Collections.singleton(UidFieldMapper.NAME));
                            ids.add(Uid.createUid(uuid.get(UidFieldMapper.NAME)));
                        }
                    }
                }
                return ids;
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
                for (IndexShard shard : this) {
                    shard.close("eol", false);
                    IOUtils.close(shard.store());
                }
            } else {
                throw new AlreadyClosedException("too bad");
            }
        }

        @Override
        public Iterator<IndexShard> iterator() {
            return Iterators.<IndexShard>concat(replicas.iterator(), Collections.singleton(primary).iterator());
        }

        public IndexShard getPrimary() {
            return primary;
        }
    }

    class IndexingOp extends ReplicationOperation<IndexRequest, IndexRequest, IndexingResult> {

        private final ReplicationGroup replicationGroup;

        public IndexingOp(IndexRequest request, ActionListener<IndexingResult> listener, ReplicationGroup replicationGroup) {
            super(request, new PrimaryRef(replicationGroup), listener, true, new ReplicasRef(replicationGroup),
                () -> null, logger, "indexing");
            this.replicationGroup = replicationGroup;
            request.process(null, true, request.index());
        }

        @Override
        protected List<ShardRouting> getShards(ShardId shardId, ClusterState state) {
            return replicationGroup.shardRoutings();
        }

        @Override
        protected String checkActiveShardCount() {
            return null;
        }
    }

    private static class PrimaryRef implements ReplicationOperation.Primary<IndexRequest, IndexRequest, IndexingResult> {
        final IndexShard primary;

        private PrimaryRef(ReplicationGroup replicationGroup) {
            this.primary = replicationGroup.primary;
        }

        @Override
        public ShardRouting routingEntry() {
            return primary.routingEntry();
        }

        @Override
        public void failShard(String message, Exception exception) {
            throw new UnsupportedOperationException();
        }

        @Override
        public IndexingResult perform(IndexRequest request) throws Exception {
            TransportWriteAction.WriteResult<IndexResponse> result = TransportIndexAction.executeIndexRequestOnPrimary(request, primary,
                null);
            request.primaryTerm(primary.getPrimaryTerm());
            return new IndexingResult(request, result.getResponse());
        }

    }

    private static class ReplicasRef implements ReplicationOperation.Replicas<IndexRequest> {
        private final ReplicationGroup replicationGroup;

        private ReplicasRef(ReplicationGroup replicationGroup) {
            this.replicationGroup = replicationGroup;
        }

        @Override
        public void performOn(ShardRouting replicaRouting, IndexRequest request, ActionListener<TransportResponse.Empty> listener) {
            try {
                IndexShard replica = replicationGroup.replicas.stream()
                    .filter(s -> replicaRouting.isSameAllocation(s.routingEntry())).findFirst().get();
                TransportIndexAction.executeIndexRequestOnReplica(request, replica);
                listener.onResponse(TransportResponse.Empty.INSTANCE);
            } catch (Exception t) {
                listener.onFailure(t);
            }
        }

        @Override
        public void failShard(ShardRouting replica, long primaryTerm, String message, Exception exception, Runnable onSuccess,
                              Consumer<Exception> onPrimaryDemoted, Consumer<Exception> onIgnoredFailure) {
            throw new UnsupportedOperationException();
        }
    }


    private static class IndexingResult implements ReplicationOperation.PrimaryResult<IndexRequest> {
        final IndexRequest replicaRequest;
        final IndexResponse finalResponse;

        public IndexingResult(IndexRequest replicaRequest, IndexResponse finalResponse) {
            this.replicaRequest = replicaRequest;
            this.finalResponse = finalResponse;
        }

        @Override
        public IndexRequest replicaRequest() {
            return replicaRequest;
        }

        @Override
        public void setShardInfo(ReplicationResponse.ShardInfo shardInfo) {
            finalResponse.setShardInfo(shardInfo);
        }

        public void respond(ActionListener<IndexResponse> listener) {
            listener.onResponse(finalResponse);
        }
    }

}
