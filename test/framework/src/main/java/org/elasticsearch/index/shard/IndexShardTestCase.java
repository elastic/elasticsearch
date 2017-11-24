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
package org.elasticsearch.index.shard;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexNotFoundException;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingHelper;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MapperTestUtils;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.cache.query.DisabledQueryCache;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.store.DirectoryService;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.RecoveryFailedException;
import org.elasticsearch.indices.recovery.RecoverySourceHandler;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.indices.recovery.RecoveryTarget;
import org.elasticsearch.indices.recovery.StartRecoveryRequest;
import org.elasticsearch.node.Node;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.test.DummyShardLock;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static org.elasticsearch.cluster.routing.TestShardRouting.newShardRouting;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;

/**
 * A base class for unit tests that need to create and shutdown {@link IndexShard} instances easily,
 * containing utilities for shard creation and recoveries. See {{@link #newShard(boolean)}} and
 * {@link #newStartedShard()} for a good starting points
 */
public abstract class IndexShardTestCase extends ESTestCase {

    protected static final PeerRecoveryTargetService.RecoveryListener recoveryListener = new PeerRecoveryTargetService.RecoveryListener() {
        @Override
        public void onRecoveryDone(RecoveryState state) {

        }

        @Override
        public void onRecoveryFailure(RecoveryState state, RecoveryFailedException e, boolean sendShardFailure) {
            throw new AssertionError(e);
        }
    };

    protected ThreadPool threadPool;
    private long primaryTerm;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getClass().getName(), threadPoolSettings());
        primaryTerm = randomIntBetween(1, 100); // use random but fixed term for creating shards
    }

    @Override
    public void tearDown() throws Exception {
        try {
            ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        } finally {
            super.tearDown();
        }
    }

    public Settings threadPoolSettings() {
        return Settings.EMPTY;
    }

    private Store createStore(IndexSettings indexSettings, ShardPath shardPath) throws IOException {
        final ShardId shardId = shardPath.getShardId();
        final DirectoryService directoryService = new DirectoryService(shardId, indexSettings) {
            @Override
            public Directory newDirectory() throws IOException {
                return newFSDirectory(shardPath.resolveIndex());
            }
        };
        return new Store(shardId, indexSettings, directoryService, new DummyShardLock(shardId));
    }

    /**
     * creates a new initializing shard. The shard will have its own unique data path.
     *
     * @param primary indicates whether to a primary shard (ready to recover from an empty store) or a replica
     *                (ready to recover from another shard)
     */
    protected IndexShard newShard(boolean primary) throws IOException {
        ShardRouting shardRouting = TestShardRouting.newShardRouting(new ShardId("index", "_na_", 0), randomAlphaOfLength(10), primary,
            ShardRoutingState.INITIALIZING,
            primary ? RecoverySource.StoreRecoverySource.EMPTY_STORE_INSTANCE : RecoverySource.PeerRecoverySource.INSTANCE);
        return newShard(shardRouting);
    }

    /**
     * creates a new initializing shard. The shard will have its own unique data path.
     *
     * @param shardRouting the {@link ShardRouting} to use for this shard
     * @param listeners    an optional set of listeners to add to the shard
     */
    protected IndexShard newShard(
            final ShardRouting shardRouting,
            final IndexingOperationListener... listeners) throws IOException {
        assert shardRouting.initializing() : shardRouting;
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        IndexMetaData.Builder metaData = IndexMetaData.builder(shardRouting.getIndexName())
            .settings(settings)
            .primaryTerm(0, primaryTerm);
        return newShard(shardRouting, metaData.build(), listeners);
    }

    /**
     * creates a new initializing shard. The shard will have its own unique data path.
     *
     * @param shardId   the shard id to use
     * @param primary   indicates whether to a primary shard (ready to recover from an empty store) or a replica
     *                  (ready to recover from another shard)
     * @param listeners an optional set of listeners to add to the shard
     */
    protected IndexShard newShard(ShardId shardId, boolean primary, IndexingOperationListener... listeners) throws IOException {
        ShardRouting shardRouting = TestShardRouting.newShardRouting(shardId, randomAlphaOfLength(5), primary,
            ShardRoutingState.INITIALIZING,
            primary ? RecoverySource.StoreRecoverySource.EMPTY_STORE_INSTANCE : RecoverySource.PeerRecoverySource.INSTANCE);
        return newShard(shardRouting, listeners);
    }

    /**
     * creates a new initializing shard. The shard will will be put in its proper path under the
     * supplied node id.
     *
     * @param shardId the shard id to use
     * @param primary indicates whether to a primary shard (ready to recover from an empty store) or a replica
     *                (ready to recover from another shard)
     */
    protected IndexShard newShard(ShardId shardId, boolean primary, String nodeId, IndexMetaData indexMetaData,
                                  @Nullable IndexSearcherWrapper searcherWrapper) throws IOException {
        return newShard(shardId, primary, nodeId, indexMetaData, searcherWrapper, () -> {});
    }

    /**
     * creates a new initializing shard. The shard will will be put in its proper path under the
     * supplied node id.
     *
     * @param shardId the shard id to use
     * @param primary indicates whether to a primary shard (ready to recover from an empty store) or a replica
     *                (ready to recover from another shard)
     */
    protected IndexShard newShard(ShardId shardId, boolean primary, String nodeId, IndexMetaData indexMetaData,
                                  @Nullable IndexSearcherWrapper searcherWrapper, Runnable globalCheckpointSyncer) throws IOException {
        ShardRouting shardRouting = TestShardRouting.newShardRouting(shardId, nodeId, primary, ShardRoutingState.INITIALIZING,
            primary ? RecoverySource.StoreRecoverySource.EMPTY_STORE_INSTANCE : RecoverySource.PeerRecoverySource.INSTANCE);
        return newShard(shardRouting, indexMetaData, searcherWrapper, null, globalCheckpointSyncer);
    }


    /**
     * creates a new initializing shard. The shard will will be put in its proper path under the
     * current node id the shard is assigned to.
     *
     * @param routing       shard routing to use
     * @param indexMetaData indexMetaData for the shard, including any mapping
     * @param listeners     an optional set of listeners to add to the shard
     */
    protected IndexShard newShard(ShardRouting routing, IndexMetaData indexMetaData, IndexingOperationListener... listeners)
        throws IOException {
        return newShard(routing, indexMetaData, null, null, () -> {}, listeners);
    }

    /**
     * creates a new initializing shard. The shard will will be put in its proper path under the
     * current node id the shard is assigned to.
     * @param routing                shard routing to use
     * @param indexMetaData          indexMetaData for the shard, including any mapping
     * @param indexSearcherWrapper   an optional wrapper to be used during searchers
     * @param globalCheckpointSyncer callback for syncing global checkpoints
     * @param listeners              an optional set of listeners to add to the shard
     */
    protected IndexShard newShard(ShardRouting routing, IndexMetaData indexMetaData,
                                  @Nullable IndexSearcherWrapper indexSearcherWrapper,
                                  @Nullable EngineFactory engineFactory,
                                  Runnable globalCheckpointSyncer,
                                  IndexingOperationListener... listeners)
        throws IOException {
        // add node id as name to settings for proper logging
        final ShardId shardId = routing.shardId();
        final NodeEnvironment.NodePath nodePath = new NodeEnvironment.NodePath(createTempDir());
        ShardPath shardPath = new ShardPath(false, nodePath.resolve(shardId), nodePath.resolve(shardId), shardId);
        return newShard(routing, shardPath, indexMetaData, indexSearcherWrapper, engineFactory, globalCheckpointSyncer, listeners);
    }

    /**
     * creates a new initializing shard.
     *
     * @param routing                shard routing to use
     * @param shardPath              path to use for shard data
     * @param indexMetaData          indexMetaData for the shard, including any mapping
     * @param indexSearcherWrapper   an optional wrapper to be used during searchers
     * @param globalCheckpointSyncer callback for syncing global checkpoints
     * @param listeners              an optional set of listeners to add to the shard
     */
    protected IndexShard newShard(ShardRouting routing, ShardPath shardPath, IndexMetaData indexMetaData,
                                  @Nullable IndexSearcherWrapper indexSearcherWrapper,
                                  @Nullable EngineFactory engineFactory,
                                  Runnable globalCheckpointSyncer,
                                  IndexingOperationListener... listeners) throws IOException {
        final Settings nodeSettings = Settings.builder().put("node.name", routing.currentNodeId()).build();
        final IndexSettings indexSettings = new IndexSettings(indexMetaData, nodeSettings);
        final IndexShard indexShard;
        final Store store = createStore(indexSettings, shardPath);
        boolean success = false;
        try {
            IndexCache indexCache = new IndexCache(indexSettings, new DisabledQueryCache(indexSettings), null);
            MapperService mapperService = MapperTestUtils.newMapperService(xContentRegistry(), createTempDir(),
                    indexSettings.getSettings(), "index");
            mapperService.merge(indexMetaData, MapperService.MergeReason.MAPPING_RECOVERY, true);
            SimilarityService similarityService = new SimilarityService(indexSettings, null, Collections.emptyMap());
            final IndexEventListener indexEventListener = new IndexEventListener() {
            };
            final Engine.Warmer warmer = searcher -> {
            };
            indexShard = new IndexShard(routing, indexSettings, shardPath, store, () -> null, indexCache, mapperService, similarityService,
                engineFactory, indexEventListener, indexSearcherWrapper, threadPool,
                BigArrays.NON_RECYCLING_INSTANCE, warmer, Collections.emptyList(), Arrays.asList(listeners), globalCheckpointSyncer);
            success = true;
        } finally {
            if (success == false) {
                IOUtils.close(store);
            }
        }
        return indexShard;
    }

    /**
     * Takes an existing shard, closes it and and starts a new initialing shard at the same location
     *
     * @param listeners new listerns to use for the newly created shard
     */
    protected IndexShard reinitShard(IndexShard current, IndexingOperationListener... listeners) throws IOException {
        final ShardRouting shardRouting = current.routingEntry();
        return reinitShard(current, ShardRoutingHelper.initWithSameId(shardRouting,
            shardRouting.primary() ? RecoverySource.StoreRecoverySource.EXISTING_STORE_INSTANCE : RecoverySource.PeerRecoverySource.INSTANCE
        ), listeners);
    }

    /**
     * Takes an existing shard, closes it and and starts a new initialing shard at the same location
     *
     * @param routing   the shard routing to use for the newly created shard.
     * @param listeners new listerns to use for the newly created shard
     */
    protected IndexShard reinitShard(IndexShard current, ShardRouting routing, IndexingOperationListener... listeners) throws IOException {
        closeShards(current);
        return newShard(
                routing,
                current.shardPath(),
                current.indexSettings().getIndexMetaData(),
                null,
                current.engineFactory,
                current.getGlobalCheckpointSyncer(),
                listeners);
    }

    /**
     * creates a new empyu shard and starts it. The shard will be either a replica or a primary.
     */
    protected IndexShard newStartedShard() throws IOException {
        return newStartedShard(randomBoolean());
    }

    /**
     * creates a new empty shard and starts it.
     *
     * @param primary controls whether the shard will be a primary or a replica.
     */
    protected IndexShard newStartedShard(boolean primary) throws IOException {
        IndexShard shard = newShard(primary);
        if (primary) {
            recoverShardFromStore(shard);
        } else {
            recoveryEmptyReplica(shard);
        }
        return shard;
    }

    protected void closeShards(IndexShard... shards) throws IOException {
        closeShards(Arrays.asList(shards));
    }

    protected void closeShards(Iterable<IndexShard> shards) throws IOException {
        for (IndexShard shard : shards) {
            if (shard != null) {
                try {
                    shard.close("test", false);
                } finally {
                    IOUtils.close(shard.store());
                }
            }
        }
    }

    protected void recoverShardFromStore(IndexShard primary) throws IOException {
        primary.markAsRecovering("store", new RecoveryState(primary.routingEntry(),
            getFakeDiscoNode(primary.routingEntry().currentNodeId()),
            null));
        primary.recoverFromStore();
        updateRoutingEntry(primary, ShardRoutingHelper.moveToStarted(primary.routingEntry()));
    }

    protected static AtomicLong currentClusterStateVersion = new AtomicLong();

    public static void updateRoutingEntry(IndexShard shard, ShardRouting shardRouting) throws IOException {
        Set<String> inSyncIds =
            shardRouting.active() ? Collections.singleton(shardRouting.allocationId().getId()) : Collections.emptySet();
        IndexShardRoutingTable newRoutingTable = new IndexShardRoutingTable.Builder(shardRouting.shardId())
            .addShard(shardRouting)
            .build();
        shard.updateShardState(shardRouting, shard.getPrimaryTerm(), null, currentClusterStateVersion.incrementAndGet(),
            inSyncIds, newRoutingTable, Collections.emptySet());
    }

    protected void recoveryEmptyReplica(IndexShard replica) throws IOException {
        IndexShard primary = null;
        try {
            primary = newStartedShard(true);
            recoverReplica(replica, primary);
        } finally {
            closeShards(primary);
        }
    }

    protected DiscoveryNode getFakeDiscoNode(String id) {
        return new DiscoveryNode(id, id, buildNewFakeTransportAddress(), Collections.emptyMap(), EnumSet.allOf(DiscoveryNode.Role.class),
            Version.CURRENT);
    }

    /** recovers a replica from the given primary **/
    protected void recoverReplica(IndexShard replica, IndexShard primary) throws IOException {
        recoverReplica(replica, primary,
            (r, sourceNode) -> new RecoveryTarget(r, sourceNode, recoveryListener, version -> {
            }),
            true);
    }

    /** recovers a replica from the given primary **/
    protected void recoverReplica(final IndexShard replica,
                                  final IndexShard primary,
                                  final BiFunction<IndexShard, DiscoveryNode, RecoveryTarget> targetSupplier,
                                  final boolean markAsRecovering) throws IOException {
        IndexShardRoutingTable.Builder newRoutingTable = new IndexShardRoutingTable.Builder(replica.shardId());
        newRoutingTable.addShard(primary.routingEntry());
        if (replica.routingEntry().isRelocationTarget() == false) {
            newRoutingTable.addShard(replica.routingEntry());
        }
        recoverReplica(replica, primary, targetSupplier, markAsRecovering,
            Collections.singleton(primary.routingEntry().allocationId().getId()),
            newRoutingTable.build());
    }

    /**
     * Recovers a replica from the give primary, allow the user to supply a custom recovery target. A typical usage of a custom recovery
     * target is to assert things in the various stages of recovery.
     * @param replica                the recovery target shard
     * @param primary                the recovery source shard
     * @param targetSupplier         supplies an instance of {@link RecoveryTarget}
     * @param markAsRecovering       set to {@code false} if the replica is marked as recovering
     */
    protected final void recoverReplica(final IndexShard replica,
                                        final IndexShard primary,
                                        final BiFunction<IndexShard, DiscoveryNode, RecoveryTarget> targetSupplier,
                                        final boolean markAsRecovering,
                                        final Set<String> inSyncIds,
                                        final IndexShardRoutingTable routingTable) throws IOException {
        final DiscoveryNode pNode = getFakeDiscoNode(primary.routingEntry().currentNodeId());
        final DiscoveryNode rNode = getFakeDiscoNode(replica.routingEntry().currentNodeId());
        if (markAsRecovering) {
            replica.markAsRecovering("remote", new RecoveryState(replica.routingEntry(), pNode, rNode));
        } else {
            assertEquals(replica.state(), IndexShardState.RECOVERING);
        }
        replica.prepareForIndexRecovery();
        final RecoveryTarget recoveryTarget = targetSupplier.apply(replica, pNode);
        final String targetAllocationId = recoveryTarget.indexShard().routingEntry().allocationId().getId();

        final Store.MetadataSnapshot snapshot = getMetadataSnapshotOrEmpty(replica);
        final long startingSeqNo;
        if (snapshot.size() > 0) {
            startingSeqNo = PeerRecoveryTargetService.getStartingSeqNo(recoveryTarget);
        } else {
            startingSeqNo = SequenceNumbers.UNASSIGNED_SEQ_NO;
        }

        final StartRecoveryRequest request = new StartRecoveryRequest(replica.shardId(), targetAllocationId,
            pNode, rNode, snapshot, replica.routingEntry().primary(), 0, startingSeqNo);
        final RecoverySourceHandler recovery = new RecoverySourceHandler(
            primary,
            recoveryTarget,
            request,
            (int) ByteSizeUnit.MB.toBytes(1),
            Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), pNode.getName()).build());
        final ShardRouting initializingReplicaRouting = replica.routingEntry();
        primary.updateShardState(primary.routingEntry(), primary.getPrimaryTerm(), null, currentClusterStateVersion.incrementAndGet(),
            inSyncIds, routingTable, Collections.emptySet());
        recovery.recoverToTarget();
        recoveryTarget.markAsDone();
        IndexShardRoutingTable newRoutingTable =
            initializingReplicaRouting.isRelocationTarget() ?
                new IndexShardRoutingTable.Builder(routingTable)
                    .removeShard(primary.routingEntry())
                    .addShard(replica.routingEntry())
                    .build() :
                new IndexShardRoutingTable.Builder(routingTable)
                .removeShard(initializingReplicaRouting)
                .addShard(replica.routingEntry())
                .build();
        Set<String> inSyncIdsWithReplica = new HashSet<>(inSyncIds);
        inSyncIdsWithReplica.add(replica.routingEntry().allocationId().getId());
        // update both primary and replica shard state
        primary.updateShardState(primary.routingEntry(), primary.getPrimaryTerm(), null, currentClusterStateVersion.incrementAndGet(),
            inSyncIdsWithReplica, newRoutingTable, Collections.emptySet());
        replica.updateShardState(replica.routingEntry().moveToStarted(), replica.getPrimaryTerm(), null,
            currentClusterStateVersion.get(), inSyncIdsWithReplica, newRoutingTable, Collections.emptySet());
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

    protected Set<String> getShardDocUIDs(final IndexShard shard) throws IOException {
        shard.refresh("get_uids");
        try (Engine.Searcher searcher = shard.acquireSearcher("test")) {
            Set<String> ids = new HashSet<>();
            for (LeafReaderContext leafContext : searcher.reader().leaves()) {
                LeafReader reader = leafContext.reader();
                Bits liveDocs = reader.getLiveDocs();
                for (int i = 0; i < reader.maxDoc(); i++) {
                    if (liveDocs == null || liveDocs.get(i)) {
                        Document uuid = reader.document(i, Collections.singleton(IdFieldMapper.NAME));
                        BytesRef binaryID = uuid.getBinaryValue(IdFieldMapper.NAME);
                        ids.add(Uid.decodeId(Arrays.copyOfRange(binaryID.bytes, binaryID.offset, binaryID.offset + binaryID.length)));
                    }
                }
            }
            return ids;
        }
    }

    protected void assertDocCount(IndexShard shard, int docDount) throws IOException {
        assertThat(getShardDocUIDs(shard), hasSize(docDount));
    }

    protected void assertDocs(IndexShard shard, String... ids) throws IOException {
        final Set<String> shardDocUIDs = getShardDocUIDs(shard);
        assertThat(shardDocUIDs, contains(ids));
        assertThat(shardDocUIDs, hasSize(ids.length));
    }


    protected Engine.IndexResult indexDoc(IndexShard shard, String type, String id) throws IOException {
        return indexDoc(shard, type, id, "{}");
    }

    protected Engine.IndexResult indexDoc(IndexShard shard, String type, String id, String source) throws IOException {
        return indexDoc(shard, type, id, source, XContentType.JSON);
    }

    protected Engine.IndexResult indexDoc(IndexShard shard, String type, String id, String source, XContentType xContentType)
        throws IOException {
        SourceToParse sourceToParse = SourceToParse.source(shard.shardId().getIndexName(), type, id, new BytesArray(source), xContentType);
        if (shard.routingEntry().primary()) {
            return shard.applyIndexOperationOnPrimary(Versions.MATCH_ANY, VersionType.INTERNAL, sourceToParse,
                IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, false, getMappingUpdater(shard, type));
        } else {
            return shard.applyIndexOperationOnReplica(shard.seqNoStats().getMaxSeqNo() + 1, 0,
                VersionType.EXTERNAL, IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, false, sourceToParse, getMappingUpdater(shard, type));
        }
    }

    protected Consumer<Mapping> getMappingUpdater(IndexShard shard, String type) {
        return update -> {
            try {
                updateMappings(shard, IndexMetaData.builder(shard.indexSettings().getIndexMetaData())
                    .putMapping(type, update.toString()).build());
            } catch (IOException e) {
                ExceptionsHelper.reThrowIfNotNull(e);
            }
        };
    }

    protected void updateMappings(IndexShard shard, IndexMetaData indexMetadata) {
        shard.indexSettings().updateIndexMetaData(indexMetadata);
        shard.mapperService().merge(indexMetadata, MapperService.MergeReason.MAPPING_UPDATE, true);
    }

    protected Engine.DeleteResult deleteDoc(IndexShard shard, String type, String id) throws IOException {
        if (shard.routingEntry().primary()) {
            return shard.applyDeleteOperationOnPrimary(Versions.MATCH_ANY, type, id, VersionType.INTERNAL, update -> {});
        } else {
            return shard.applyDeleteOperationOnReplica(shard.seqNoStats().getMaxSeqNo() + 1,
                0L, type, id, VersionType.EXTERNAL, update -> {});
        }
    }

    protected void flushShard(IndexShard shard) {
        flushShard(shard, false);
    }

    protected void flushShard(IndexShard shard, boolean force) {
        shard.flush(new FlushRequest(shard.shardId().getIndexName()).force(force));
    }

    /** Recover a shard from a snapshot using a given repository **/
    protected void recoverShardFromSnapshot(final IndexShard shard,
                                            final Snapshot snapshot,
                                            final Repository repository) throws IOException {
        final Version version = Version.CURRENT;
        final ShardId shardId = shard.shardId();
        final String index = shardId.getIndexName();
        final IndexId indexId = new IndexId(shardId.getIndex().getName(), shardId.getIndex().getUUID());
        final DiscoveryNode node = getFakeDiscoNode(shard.routingEntry().currentNodeId());
        final RecoverySource.SnapshotRecoverySource recoverySource = new RecoverySource.SnapshotRecoverySource(snapshot, version, index);
        final ShardRouting shardRouting = newShardRouting(shardId, node.getId(), true, ShardRoutingState.INITIALIZING, recoverySource);

        shard.markAsRecovering("from snapshot", new RecoveryState(shardRouting, node, null));
        repository.restoreShard(shard, snapshot.getSnapshotId(), version, indexId, shard.shardId(), shard.recoveryState());
    }

    /** Snapshot a shard using a given repository **/
    protected void snapshotShard(final IndexShard shard,
                                 final Snapshot snapshot,
                                 final Repository repository) throws IOException {
        final IndexShardSnapshotStatus snapshotStatus = new IndexShardSnapshotStatus();
        try (Engine.IndexCommitRef indexCommitRef = shard.acquireIndexCommit(true)) {
            Index index = shard.shardId().getIndex();
            IndexId indexId = new IndexId(index.getName(), index.getUUID());

            repository.snapshotShard(shard, snapshot.getSnapshotId(), indexId, indexCommitRef.getIndexCommit(), snapshotStatus);
        }
        assertEquals(IndexShardSnapshotStatus.Stage.DONE, snapshotStatus.stage());
        assertEquals(shard.snapshotStoreMetadata().size(), snapshotStatus.numberOfFiles());
        assertNull(snapshotStatus.failure());
    }

    /**
     * Helper method to access (package-protected) engine from tests
     */
    public static Engine getEngine(IndexShard indexShard) {
        return indexShard.getEngine();
    }
}
