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

import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.TransportIndexAction;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.RestoreSource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingHelper;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.LocalTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.ShardLock;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.fielddata.FieldDataStats;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.flush.FlushStats;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotShardFailure;
import org.elasticsearch.test.DummyShardLock;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.FieldMaskingReader;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.NONE;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_VERSION_CREATED;
import static org.elasticsearch.common.lucene.Lucene.cleanLuceneIndex;
import static org.elasticsearch.common.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

/**
 * Simple unit-test IndexShard related operations.
 */
public class IndexShardTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class);
    }

    public void testWriteShardState() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            ShardId id = new ShardId("foo", "fooUUID", 1);
            long version = between(1, Integer.MAX_VALUE / 2);
            boolean primary = randomBoolean();
            AllocationId allocationId = randomBoolean() ? null : randomAllocationId();
            ShardStateMetaData state1 = new ShardStateMetaData(version, primary, "fooUUID", allocationId);
            write(state1, env.availableShardPaths(id));
            ShardStateMetaData shardStateMetaData = load(logger, env.availableShardPaths(id));
            assertEquals(shardStateMetaData, state1);

            ShardStateMetaData state2 = new ShardStateMetaData(version, primary, "fooUUID", allocationId);
            write(state2, env.availableShardPaths(id));
            shardStateMetaData = load(logger, env.availableShardPaths(id));
            assertEquals(shardStateMetaData, state1);

            ShardStateMetaData state3 = new ShardStateMetaData(version + 1, primary, "fooUUID", allocationId);
            write(state3, env.availableShardPaths(id));
            shardStateMetaData = load(logger, env.availableShardPaths(id));
            assertEquals(shardStateMetaData, state3);
            assertEquals("fooUUID", state3.indexUUID);
        }
    }

    public void testLockTryingToDelete() throws Exception {
        createIndex("test");
        ensureGreen();
        NodeEnvironment env = getInstanceFromNode(NodeEnvironment.class);

        ClusterService cs = getInstanceFromNode(ClusterService.class);
        final Index index = cs.state().metaData().index("test").getIndex();
        Path[] shardPaths = env.availableShardPaths(new ShardId(index, 0));
        logger.info("--> paths: [{}]", (Object)shardPaths);
        // Should not be able to acquire the lock because it's already open
        try {
            NodeEnvironment.acquireFSLockForPaths(IndexSettingsModule.newIndexSettings("test", Settings.EMPTY), shardPaths);
            fail("should not have been able to acquire the lock");
        } catch (LockObtainFailedException e) {
            assertTrue("msg: " + e.getMessage(), e.getMessage().contains("unable to acquire write.lock"));
        }
        // Test without the regular shard lock to assume we can acquire it
        // (worst case, meaning that the shard lock could be acquired and
        // we're green to delete the shard's directory)
        ShardLock sLock = new DummyShardLock(new ShardId(index, 0));
        try {
            env.deleteShardDirectoryUnderLock(sLock, IndexSettingsModule.newIndexSettings("test", Settings.EMPTY));
            fail("should not have been able to delete the directory");
        } catch (LockObtainFailedException e) {
            assertTrue("msg: " + e.getMessage(), e.getMessage().contains("unable to acquire write.lock"));
        }
    }

    public void testPersistenceStateMetadataPersistence() throws Exception {
        createIndex("test");
        ensureGreen();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        NodeEnvironment env = getInstanceFromNode(NodeEnvironment.class);
        IndexService test = indicesService.indexService(resolveIndex("test"));
        IndexShard shard = test.getShardOrNull(0);
        ShardStateMetaData shardStateMetaData = load(logger, env.availableShardPaths(shard.shardId));
        assertEquals(getShardStateMetadata(shard), shardStateMetaData);
        ShardRouting routing = shard.shardRouting;
        shard.updateRoutingEntry(routing);

        shardStateMetaData = load(logger, env.availableShardPaths(shard.shardId));
        assertEquals(shardStateMetaData, getShardStateMetadata(shard));
        assertEquals(shardStateMetaData, new ShardStateMetaData(routing.primary(), shard.indexSettings().getUUID(), routing.allocationId()));

        routing = TestShardRouting.relocate(shard.shardRouting, "some node", 42L);
        shard.updateRoutingEntry(routing);
        shardStateMetaData = load(logger, env.availableShardPaths(shard.shardId));
        assertEquals(shardStateMetaData, getShardStateMetadata(shard));
        assertEquals(shardStateMetaData, new ShardStateMetaData(routing.primary(), shard.indexSettings().getUUID(), routing.allocationId()));
    }

    public void testFailShard() throws Exception {
        createIndex("test");
        ensureGreen();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        NodeEnvironment env = getInstanceFromNode(NodeEnvironment.class);
        IndexService test = indicesService.indexService(resolveIndex("test"));
        IndexShard shard = test.getShardOrNull(0);
        // fail shard
        shard.failShard("test shard fail", new CorruptIndexException("", ""));
        // check state file still exists
        ShardStateMetaData shardStateMetaData = load(logger, env.availableShardPaths(shard.shardId));
        assertEquals(shardStateMetaData, getShardStateMetadata(shard));
        ShardPath shardPath = ShardPath.loadShardPath(logger, env, shard.shardId(), test.getIndexSettings());
        assertNotNull(shardPath);
        // but index can't be opened for a failed shard
        assertThat("store index should be corrupted", Store.canOpenIndex(logger, shardPath.resolveIndex(), shard.shardId(), env::shardLock),
            equalTo(false));
    }

    ShardStateMetaData getShardStateMetadata(IndexShard shard) {
        ShardRouting shardRouting = shard.routingEntry();
        if (shardRouting == null) {
            return null;
        } else {
            return new ShardStateMetaData(shardRouting.primary(), shard.indexSettings().getUUID(), shardRouting.allocationId());
        }
    }

    private AllocationId randomAllocationId() {
        AllocationId allocationId = AllocationId.newInitializing();
        if (randomBoolean()) {
            allocationId = AllocationId.newRelocation(allocationId);
        }
        return allocationId;
    }

    public void testShardStateMetaHashCodeEquals() {
        AllocationId allocationId = randomBoolean() ? null : randomAllocationId();
        ShardStateMetaData meta = new ShardStateMetaData(randomLong(), randomBoolean(), randomRealisticUnicodeOfCodepointLengthBetween(1, 10), allocationId);

        assertEquals(meta, new ShardStateMetaData(meta.legacyVersion, meta.primary, meta.indexUUID, meta.allocationId));
        assertEquals(meta.hashCode(), new ShardStateMetaData(meta.legacyVersion, meta.primary, meta.indexUUID, meta.allocationId).hashCode());

        assertFalse(meta.equals(new ShardStateMetaData(meta.legacyVersion, !meta.primary, meta.indexUUID, meta.allocationId)));
        assertFalse(meta.equals(new ShardStateMetaData(meta.legacyVersion + 1, meta.primary, meta.indexUUID, meta.allocationId)));
        assertFalse(meta.equals(new ShardStateMetaData(meta.legacyVersion, !meta.primary, meta.indexUUID + "foo", meta.allocationId)));
        assertFalse(meta.equals(new ShardStateMetaData(meta.legacyVersion, !meta.primary, meta.indexUUID + "foo", randomAllocationId())));
        Set<Integer> hashCodes = new HashSet<>();
        for (int i = 0; i < 30; i++) { // just a sanity check that we impl hashcode
            allocationId = randomBoolean() ? null : randomAllocationId();
            meta = new ShardStateMetaData(randomLong(), randomBoolean(), randomRealisticUnicodeOfCodepointLengthBetween(1, 10), allocationId);
            hashCodes.add(meta.hashCode());
        }
        assertTrue("more than one unique hashcode expected but got: " + hashCodes.size(), hashCodes.size() > 1);

    }

    public void testDeleteIndexPreventsNewOperations() throws InterruptedException, ExecutionException, IOException {
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0)).get());
        ensureGreen("test");
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexServiceSafe(resolveIndex("test"));
        IndexShard indexShard = indexService.getShardOrNull(0);
        client().admin().indices().prepareDelete("test").get();
        assertThat(indexShard.getActiveOperationsCount(), equalTo(0));
        try {
            indexShard.acquirePrimaryOperationLock(null, ThreadPool.Names.INDEX);
            fail("we should not be able to increment anymore");
        } catch (IndexShardClosedException e) {
            // expected
        }
        try {
            indexShard.acquireReplicaOperationLock(indexShard.getPrimaryTerm(), null, ThreadPool.Names.INDEX);
            fail("we should not be able to increment anymore");
        } catch (IndexShardClosedException e) {
            // expected
        }
    }

    public void testOperationLocksOnPrimaryShards() throws InterruptedException, ExecutionException, IOException {
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0)).get());
        ensureGreen("test");
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexServiceSafe(resolveIndex("test"));
        IndexShard indexShard = indexService.getShardOrNull(0);
        long primaryTerm = indexShard.getPrimaryTerm();

        ShardRouting temp = indexShard.routingEntry();
        final ShardRouting newPrimaryShardRouting;
        if (randomBoolean()) {
            // relocation target
            newPrimaryShardRouting = TestShardRouting.newShardRouting(temp.shardId(), temp.currentNodeId(), "other node",
                true, ShardRoutingState.INITIALIZING, AllocationId.newRelocation(temp.allocationId()));
        } else if (randomBoolean()) {
            // simulate promotion
            ShardRouting newReplicaShardRouting = TestShardRouting.newShardRouting(temp.shardId(), temp.currentNodeId(), null,
                false, ShardRoutingState.STARTED, temp.allocationId());
            indexShard.updateRoutingEntry(newReplicaShardRouting);
            primaryTerm = primaryTerm + 1;
            indexShard.updatePrimaryTerm(primaryTerm);
            newPrimaryShardRouting = TestShardRouting.newShardRouting(temp.shardId(), temp.currentNodeId(), null,
                true, ShardRoutingState.STARTED, temp.allocationId());
        } else {
            newPrimaryShardRouting = temp;
        }
        indexShard.updateRoutingEntry(newPrimaryShardRouting);

        assertEquals(0, indexShard.getActiveOperationsCount());
        if (newPrimaryShardRouting.isRelocationTarget() == false) {
            try {
                indexShard.acquireReplicaOperationLock(primaryTerm, null, ThreadPool.Names.INDEX);
                fail("shard shouldn't accept operations as replica");
            } catch (IllegalStateException ignored) {

            }
        }
        Releasable operation1 = acquirePrimaryOperationLockBlockingly(indexShard);
        assertEquals(1, indexShard.getActiveOperationsCount());
        Releasable operation2 = acquirePrimaryOperationLockBlockingly(indexShard);
        assertEquals(2, indexShard.getActiveOperationsCount());

        Releasables.close(operation1, operation2);
        assertEquals(0, indexShard.getActiveOperationsCount());
    }

    private Releasable acquirePrimaryOperationLockBlockingly(IndexShard indexShard) throws ExecutionException, InterruptedException {
        PlainActionFuture<Releasable> fut = new PlainActionFuture<>();
        indexShard.acquirePrimaryOperationLock(fut, ThreadPool.Names.INDEX);
        return fut.get();
    }

    private Releasable acquireReplicaOperationLockBlockingly(IndexShard indexShard, long opPrimaryTerm) throws ExecutionException, InterruptedException {
        PlainActionFuture<Releasable> fut = new PlainActionFuture<>();
        indexShard.acquireReplicaOperationLock(opPrimaryTerm, fut, ThreadPool.Names.INDEX);
        return fut.get();
    }

    public void testOperationLocksOnReplicaShards() throws InterruptedException, ExecutionException, IOException {
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0)).get());
        ensureGreen("test");
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexServiceSafe(resolveIndex("test"));
        IndexShard indexShard = indexService.getShardOrNull(0);
        long primaryTerm = indexShard.getPrimaryTerm();

        // ugly hack to allow the shard to operated as a replica
        final ShardRouting temp = indexShard.routingEntry();
        final ShardRouting newShardRouting;
        switch (randomInt(2)) {
            case 0:
                // started replica
                newShardRouting = TestShardRouting.newShardRouting(temp.shardId(), temp.currentNodeId(), null,
                    false, ShardRoutingState.STARTED, AllocationId.newRelocation(temp.allocationId()));

                indexShard.updateRoutingEntry(newShardRouting);
                break;
            case 1:
                // initializing replica / primary
                final boolean relocating = randomBoolean();
                newShardRouting = TestShardRouting.newShardRouting(temp.shardId(), temp.currentNodeId(),
                    relocating ? "sourceNode" : null,
                    relocating ? randomBoolean() : false,
                    ShardRoutingState.INITIALIZING,
                    relocating ? AllocationId.newRelocation(temp.allocationId()) : temp.allocationId());
                indexShard.updateRoutingEntry(newShardRouting);
                break;
            case 2:
                // relocation source
                newShardRouting = TestShardRouting.newShardRouting(temp.shardId(), temp.currentNodeId(), "otherNode",
                    false, ShardRoutingState.RELOCATING, AllocationId.newRelocation(temp.allocationId()));
                indexShard.updateRoutingEntry(newShardRouting);
                indexShard.relocated("test");
                break;
            default:
                throw new UnsupportedOperationException("get your numbers straight");

        }
        logger.info("updated shard routing to {}", newShardRouting);

        assertEquals(0, indexShard.getActiveOperationsCount());
        if (newShardRouting.primary() == false) {
            try {
                indexShard.acquirePrimaryOperationLock(null, ThreadPool.Names.INDEX);
                fail("shard shouldn't accept primary ops");
            } catch (IllegalStateException ignored) {

            }
        }

        Releasable operation1 = acquireReplicaOperationLockBlockingly(indexShard, primaryTerm);
        assertEquals(1, indexShard.getActiveOperationsCount());
        Releasable operation2 = acquireReplicaOperationLockBlockingly(indexShard, primaryTerm);
        assertEquals(2, indexShard.getActiveOperationsCount());

        try {
            indexShard.acquireReplicaOperationLock(primaryTerm - 1, null, ThreadPool.Names.INDEX);
            fail("you can not increment the operation counter with an older primary term");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("operation term"));
            assertThat(e.getMessage(), containsString("too old"));
        }

        // but you can increment with a newer one..
        acquireReplicaOperationLockBlockingly(indexShard, primaryTerm + 1 + randomInt(20)).close();
        Releasables.close(operation1, operation2);
        assertEquals(0, indexShard.getActiveOperationsCount());
    }

    public void testMarkAsInactiveTriggersSyncedFlush() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
            .setSettings(SETTING_NUMBER_OF_SHARDS, 1, SETTING_NUMBER_OF_REPLICAS, 0));
        client().prepareIndex("test", "test").setSource("{}").get();
        ensureGreen("test");
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        indicesService.indexService(resolveIndex("test")).getShardOrNull(0).checkIdle(0);
        assertBusy(() -> {
            IndexStats indexStats = client().admin().indices().prepareStats("test").clear().get().getIndex("test");
            assertNotNull(indexStats.getShards()[0].getCommitStats().getUserData().get(Engine.SYNC_COMMIT_ID));
            indicesService.indexService(resolveIndex("test")).getShardOrNull(0).checkIdle(0);
            }
        );
        IndexStats indexStats = client().admin().indices().prepareStats("test").get().getIndex("test");
        assertNotNull(indexStats.getShards()[0].getCommitStats().getUserData().get(Engine.SYNC_COMMIT_ID));
    }

    public static ShardStateMetaData load(ESLogger logger, Path... shardPaths) throws IOException {
        return ShardStateMetaData.FORMAT.loadLatestState(logger, shardPaths);
    }

    public static void write(ShardStateMetaData shardStateMetaData,
                             Path... shardPaths) throws IOException {
        ShardStateMetaData.FORMAT.write(shardStateMetaData, shardPaths);
    }

    public void testAcquireIndexCommit() throws IOException {
        createIndex("test");
        ensureGreen();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService test = indicesService.indexService(resolveIndex("test"));
        final IndexShard shard = test.getShardOrNull(0);
        int numDocs = randomInt(20);
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex("test", "type", "id_" + i).setSource("{}").get();
        }
        final boolean flushFirst = randomBoolean();
        IndexCommit commit = shard.acquireIndexCommit(flushFirst);
        int moreDocs = randomInt(20);
        for (int i = 0; i < moreDocs; i++) {
            client().prepareIndex("test", "type", "id_" + numDocs + i).setSource("{}").get();
        }
        shard.flush(new FlushRequest("index"));
        // check that we can still read the commit that we captured
        try (IndexReader reader = DirectoryReader.open(commit)) {
            assertThat(reader.numDocs(), equalTo(flushFirst ? numDocs : 0));
        }
        shard.releaseIndexCommit(commit);
        shard.flush(new FlushRequest("index").force(true));
        // check it's clean up
        assertThat(DirectoryReader.listCommits(shard.store().directory()), hasSize(1));
    }

    /***
     * test one can snapshot the store at various lifecycle stages
     */
    public void testSnapshotStore() throws IOException {
        createIndex("test");
        ensureGreen();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService test = indicesService.indexService(resolveIndex("test"));
        final IndexShard shard = test.getShardOrNull(0);
        client().prepareIndex("test", "test", "0").setSource("{}").setRefreshPolicy(randomBoolean() ? IMMEDIATE : NONE).get();
        client().admin().indices().prepareFlush().get();
        ShardRouting routing = shard.routingEntry();
        test.removeShard(0, "b/c simon says so");
        routing = ShardRoutingHelper.reinit(routing);
        IndexShard newShard = test.createShard(routing);
        newShard.updateRoutingEntry(routing);
        DiscoveryNode localNode = new DiscoveryNode("foo", LocalTransportAddress.buildUnique(), emptyMap(), emptySet(), Version.CURRENT);

        Store.MetadataSnapshot snapshot = newShard.snapshotStoreMetadata();
        assertThat(snapshot.getSegmentsFile().name(), equalTo("segments_2"));

        newShard.markAsRecovering("store", new RecoveryState(newShard.shardId(), routing.primary(), RecoveryState.Type.STORE, localNode,
            localNode));

        snapshot = newShard.snapshotStoreMetadata();
        assertThat(snapshot.getSegmentsFile().name(), equalTo("segments_2"));

        assertTrue(newShard.recoverFromStore());

        snapshot = newShard.snapshotStoreMetadata();
        assertThat(snapshot.getSegmentsFile().name(), equalTo("segments_2"));

        newShard.updateRoutingEntry(getInitializingShardRouting(routing).moveToStarted());

        snapshot = newShard.snapshotStoreMetadata();
        assertThat(snapshot.getSegmentsFile().name(), equalTo("segments_2"));

        newShard.close("test", false);

        snapshot = newShard.snapshotStoreMetadata();
        assertThat(snapshot.getSegmentsFile().name(), equalTo("segments_2"));
    }

    public void testDurableFlagHasEffect() {
        createIndex("test");
        ensureGreen();
        client().prepareIndex("test", "bar", "1").setSource("{}").get();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService test = indicesService.indexService(resolveIndex("test"));
        IndexShard shard = test.getShardOrNull(0);
        setDurability(shard, Translog.Durability.REQUEST);
        assertFalse(shard.getEngine().getTranslog().syncNeeded());
        setDurability(shard, Translog.Durability.ASYNC);
        client().prepareIndex("test", "bar", "2").setSource("{}").get();
        assertTrue(shard.getEngine().getTranslog().syncNeeded());
        setDurability(shard, Translog.Durability.REQUEST);
        client().prepareDelete("test", "bar", "1").get();
        assertFalse(shard.getEngine().getTranslog().syncNeeded());

        setDurability(shard, Translog.Durability.ASYNC);
        client().prepareDelete("test", "bar", "2").get();
        assertTrue(shard.getEngine().getTranslog().syncNeeded());
        setDurability(shard, Translog.Durability.REQUEST);
        assertNoFailures(client().prepareBulk()
            .add(client().prepareIndex("test", "bar", "3").setSource("{}"))
            .add(client().prepareDelete("test", "bar", "1")).get());
        assertFalse(shard.getEngine().getTranslog().syncNeeded());

        setDurability(shard, Translog.Durability.ASYNC);
        assertNoFailures(client().prepareBulk()
            .add(client().prepareIndex("test", "bar", "4").setSource("{}"))
            .add(client().prepareDelete("test", "bar", "3")).get());
        setDurability(shard, Translog.Durability.REQUEST);
        assertTrue(shard.getEngine().getTranslog().syncNeeded());
    }

    private void setDurability(IndexShard shard, Translog.Durability durability) {
        client().admin().indices().prepareUpdateSettings(shard.shardId.getIndexName()).setSettings(Settings.builder().put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), durability.name()).build()).get();
        assertEquals(durability, shard.getTranslogDurability());
    }

    public void testMinimumCompatVersion() {
        Version versionCreated = VersionUtils.randomVersion(random());
        assertAcked(client().admin().indices().prepareCreate("test")
            .setSettings(SETTING_NUMBER_OF_SHARDS, 1, SETTING_NUMBER_OF_REPLICAS, 0, SETTING_VERSION_CREATED, versionCreated.id));
        client().prepareIndex("test", "test").setSource("{}").get();
        ensureGreen("test");
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexShard test = indicesService.indexService(resolveIndex("test")).getShardOrNull(0);
        assertEquals(versionCreated.luceneVersion, test.minimumCompatibleVersion());
        client().prepareIndex("test", "test").setSource("{}").get();
        assertEquals(versionCreated.luceneVersion, test.minimumCompatibleVersion());
        test.getEngine().flush();
        assertEquals(Version.CURRENT.luceneVersion, test.minimumCompatibleVersion());
    }

    public void testUpdatePriority() {
        assertAcked(client().admin().indices().prepareCreate("test")
            .setSettings(IndexMetaData.SETTING_PRIORITY, 200));
        IndexService indexService = getInstanceFromNode(IndicesService.class).indexService(resolveIndex("test"));
        assertEquals(200, indexService.getIndexSettings().getSettings().getAsInt(IndexMetaData.SETTING_PRIORITY, 0).intValue());
        client().admin().indices().prepareUpdateSettings("test").setSettings(Settings.builder().put(IndexMetaData.SETTING_PRIORITY, 400).build()).get();
        assertEquals(400, indexService.getIndexSettings().getSettings().getAsInt(IndexMetaData.SETTING_PRIORITY, 0).intValue());
    }

    public void testRecoverIntoLeftover() throws IOException {
        createIndex("test");
        ensureGreen("test");
        client().prepareIndex("test", "bar", "1").setSource("{}").setRefreshPolicy(IMMEDIATE).get();
        client().admin().indices().prepareFlush("test").get();
        SearchResponse response = client().prepareSearch("test").get();
        assertHitCount(response, 1L);
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService test = indicesService.indexService(resolveIndex("test"));
        IndexShard shard = test.getShardOrNull(0);
        ShardPath shardPath = shard.shardPath();
        Path dataPath = shardPath.getDataPath();
        client().admin().indices().prepareClose("test").get();
        Path tempDir = createTempDir();
        Files.move(dataPath, tempDir.resolve("test"));
        client().admin().indices().prepareDelete("test").get();
        Files.createDirectories(dataPath.getParent());
        Files.move(tempDir.resolve("test"), dataPath);
        createIndex("test");
        ensureGreen("test");
        response = client().prepareSearch("test").get();
        assertHitCount(response, 0L);
    }

    public void testIndexDirIsDeletedWhenShardRemoved() throws Exception {
        Environment env = getInstanceFromNode(Environment.class);
        Path idxPath = env.sharedDataFile().resolve(randomAsciiOfLength(10));
        logger.info("--> idxPath: [{}]", idxPath);
        Settings idxSettings = Settings.builder()
            .put(IndexMetaData.SETTING_DATA_PATH, idxPath)
            .build();
        createIndex("test", idxSettings);
        ensureGreen("test");
        client().prepareIndex("test", "bar", "1").setSource("{}").setRefreshPolicy(IMMEDIATE).get();
        SearchResponse response = client().prepareSearch("test").get();
        assertHitCount(response, 1L);
        client().admin().indices().prepareDelete("test").get();
        assertAllIndicesRemovedAndDeletionCompleted(Collections.singleton(getInstanceFromNode(IndicesService.class)));
        assertPathHasBeenCleared(idxPath);
    }

    public void testExpectedShardSizeIsPresent() throws InterruptedException {
        assertAcked(client().admin().indices().prepareCreate("test")
            .setSettings(SETTING_NUMBER_OF_SHARDS, 1, SETTING_NUMBER_OF_REPLICAS, 0));
        for (int i = 0; i < 50; i++) {
            client().prepareIndex("test", "test").setSource("{}").get();
        }
        ensureGreen("test");
        InternalClusterInfoService clusterInfoService = (InternalClusterInfoService) getInstanceFromNode(ClusterInfoService.class);
        clusterInfoService.refresh();
        ClusterState state = getInstanceFromNode(ClusterService.class).state();
        Long test = clusterInfoService.getClusterInfo().getShardSize(state.getRoutingTable().index("test").getShards().get(0).primaryShard());
        assertNotNull(test);
        assertTrue(test > 0);
    }

    public void testIndexCanChangeCustomDataPath() throws Exception {
        Environment env = getInstanceFromNode(Environment.class);
        Path idxPath = env.sharedDataFile().resolve(randomAsciiOfLength(10));
        final String INDEX = "idx";
        Path startDir = idxPath.resolve("start-" + randomAsciiOfLength(10));
        Path endDir = idxPath.resolve("end-" + randomAsciiOfLength(10));
        logger.info("--> start dir: [{}]", startDir.toAbsolutePath().toString());
        logger.info("-->   end dir: [{}]", endDir.toAbsolutePath().toString());
        // temp dirs are automatically created, but the end dir is what
        // startDir is going to be renamed as, so it needs to be deleted
        // otherwise we get all sorts of errors about the directory
        // already existing
        IOUtils.rm(endDir);

        Settings sb = Settings.builder()
            .put(IndexMetaData.SETTING_DATA_PATH, startDir.toAbsolutePath().toString())
            .build();
        Settings sb2 = Settings.builder()
            .put(IndexMetaData.SETTING_DATA_PATH, endDir.toAbsolutePath().toString())
            .build();

        logger.info("--> creating an index with data_path [{}]", startDir.toAbsolutePath().toString());
        createIndex(INDEX, sb);
        ensureGreen(INDEX);
        client().prepareIndex(INDEX, "bar", "1").setSource("{}").setRefreshPolicy(IMMEDIATE).get();

        SearchResponse resp = client().prepareSearch(INDEX).setQuery(matchAllQuery()).get();
        assertThat("found the hit", resp.getHits().getTotalHits(), equalTo(1L));

        logger.info("--> closing the index [{}]", INDEX);
        client().admin().indices().prepareClose(INDEX).get();
        logger.info("--> index closed, re-opening...");
        client().admin().indices().prepareOpen(INDEX).get();
        logger.info("--> index re-opened");
        ensureGreen(INDEX);

        resp = client().prepareSearch(INDEX).setQuery(matchAllQuery()).get();
        assertThat("found the hit", resp.getHits().getTotalHits(), equalTo(1L));

        // Now, try closing and changing the settings

        logger.info("--> closing the index [{}]", INDEX);
        client().admin().indices().prepareClose(INDEX).get();

        logger.info("--> moving data on disk [{}] to [{}]", startDir.getFileName(), endDir.getFileName());
        assert Files.exists(endDir) == false : "end directory should not exist!";
        Files.move(startDir, endDir, StandardCopyOption.REPLACE_EXISTING);

        logger.info("--> updating settings...");
        client().admin().indices().prepareUpdateSettings(INDEX)
            .setSettings(sb2)
            .setIndicesOptions(IndicesOptions.fromOptions(true, false, true, true))
            .get();

        assert Files.exists(startDir) == false : "start dir shouldn't exist";

        logger.info("--> settings updated and files moved, re-opening index");
        client().admin().indices().prepareOpen(INDEX).get();
        logger.info("--> index re-opened");
        ensureGreen(INDEX);

        resp = client().prepareSearch(INDEX).setQuery(matchAllQuery()).get();
        assertThat("found the hit", resp.getHits().getTotalHits(), equalTo(1L));

        assertAcked(client().admin().indices().prepareDelete(INDEX));
        assertAllIndicesRemovedAndDeletionCompleted(Collections.singleton(getInstanceFromNode(IndicesService.class)));
        assertPathHasBeenCleared(startDir.toAbsolutePath());
        assertPathHasBeenCleared(endDir.toAbsolutePath());
    }

    public void testShardStats() throws IOException {
        createIndex("test");
        ensureGreen();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService test = indicesService.indexService(resolveIndex("test"));
        IndexShard shard = test.getShardOrNull(0);
        ShardStats stats = new ShardStats(shard.routingEntry(), shard.shardPath(), new CommonStats(indicesService.getIndicesQueryCache(), shard, new CommonStatsFlags()), shard.commitStats());
        assertEquals(shard.shardPath().getRootDataPath().toString(), stats.getDataPath());
        assertEquals(shard.shardPath().getRootStatePath().toString(), stats.getStatePath());
        assertEquals(shard.shardPath().isCustomDataPath(), stats.isCustomDataPath());

        if (randomBoolean() || true) { // try to serialize it to ensure values survive the serialization
            BytesStreamOutput out = new BytesStreamOutput();
            stats.writeTo(out);
            StreamInput in = out.bytes().streamInput();
            stats = ShardStats.readShardStats(in);
        }
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        stats.toXContent(builder, EMPTY_PARAMS);
        builder.endObject();
        String xContent = builder.string();
        StringBuilder expectedSubSequence = new StringBuilder("\"shard_path\":{\"state_path\":\"");
        expectedSubSequence.append(shard.shardPath().getRootStatePath().toString());
        expectedSubSequence.append("\",\"data_path\":\"");
        expectedSubSequence.append(shard.shardPath().getRootDataPath().toString());
        expectedSubSequence.append("\",\"is_custom_data_path\":").append(shard.shardPath().isCustomDataPath()).append("}");
        assumeFalse("Some path weirdness on windows", Constants.WINDOWS);
        assertTrue(xContent.contains(expectedSubSequence));
    }

    private ParsedDocument testParsedDocument(String uid, String id, String type, String routing, long timestamp, long ttl, ParseContext.Document document, BytesReference source, Mapping mappingUpdate) {
        Field uidField = new Field("_uid", uid, UidFieldMapper.Defaults.FIELD_TYPE);
        Field versionField = new NumericDocValuesField("_version", 0);
        document.add(uidField);
        document.add(versionField);
        return new ParsedDocument(versionField, id, type, routing, timestamp, ttl, Arrays.asList(document), source, mappingUpdate);
    }

    public void testIndexingOperationsListeners() throws IOException {
        createIndex("test_iol");
        ensureGreen();
        client().prepareIndex("test_iol", "test", "0").setSource("{\"foo\" : \"bar\"}").setRefreshPolicy(IMMEDIATE).get();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService test = indicesService.indexService(resolveIndex("test_iol"));
        IndexShard shard = test.getShardOrNull(0);
        AtomicInteger preIndex = new AtomicInteger();
        AtomicInteger postIndexCreate = new AtomicInteger();
        AtomicInteger postIndexUpdate = new AtomicInteger();
        AtomicInteger postIndexException = new AtomicInteger();
        AtomicInteger preDelete = new AtomicInteger();
        AtomicInteger postDelete = new AtomicInteger();
        AtomicInteger postDeleteException = new AtomicInteger();
        shard.close("simon says", true);
        shard = reinitWithWrapper(test, shard, null, new IndexingOperationListener() {
            @Override
            public Engine.Index preIndex(Engine.Index operation) {
                preIndex.incrementAndGet();
                return operation;
            }

            @Override
            public void postIndex(Engine.Index index, boolean created) {
                if(created) {
                    postIndexCreate.incrementAndGet();
                } else {
                    postIndexUpdate.incrementAndGet();
                }
            }

            @Override
            public void postIndex(Engine.Index index, Exception ex) {
                postIndexException.incrementAndGet();
            }

            @Override
            public Engine.Delete preDelete(Engine.Delete delete) {
                preDelete.incrementAndGet();
                return delete;
            }

            @Override
            public void postDelete(Engine.Delete delete) {
                postDelete.incrementAndGet();
            }

            @Override
            public void postDelete(Engine.Delete delete, Exception ex) {
                postDeleteException.incrementAndGet();

            }
        });

        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, new ParseContext.Document(), new BytesArray(new byte[]{1}), null);
        Engine.Index index = new Engine.Index(new Term("_uid", "1"), doc);
        shard.index(index);
        assertEquals(1, preIndex.get());
        assertEquals(1, postIndexCreate.get());
        assertEquals(0, postIndexUpdate.get());
        assertEquals(0, postIndexException.get());
        assertEquals(0, preDelete.get());
        assertEquals(0, postDelete.get());
        assertEquals(0, postDeleteException.get());

        shard.index(index);
        assertEquals(2, preIndex.get());
        assertEquals(1, postIndexCreate.get());
        assertEquals(1, postIndexUpdate.get());
        assertEquals(0, postIndexException.get());
        assertEquals(0, preDelete.get());
        assertEquals(0, postDelete.get());
        assertEquals(0, postDeleteException.get());

        Engine.Delete delete = new Engine.Delete("test", "1", new Term("_uid", "1"));
        shard.delete(delete);

        assertEquals(2, preIndex.get());
        assertEquals(1, postIndexCreate.get());
        assertEquals(1, postIndexUpdate.get());
        assertEquals(0, postIndexException.get());
        assertEquals(1, preDelete.get());
        assertEquals(1, postDelete.get());
        assertEquals(0, postDeleteException.get());

        shard.close("Unexpected close", true);
        shard.state = IndexShardState.STARTED; // It will generate exception

        try {
            shard.index(index);
            fail();
        } catch (IllegalIndexShardStateException e) {

        }

        assertEquals(2, preIndex.get());
        assertEquals(1, postIndexCreate.get());
        assertEquals(1, postIndexUpdate.get());
        assertEquals(0, postIndexException.get());
        assertEquals(1, preDelete.get());
        assertEquals(1, postDelete.get());
        assertEquals(0, postDeleteException.get());
        try {
            shard.delete(delete);
            fail();
        } catch (IllegalIndexShardStateException e) {

        }

        assertEquals(2, preIndex.get());
        assertEquals(1, postIndexCreate.get());
        assertEquals(1, postIndexUpdate.get());
        assertEquals(0, postIndexException.get());
        assertEquals(1, preDelete.get());
        assertEquals(1, postDelete.get());
        assertEquals(0, postDeleteException.get());
    }

    public void testMaybeFlush() throws Exception {
        createIndex("test", Settings.builder().put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.REQUEST).build());
        ensureGreen();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService test = indicesService.indexService(resolveIndex("test"));
        IndexShard shard = test.getShardOrNull(0);
        assertFalse(shard.shouldFlush());
        client().admin().indices().prepareUpdateSettings("test").setSettings(Settings.builder().put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), new ByteSizeValue(133 /* size of the operation + header&footer*/, ByteSizeUnit.BYTES)).build()).get();
        client().prepareIndex("test", "test", "0").setSource("{}").setRefreshPolicy(randomBoolean() ? IMMEDIATE : NONE).get();
        assertFalse(shard.shouldFlush());
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, new ParseContext.Document(), new BytesArray(new byte[]{1}), null);
        Engine.Index index = new Engine.Index(new Term("_uid", "1"), doc);
        shard.index(index);
        assertTrue(shard.shouldFlush());
        assertEquals(2, shard.getEngine().getTranslog().totalOperations());
        client().prepareIndex("test", "test", "2").setSource("{}").setRefreshPolicy(randomBoolean() ? IMMEDIATE : NONE).get();
        assertBusy(() -> { // this is async
            assertFalse(shard.shouldFlush());
        });
        assertEquals(0, shard.getEngine().getTranslog().totalOperations());
        shard.getEngine().getTranslog().sync();
        long size = shard.getEngine().getTranslog().sizeInBytes();
        logger.info("--> current translog size: [{}] num_ops [{}] generation [{}]", shard.getEngine().getTranslog().sizeInBytes(), shard.getEngine().getTranslog().totalOperations(), shard.getEngine().getTranslog().getGeneration());
        client().admin().indices().prepareUpdateSettings("test").setSettings(Settings.builder().put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), new ByteSizeValue(size, ByteSizeUnit.BYTES))
            .build()).get();
        client().prepareDelete("test", "test", "2").get();
        logger.info("--> translog size after delete: [{}] num_ops [{}] generation [{}]", shard.getEngine().getTranslog().sizeInBytes(), shard.getEngine().getTranslog().totalOperations(), shard.getEngine().getTranslog().getGeneration());
        assertBusy(() -> { // this is async
            logger.info("--> translog size on iter  : [{}] num_ops [{}] generation [{}]", shard.getEngine().getTranslog().sizeInBytes(), shard.getEngine().getTranslog().totalOperations(), shard.getEngine().getTranslog().getGeneration());
            assertFalse(shard.shouldFlush());
        });
        assertEquals(0, shard.getEngine().getTranslog().totalOperations());
    }

    public void testStressMaybeFlush() throws Exception {
        createIndex("test");
        ensureGreen();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService test = indicesService.indexService(resolveIndex("test"));
        final IndexShard shard = test.getShardOrNull(0);
        assertFalse(shard.shouldFlush());
        client().admin().indices().prepareUpdateSettings("test").setSettings(Settings.builder().put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), new ByteSizeValue(133/* size of the operation + header&footer*/, ByteSizeUnit.BYTES)).build()).get();
        client().prepareIndex("test", "test", "0").setSource("{}").setRefreshPolicy(randomBoolean() ? IMMEDIATE : NONE).get();
        assertFalse(shard.shouldFlush());
        final AtomicBoolean running = new AtomicBoolean(true);
        final int numThreads = randomIntBetween(2, 4);
        Thread[] threads = new Thread[numThreads];
        CyclicBarrier barrier = new CyclicBarrier(numThreads + 1);
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread() {
                @Override
                public void run() {
                    try {
                        barrier.await();
                    } catch (InterruptedException | BrokenBarrierException e) {
                        throw new RuntimeException(e);
                    }
                    while (running.get()) {
                        shard.maybeFlush();
                    }
                }
            };
            threads[i].start();
        }
        barrier.await();
        FlushStats flushStats = shard.flushStats();
        long total = flushStats.getTotal();
        client().prepareIndex("test", "test", "1").setSource("{}").get();
        assertBusy(() -> {
            assertEquals(total + 1, shard.flushStats().getTotal());
        });
        running.set(false);
        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
        }
        assertEquals(total + 1, shard.flushStats().getTotal());
    }

    public void testLockingBeforeAndAfterRelocated() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(
            Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0)
        ).get());
        ensureGreen();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService test = indicesService.indexService(resolveIndex("test"));
        final IndexShard shard = test.getShardOrNull(0);
        assertBusy(() -> assertThat(shard.state(), equalTo(IndexShardState.STARTED)));
        CountDownLatch latch = new CountDownLatch(1);
        Thread recoveryThread = new Thread(() -> {
            latch.countDown();
            try {
                shard.relocated("simulated recovery");
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        try (Releasable ignored = acquirePrimaryOperationLockBlockingly(shard)) {
            // start finalization of recovery
            recoveryThread.start();
            latch.await();
            // recovery can only be finalized after we release the current primaryOperationLock
            assertThat(shard.state(), equalTo(IndexShardState.STARTED));
        }
        // recovery can be now finalized
        recoveryThread.join();
        assertThat(shard.state(), equalTo(IndexShardState.RELOCATED));
        try (Releasable ignored = acquirePrimaryOperationLockBlockingly(shard)) {
            // lock can again be acquired
            assertThat(shard.state(), equalTo(IndexShardState.RELOCATED));
        }
    }

    public void testDelayedOperationsBeforeAndAfterRelocated() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(
            Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0)
        ).get());
        ensureGreen();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService test = indicesService.indexService(resolveIndex("test"));
        final IndexShard shard = test.getShardOrNull(0);
        assertBusy(() -> assertThat(shard.state(), equalTo(IndexShardState.STARTED)));
        Thread recoveryThread = new Thread(() -> {
            try {
                shard.relocated("simulated recovery");
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        recoveryThread.start();
        List<PlainActionFuture<Releasable>> onLockAcquiredActions = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            PlainActionFuture<Releasable> onLockAcquired = new PlainActionFuture<Releasable>() {
                @Override
                public void onResponse(Releasable releasable) {
                    releasable.close();
                    super.onResponse(releasable);
                }
            };
            shard.acquirePrimaryOperationLock(onLockAcquired, ThreadPool.Names.INDEX);
            onLockAcquiredActions.add(onLockAcquired);
        }

        for (PlainActionFuture<Releasable> onLockAcquired : onLockAcquiredActions) {
            assertNotNull(onLockAcquired.get(30, TimeUnit.SECONDS));
        }

        recoveryThread.join();
    }

    public void testStressRelocated() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(
            Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0)
        ).get());
        ensureGreen();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService test = indicesService.indexService(resolveIndex("test"));
        final IndexShard shard = test.getShardOrNull(0);
        final int numThreads = randomIntBetween(2, 4);
        Thread[] indexThreads = new Thread[numThreads];
        CountDownLatch allPrimaryOperationLocksAcquired = new CountDownLatch(numThreads);
        CyclicBarrier barrier = new CyclicBarrier(numThreads + 1);
        for (int i = 0; i < indexThreads.length; i++) {
            indexThreads[i] = new Thread() {
                @Override
                public void run() {
                    try (Releasable operationLock = acquirePrimaryOperationLockBlockingly(shard)) {
                        allPrimaryOperationLocksAcquired.countDown();
                        barrier.await();
                    } catch (InterruptedException | BrokenBarrierException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                }
            };
            indexThreads[i].start();
        }
        AtomicBoolean relocated = new AtomicBoolean();
        final Thread recoveryThread = new Thread(() -> {
            try {
                shard.relocated("simulated recovery");
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            relocated.set(true);
        });
        // ensure we wait for all primary operation locks to be acquired
        allPrimaryOperationLocksAcquired.await();
        // start recovery thread
        recoveryThread.start();
        assertThat(relocated.get(), equalTo(false));
        assertThat(shard.getActiveOperationsCount(), greaterThan(0));
        // ensure we only transition to RELOCATED state after pending operations completed
        assertThat(shard.state(), equalTo(IndexShardState.STARTED));
        // complete pending operations
        barrier.await();
        // complete recovery/relocation
        recoveryThread.join();
        // ensure relocated successfully once pending operations are done
        assertThat(relocated.get(), equalTo(true));
        assertThat(shard.state(), equalTo(IndexShardState.RELOCATED));
        assertThat(shard.getActiveOperationsCount(), equalTo(0));

        for (Thread indexThread : indexThreads) {
            indexThread.join();
        }
    }

    public void testRecoverFromStore() throws IOException {
        createIndex("test");
        ensureGreen();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService test = indicesService.indexService(resolveIndex("test"));
        final IndexShard shard = test.getShardOrNull(0);
        int translogOps = 1;
        client().prepareIndex("test", "test", "0").setSource("{}").setRefreshPolicy(randomBoolean() ? IMMEDIATE : NONE).get();
        if (randomBoolean()) {
            client().admin().indices().prepareFlush().get();
            translogOps = 0;
        }
        ShardRouting routing = shard.routingEntry();
        test.removeShard(0, "b/c simon says so");
        routing = ShardRoutingHelper.reinit(routing);
        IndexShard newShard = test.createShard(routing);
        newShard.updateRoutingEntry(routing);
        DiscoveryNode localNode = new DiscoveryNode("foo", LocalTransportAddress.buildUnique(), emptyMap(), emptySet(), Version.CURRENT);
        newShard.markAsRecovering("store", new RecoveryState(newShard.shardId(), routing.primary(), RecoveryState.Type.STORE, localNode, localNode));
        assertTrue(newShard.recoverFromStore());
        assertEquals(translogOps, newShard.recoveryState().getTranslog().recoveredOperations());
        assertEquals(translogOps, newShard.recoveryState().getTranslog().totalOperations());
        assertEquals(translogOps, newShard.recoveryState().getTranslog().totalOperationsOnStart());
        assertEquals(100.0f, newShard.recoveryState().getTranslog().recoveredPercent(), 0.01f);
        newShard.updateRoutingEntry(routing.moveToStarted());
        SearchResponse response = client().prepareSearch().get();
        assertHitCount(response, 1);
    }

    public void testRecoverFromCleanStore() throws IOException {
        createIndex("test");
        ensureGreen();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService test = indicesService.indexService(resolveIndex("test"));
        final IndexShard shard = test.getShardOrNull(0);
        client().prepareIndex("test", "test", "0").setSource("{}").setRefreshPolicy(randomBoolean() ? IMMEDIATE : NONE).get();
        if (randomBoolean()) {
            client().admin().indices().prepareFlush().get();
        }
        ShardRouting routing = shard.routingEntry();
        test.removeShard(0, "b/c simon says so");
        routing = ShardRoutingHelper.reinit(routing, UnassignedInfo.Reason.INDEX_CREATED);
        IndexShard newShard = test.createShard(routing);
        newShard.updateRoutingEntry(routing);
        DiscoveryNode localNode = new DiscoveryNode("foo", LocalTransportAddress.buildUnique(), emptyMap(), emptySet(), Version.CURRENT);
        newShard.markAsRecovering("store", new RecoveryState(newShard.shardId(), routing.primary(), RecoveryState.Type.STORE, localNode,
            localNode));
        assertTrue(newShard.recoverFromStore());
        assertEquals(0, newShard.recoveryState().getTranslog().recoveredOperations());
        assertEquals(0, newShard.recoveryState().getTranslog().totalOperations());
        assertEquals(0, newShard.recoveryState().getTranslog().totalOperationsOnStart());
        assertEquals(100.0f, newShard.recoveryState().getTranslog().recoveredPercent(), 0.01f);
        newShard.updateRoutingEntry(getInitializingShardRouting(routing).moveToStarted());
        SearchResponse response = client().prepareSearch().get();
        assertHitCount(response, 0);
    }

    public void testFailIfIndexNotPresentInRecoverFromStore() throws Exception {
        createIndex("test");
        ensureGreen();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        DiscoveryNode localNode = new DiscoveryNode("foo", LocalTransportAddress.buildUnique(), emptyMap(), emptySet(), Version.CURRENT);
        IndexService test = indicesService.indexService(resolveIndex("test"));
        final IndexShard shard = test.getShardOrNull(0);

        client().prepareIndex("test", "test", "0").setSource("{}").setRefreshPolicy(randomBoolean() ? IMMEDIATE : NONE).get();
        if (randomBoolean()) {
            client().admin().indices().prepareFlush().get();
        }
        final ShardRouting origRouting = shard.routingEntry();
        ShardRouting routing = origRouting;
        Store store = shard.store();
        store.incRef();
        test.removeShard(0, "b/c simon says so");
        cleanLuceneIndex(store.directory());
        store.decRef();
        routing = ShardRoutingHelper.reinit(routing);
        IndexShard newShard = test.createShard(routing);
        newShard.updateRoutingEntry(routing);
        newShard.markAsRecovering("store", new RecoveryState(newShard.shardId(), routing.primary(), RecoveryState.Type.STORE, localNode, localNode));
        try {
            newShard.recoverFromStore();
            fail("index not there!");
        } catch (IndexShardRecoveryException ex) {
            assertTrue(ex.getMessage().contains("failed to fetch index version after copying it over"));
        }

        routing = ShardRoutingHelper.moveToUnassigned(routing, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "because I say so"));
        routing = ShardRoutingHelper.initialize(routing, origRouting.currentNodeId());
        assertTrue("it's already recovering, we should ignore new ones", newShard.ignoreRecoveryAttempt());
        try {
            newShard.markAsRecovering("store", new RecoveryState(newShard.shardId(), routing.primary(), RecoveryState.Type.STORE, localNode, localNode));
            fail("we are already recovering, can't mark again");
        } catch (IllegalIndexShardStateException e) {
            // OK!
        }
        test.removeShard(0, "I broken it");
        newShard = test.createShard(routing);
        newShard.updateRoutingEntry(routing);
        newShard.markAsRecovering("store", new RecoveryState(newShard.shardId(), routing.primary(), RecoveryState.Type.STORE, localNode, localNode));
        assertTrue("recover even if there is nothing to recover", newShard.recoverFromStore());

        newShard.updateRoutingEntry(getInitializingShardRouting(routing).moveToStarted());
        SearchResponse response = client().prepareSearch().get();
        assertHitCount(response, 0);
        // we can't issue this request through a client because of the inconsistencies we created with the cluster state
        // doing it directly instead
        IndexRequest request = client().prepareIndex("test", "test", "0").setSource("{}").request();
        request.process(null, false, "test");
        TransportIndexAction.executeIndexRequestOnPrimary(request, newShard, null);
        newShard.refresh("test");
        assertHitCount(client().prepareSearch().get(), 1);
    }

    public void testRecoveryFailsAfterMovingToRelocatedState() throws InterruptedException, IOException {
        createIndex("test");
        ensureGreen();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService test = indicesService.indexService(resolveIndex("test"));
        final IndexShard shard = test.getShardOrNull(0);
        ShardRouting origRouting = shard.routingEntry();
        assertThat(shard.state(), equalTo(IndexShardState.STARTED));
        ShardRouting inRecoveryRouting = ShardRoutingHelper.relocate(origRouting, "some_node");
        shard.updateRoutingEntry(inRecoveryRouting);
        shard.relocated("simulate mark as relocated");
        assertThat(shard.state(), equalTo(IndexShardState.RELOCATED));
        try {
            shard.updateRoutingEntry(origRouting);
            fail("Expected IndexShardRelocatedException");
        } catch (IndexShardRelocatedException expected) {
        }
    }

    public void testRestoreShard() throws IOException {
        createIndex("test");
        createIndex("test_target");
        ensureGreen();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService test = indicesService.indexService(resolveIndex("test"));
        IndexService test_target = indicesService.indexService(resolveIndex("test_target"));
        final IndexShard test_shard = test.getShardOrNull(0);

        client().prepareIndex("test", "test", "0").setSource("{}").setRefreshPolicy(randomBoolean() ? IMMEDIATE : NONE).get();
        client().prepareIndex("test_target", "test", "1").setSource("{}").setRefreshPolicy(IMMEDIATE).get();
        assertHitCount(client().prepareSearch("test_target").get(), 1);
        assertSearchHits(client().prepareSearch("test_target").get(), "1");
        client().admin().indices().prepareFlush("test").get(); // only flush test
        final ShardRouting origRouting = test_target.getShardOrNull(0).routingEntry();
        ShardRouting routing = ShardRoutingHelper.reinit(origRouting);
        final Snapshot snapshot = new Snapshot("foo", new SnapshotId("bar", UUIDs.randomBase64UUID()));
        routing = ShardRoutingHelper.newWithRestoreSource(routing, new RestoreSource(snapshot, Version.CURRENT, "test"));
        test_target.removeShard(0, "just do it man!");
        final IndexShard test_target_shard = test_target.createShard(routing);
        Store sourceStore = test_shard.store();
        Store targetStore = test_target_shard.store();

        test_target_shard.updateRoutingEntry(routing);
        DiscoveryNode localNode = new DiscoveryNode("foo", LocalTransportAddress.buildUnique(), emptyMap(), emptySet(), Version.CURRENT);
        test_target_shard.markAsRecovering("store", new RecoveryState(routing.shardId(), routing.primary(), RecoveryState.Type.SNAPSHOT, routing.restoreSource(), localNode));
        assertTrue(test_target_shard.restoreFromRepository(new RestoreOnlyRepository("test") {
            @Override
            public void restoreShard(IndexShard shard, SnapshotId snapshotId, Version version, IndexId indexId, ShardId snapshotShardId, RecoveryState recoveryState) {
                try {
                    cleanLuceneIndex(targetStore.directory());
                    for (String file : sourceStore.directory().listAll()) {
                        if (file.equals("write.lock") || file.startsWith("extra")) {
                            continue;
                        }
                        targetStore.directory().copyFrom(sourceStore.directory(), file, file, IOContext.DEFAULT);
                    }
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }
        }));

        test_target_shard.updateRoutingEntry(routing.moveToStarted());
        assertHitCount(client().prepareSearch("test_target").get(), 1);
        assertSearchHits(client().prepareSearch("test_target").get(), "0");
    }

    public void testSearcherWrapperIsUsed() throws IOException {
        createIndex("test");
        ensureGreen();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexService(resolveIndex("test"));
        IndexShard shard = indexService.getShardOrNull(0);
        client().prepareIndex("test", "test", "0").setSource("{\"foo\" : \"bar\"}").setRefreshPolicy(IMMEDIATE).get();
        client().prepareIndex("test", "test", "1").setSource("{\"foobar\" : \"bar\"}").setRefreshPolicy(IMMEDIATE).get();

        Engine.GetResult getResult = shard.get(new Engine.Get(false, new Term(UidFieldMapper.NAME, Uid.createUid("test", "1"))));
        assertTrue(getResult.exists());
        assertNotNull(getResult.searcher());
        getResult.release();
        try (Engine.Searcher searcher = shard.acquireSearcher("test")) {
            TopDocs search = searcher.searcher().search(new TermQuery(new Term("foo", "bar")), 10);
            assertEquals(search.totalHits, 1);
            search = searcher.searcher().search(new TermQuery(new Term("foobar", "bar")), 10);
            assertEquals(search.totalHits, 1);
        }
        IndexSearcherWrapper wrapper = new IndexSearcherWrapper() {
            @Override
            public DirectoryReader wrap(DirectoryReader reader) throws IOException {
                return new FieldMaskingReader("foo", reader);
            }

            @Override
            public IndexSearcher wrap(IndexSearcher searcher) throws EngineException {
                return searcher;
            }
        };
        shard.close("simon says", true);
        IndexShard newShard = reinitWithWrapper(indexService, shard, wrapper);
        try {
            try (Engine.Searcher searcher = newShard.acquireSearcher("test")) {
                TopDocs search = searcher.searcher().search(new TermQuery(new Term("foo", "bar")), 10);
                assertEquals(search.totalHits, 0);
                search = searcher.searcher().search(new TermQuery(new Term("foobar", "bar")), 10);
                assertEquals(search.totalHits, 1);
            }
            getResult = newShard.get(new Engine.Get(false, new Term(UidFieldMapper.NAME, Uid.createUid("test", "1"))));
            assertTrue(getResult.exists());
            assertNotNull(getResult.searcher()); // make sure get uses the wrapped reader
            assertTrue(getResult.searcher().reader() instanceof FieldMaskingReader);
            getResult.release();
        } finally {
            newShard.close("just do it", randomBoolean());
        }
    }

    public void testSearcherWrapperWorksWithGlobaOrdinals() throws IOException {
        createIndex("test");
        ensureGreen();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexService(resolveIndex("test"));
        IndexShard shard = indexService.getShardOrNull(0);
        client().admin().indices().preparePutMapping("test").setType("test").setSource("foo", "type=text,fielddata=true").get();
        client().prepareIndex("test", "test", "0").setSource("{\"foo\" : \"bar\"}").setRefreshPolicy(IMMEDIATE).get();
        client().prepareIndex("test", "test", "1").setSource("{\"foobar\" : \"bar\"}").setRefreshPolicy(IMMEDIATE).get();

        IndexSearcherWrapper wrapper = new IndexSearcherWrapper() {
            @Override
            public DirectoryReader wrap(DirectoryReader reader) throws IOException {
                return new FieldMaskingReader("foo", reader);
            }

            @Override
            public IndexSearcher wrap(IndexSearcher searcher) throws EngineException {
                return searcher;
            }
        };

        shard.close("simon says", true);
        IndexShard newShard = reinitWithWrapper(indexService, shard, wrapper);
        try {
            // test global ordinals are evicted
            MappedFieldType foo = newShard.mapperService().fullName("foo");
            IndexFieldData.Global ifd = shard.indexFieldDataService().getForField(foo);
            FieldDataStats before = shard.fieldData().stats("foo");
            assertThat(before.getMemorySizeInBytes(), equalTo(0L));
            FieldDataStats after = null;
            try (Engine.Searcher searcher = newShard.acquireSearcher("test")) {
                assumeTrue("we have to have more than one segment", searcher.getDirectoryReader().leaves().size() > 1);
                ifd.loadGlobal(searcher.getDirectoryReader());
                after = shard.fieldData().stats("foo");
                assertEquals(after.getEvictions(), before.getEvictions());
                // If a field doesn't exist an empty IndexFieldData is returned and that isn't cached:
                assertThat(after.getMemorySizeInBytes(), equalTo(0L));
            }
            assertEquals(shard.fieldData().stats("foo").getEvictions(), before.getEvictions());
            assertEquals(shard.fieldData().stats("foo").getMemorySizeInBytes(), after.getMemorySizeInBytes());
            newShard.flush(new FlushRequest().force(true).waitIfOngoing(true));
            newShard.refresh("test");
            assertEquals(shard.fieldData().stats("foo").getMemorySizeInBytes(), before.getMemorySizeInBytes());
            assertEquals(shard.fieldData().stats("foo").getEvictions(), before.getEvictions());
        } finally {
            newShard.close("just do it", randomBoolean());
        }
    }

    public void testIndexingOperationListnenersIsInvokedOnRecovery() throws IOException {
        createIndex("test");
        ensureGreen();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexService(resolveIndex("test"));
        IndexShard shard = indexService.getShardOrNull(0);
        client().prepareIndex("test", "test", "0").setSource("{\"foo\" : \"bar\"}").get();
        client().prepareDelete("test", "test", "0").get();
        client().prepareIndex("test", "test", "1").setSource("{\"foo\" : \"bar\"}").setRefreshPolicy(IMMEDIATE).get();

        IndexSearcherWrapper wrapper = new IndexSearcherWrapper() {};
        shard.close("simon says", false);
        final AtomicInteger preIndex = new AtomicInteger();
        final AtomicInteger postIndex = new AtomicInteger();
        final AtomicInteger preDelete = new AtomicInteger();
        final AtomicInteger postDelete = new AtomicInteger();
        IndexingOperationListener listener = new IndexingOperationListener() {
            @Override
            public Engine.Index preIndex(Engine.Index operation) {
                preIndex.incrementAndGet();
                return operation;
            }

            @Override
            public void postIndex(Engine.Index index, boolean created) {
                postIndex.incrementAndGet();
            }

            @Override
            public Engine.Delete preDelete(Engine.Delete delete) {
                preDelete.incrementAndGet();
                return delete;
            }

            @Override
            public void postDelete(Engine.Delete delete) {
                postDelete.incrementAndGet();

            }
        };
        final IndexShard newShard = reinitWithWrapper(indexService, shard, wrapper, listener);
        try {
            IndexingStats indexingStats = newShard.indexingStats();
            // ensure we are not influencing the indexing stats
            assertEquals(0, indexingStats.getTotal().getDeleteCount());
            assertEquals(0, indexingStats.getTotal().getDeleteCurrent());
            assertEquals(0, indexingStats.getTotal().getIndexCount());
            assertEquals(0, indexingStats.getTotal().getIndexCurrent());
            assertEquals(0, indexingStats.getTotal().getIndexFailedCount());
            assertEquals(2, preIndex.get());
            assertEquals(2, postIndex.get());
            assertEquals(1, preDelete.get());
            assertEquals(1, postDelete.get());
        } finally {
            newShard.close("just do it", randomBoolean());
        }
    }

    public void testShardHasMemoryBufferOnTranslogRecover() throws Throwable {
        createIndex("test");
        ensureGreen();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexService(resolveIndex("test"));
        IndexShard shard = indexService.getShardOrNull(0);
        client().prepareIndex("test", "test", "0").setSource("{\"foo\" : \"bar\"}").get();
        client().prepareDelete("test", "test", "0").get();
        client().prepareIndex("test", "test", "1").setSource("{\"foo\" : \"bar\"}").setRefreshPolicy(IMMEDIATE).get();

        IndexSearcherWrapper wrapper = new IndexSearcherWrapper() {};
        shard.close("simon says", false);
        AtomicReference<IndexShard> shardRef = new AtomicReference<>();
        List<Exception> failures = new ArrayList<>();
        IndexingOperationListener listener = new IndexingOperationListener() {

            @Override
            public void postIndex(Engine.Index index, boolean created) {
                try {
                    assertNotNull(shardRef.get());
                    // this is all IMC needs to do - check current memory and refresh
                    assertTrue(shardRef.get().getIndexBufferRAMBytesUsed() > 0);
                    shardRef.get().refresh("test");
                } catch (Exception e) {
                    failures.add(e);
                    throw e;
                }
            }


            @Override
            public void postDelete(Engine.Delete delete) {
                try {
                    assertNotNull(shardRef.get());
                    // this is all IMC needs to do - check current memory and refresh
                    assertTrue(shardRef.get().getIndexBufferRAMBytesUsed() > 0);
                    shardRef.get().refresh("test");
                } catch (Exception e) {
                    failures.add(e);
                    throw e;
                }
            }
        };
        final IndexShard newShard = newIndexShard(indexService, shard, wrapper, listener);
        shardRef.set(newShard);
        recoverShard(newShard);

        try {
            ExceptionsHelper.rethrowAndSuppress(failures);
        } finally {
            newShard.close("just do it", randomBoolean());
        }
    }

    public void testSearchIsReleaseIfWrapperFails() throws IOException {
        createIndex("test");
        ensureGreen();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexService(resolveIndex("test"));
        IndexShard shard = indexService.getShardOrNull(0);
        client().prepareIndex("test", "test", "0").setSource("{\"foo\" : \"bar\"}").setRefreshPolicy(IMMEDIATE).get();
        IndexSearcherWrapper wrapper = new IndexSearcherWrapper() {
            @Override
            public DirectoryReader wrap(DirectoryReader reader) throws IOException {
                throw new RuntimeException("boom");
            }

            public IndexSearcher wrap(IndexSearcher searcher) throws EngineException {
                return searcher;
            }
        };

        shard.close("simon says", true);
        IndexShard newShard = reinitWithWrapper(indexService, shard, wrapper);
        try {
            newShard.acquireSearcher("test");
            fail("exception expected");
        } catch (RuntimeException ex) {
            //
        } finally {
            newShard.close("just do it", randomBoolean());
        }
    }

    public static final IndexShard reinitWithWrapper(IndexService indexService, IndexShard shard, IndexSearcherWrapper wrapper, IndexingOperationListener... listeners) throws IOException {
        IndexShard newShard = newIndexShard(indexService, shard, wrapper, listeners);
        return recoverShard(newShard);
    }

    public  static final IndexShard recoverShard(IndexShard newShard) throws IOException {
        DiscoveryNode localNode = new DiscoveryNode("foo", LocalTransportAddress.buildUnique(), emptyMap(), emptySet(), Version.CURRENT);
        newShard.markAsRecovering("store", new RecoveryState(newShard.shardId(), newShard.routingEntry().primary(), RecoveryState.Type.STORE, localNode, localNode));
        assertTrue(newShard.recoverFromStore());
        newShard.updateRoutingEntry(newShard.routingEntry().moveToStarted());
        return newShard;
    }

    public  static final IndexShard newIndexShard(IndexService indexService, IndexShard shard, IndexSearcherWrapper wrapper, IndexingOperationListener... listeners) throws IOException {
        ShardRouting initializingShardRouting = getInitializingShardRouting(shard.routingEntry());
        IndexShard newShard = new IndexShard(initializingShardRouting, indexService.getIndexSettings(), shard.shardPath(),
            shard.store(), indexService.cache(), indexService.mapperService(), indexService.similarityService(),
            indexService.fieldData(), shard.getEngineFactory(), indexService.getIndexEventListener(), wrapper,
            indexService.getThreadPool(), indexService.getBigArrays(), null, Collections.emptyList(), Arrays.asList(listeners));
        return newShard;
    }

    private static ShardRouting getInitializingShardRouting(ShardRouting existingShardRouting) {
        ShardRouting shardRouting = TestShardRouting.newShardRouting(existingShardRouting.shardId(),
            existingShardRouting.currentNodeId(), null, existingShardRouting.primary(), ShardRoutingState.INITIALIZING,
            existingShardRouting.allocationId());
        shardRouting = shardRouting.updateUnassignedInfo(new UnassignedInfo(UnassignedInfo.Reason.INDEX_REOPENED, "fake recovery"));
        return shardRouting;
    }

    public void testTranslogRecoverySyncsTranslog() throws IOException {
        createIndex("testindexfortranslogsync");
        client().admin().indices().preparePutMapping("testindexfortranslogsync").setType("testtype").setSource(jsonBuilder().startObject()
            .startObject("testtype")
            .startObject("properties")
            .startObject("foo")
            .field("type", "text")
            .endObject()
            .endObject().endObject().endObject()).get();
        ensureGreen();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService test = indicesService.indexService(resolveIndex("testindexfortranslogsync"));
        IndexShard shard = test.getShardOrNull(0);
        ShardRouting routing = getInitializingShardRouting(shard.routingEntry());
        test.removeShard(0, "b/c britta says so");
        IndexShard newShard = test.createShard(routing);
        DiscoveryNode localNode = new DiscoveryNode("foo", LocalTransportAddress.buildUnique(), emptyMap(), emptySet(), Version.CURRENT);
        newShard.markAsRecovering("for testing", new RecoveryState(newShard.shardId(), routing.primary(), RecoveryState.Type.REPLICA, localNode, localNode));
        List<Translog.Operation> operations = new ArrayList<>();
        operations.add(new Translog.Index("testtype", "1", BytesReference.toBytes(jsonBuilder().startObject().field("foo", "bar").endObject().bytes())));
        newShard.prepareForIndexRecovery();
        newShard.recoveryState().getTranslog().totalOperations(operations.size());
        newShard.skipTranslogRecovery();
        newShard.performBatchRecovery(operations);
        assertFalse(newShard.getTranslog().syncNeeded());
    }

    public void testIndexingBufferDuringInternalRecovery() throws IOException {
        createIndex("index");
        client().admin().indices().preparePutMapping("index").setType("testtype").setSource(jsonBuilder().startObject()
            .startObject("testtype")
            .startObject("properties")
            .startObject("foo")
            .field("type", "text")
            .endObject()
            .endObject().endObject().endObject()).get();
        ensureGreen();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService test = indicesService.indexService(resolveIndex("index"));
        IndexShard shard = test.getShardOrNull(0);
        ShardRouting routing = getInitializingShardRouting(shard.routingEntry());
        test.removeShard(0, "b/c britta says so");
        IndexShard newShard = test.createShard(routing);
        newShard.shardRouting = routing;
        DiscoveryNode localNode = new DiscoveryNode("foo", LocalTransportAddress.buildUnique(), emptyMap(), emptySet(), Version.CURRENT);
        newShard.markAsRecovering("for testing", new RecoveryState(newShard.shardId(), routing.primary(), RecoveryState.Type.REPLICA, localNode, localNode));
        // Shard is still inactive since we haven't started recovering yet
        assertFalse(newShard.isActive());
        newShard.prepareForIndexRecovery();
        // Shard is still inactive since we haven't started recovering yet
        assertFalse(newShard.isActive());
        newShard.performTranslogRecovery(true);
        // Shard should now be active since we did recover:
        assertTrue(newShard.isActive());
    }

    public void testIndexingBufferDuringPeerRecovery() throws IOException {
        createIndex("index");
        client().admin().indices().preparePutMapping("index").setType("testtype").setSource(jsonBuilder().startObject()
            .startObject("testtype")
            .startObject("properties")
            .startObject("foo")
            .field("type", "text")
            .endObject()
            .endObject().endObject().endObject()).get();
        ensureGreen();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService test = indicesService.indexService(resolveIndex("index"));
        IndexShard shard = test.getShardOrNull(0);
        ShardRouting routing = getInitializingShardRouting(shard.routingEntry());
        test.removeShard(0, "b/c britta says so");
        IndexShard newShard = test.createShard(routing);
        DiscoveryNode localNode = new DiscoveryNode("foo", LocalTransportAddress.buildUnique(), emptyMap(), emptySet(), Version.CURRENT);
        newShard.markAsRecovering("for testing", new RecoveryState(newShard.shardId(), routing.primary(), RecoveryState.Type.REPLICA, localNode, localNode));
        // Shard is still inactive since we haven't started recovering yet
        assertFalse(newShard.isActive());
        List<Translog.Operation> operations = new ArrayList<>();
        operations.add(new Translog.Index("testtype", "1", BytesReference.toBytes(jsonBuilder().startObject().field("foo", "bar").endObject().bytes())));
        newShard.prepareForIndexRecovery();
        newShard.skipTranslogRecovery();
        // Shard is still inactive since we haven't started recovering yet
        assertFalse(newShard.isActive());
        newShard.performBatchRecovery(operations);
        // Shard should now be active since we did recover:
        assertTrue(newShard.isActive());
    }

    public void testRecoverFromLocalShard() throws IOException {
        createIndex("index");
        createIndex("index_1");
        createIndex("index_2");
        client().admin().indices().preparePutMapping("index").setType("test").setSource(jsonBuilder().startObject()
            .startObject("test")
            .startObject("properties")
            .startObject("foo")
            .field("type", "text")
            .endObject()
            .endObject().endObject().endObject()).get();
        client().prepareIndex("index", "test", "0").setSource("{\"foo\" : \"bar\"}").setRefreshPolicy(IMMEDIATE).get();
        client().prepareIndex("index", "test", "1").setSource("{\"foo\" : \"bar\"}").setRefreshPolicy(IMMEDIATE).get();


        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService test = indicesService.indexService(resolveIndex("index_1"));
        IndexShard shard = test.getShardOrNull(0);
        ShardRouting routing = ShardRoutingHelper.initWithSameId(shard.routingEntry());
        test.removeShard(0, "b/c simon says so");
        DiscoveryNode localNode = new DiscoveryNode("foo", LocalTransportAddress.buildUnique(), emptyMap(), emptySet(), Version.CURRENT);
        {
            final IndexShard newShard = test.createShard(routing);
            newShard.updateRoutingEntry(routing);
            newShard.markAsRecovering("store", new RecoveryState(newShard.shardId(), routing.primary(), RecoveryState.Type.LOCAL_SHARDS, localNode, localNode));

            BiConsumer<String, MappingMetaData> mappingConsumer = (type, mapping) -> {
                try {
                    client().admin().indices().preparePutMapping().setConcreteIndex(newShard.indexSettings().getIndex())
                        .setType(type)
                        .setSource(mapping.source().string())
                        .get();
                } catch (IOException ex) {
                    throw new ElasticsearchException("failed to stringify mapping source", ex);
                }
            };
            expectThrows(IllegalArgumentException.class, () -> {
                IndexService index = indicesService.indexService(resolveIndex("index"));
                IndexService index_2 = indicesService.indexService(resolveIndex("index_2"));
                newShard.recoverFromLocalShards(mappingConsumer, Arrays.asList(index.getShard(0), index_2.getShard(0)));
            });

            IndexService indexService = indicesService.indexService(resolveIndex("index"));
            assertTrue(newShard.recoverFromLocalShards(mappingConsumer, Arrays.asList(indexService.getShard(0))));
            RecoveryState recoveryState = newShard.recoveryState();
            assertEquals(RecoveryState.Stage.DONE, recoveryState.getStage());
            assertTrue(recoveryState.getIndex().fileDetails().size() > 0);
            for (RecoveryState.File file : recoveryState.getIndex().fileDetails()) {
                if (file.reused()) {
                    assertEquals(file.recovered(), 0);
                } else {
                    assertEquals(file.recovered(), file.length());
                }
            }
            routing = ShardRoutingHelper.moveToStarted(routing);
            newShard.updateRoutingEntry(routing);
            assertHitCount(client().prepareSearch("index_1").get(), 2);
        }
        // now check that it's persistent ie. that the added shards are committed
        {
            routing = shard.routingEntry();
            test.removeShard(0, "b/c simon says so");
            routing = ShardRoutingHelper.reinit(routing);
            final IndexShard newShard = test.createShard(routing);
            newShard.markAsRecovering("store", new RecoveryState(newShard.shardId(), routing.primary(), RecoveryState.Type.LOCAL_SHARDS, localNode, localNode));
            assertTrue(newShard.recoverFromStore());
            routing = ShardRoutingHelper.moveToStarted(routing);
            newShard.updateRoutingEntry(routing);
            assertHitCount(client().prepareSearch("index_1").get(), 2);
        }

        GetMappingsResponse mappingsResponse = client().admin().indices().prepareGetMappings("index_1").get();
        ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings = mappingsResponse.getMappings();
        assertNotNull(mappings.get("index_1"));
        assertNotNull(mappings.get("index_1").get("test"));
        assertEquals(mappings.get("index_1").get("test").get().source().string(), "{\"test\":{\"properties\":{\"foo\":{\"type\":\"text\"}}}}");

    }

    /** A dummy repository for testing which just needs restore overridden */
    private abstract static class RestoreOnlyRepository extends AbstractLifecycleComponent implements Repository {
        private final String indexName;
        public RestoreOnlyRepository(String indexName) {
            super(Settings.EMPTY);
            this.indexName = indexName;
        }
        @Override
        protected void doStart() {}
        @Override
        protected void doStop() {}
        @Override
        protected void doClose() {}
        @Override
        public RepositoryMetaData getMetadata() {
            return null;
        }
        @Override
        public SnapshotInfo getSnapshotInfo(SnapshotId snapshotId) {
            return null;
        }
        @Override
        public MetaData getSnapshotMetaData(SnapshotInfo snapshot, List<IndexId> indices) throws IOException {
            return null;
        }
        @Override
        public RepositoryData getRepositoryData() {
            Map<IndexId, Set<SnapshotId>> map = new HashMap<>();
            map.put(new IndexId(indexName, "blah"), Collections.emptySet());
            return new RepositoryData(Collections.emptyList(), map);
        }
        @Override
        public void initializeSnapshot(SnapshotId snapshotId, List<IndexId> indices, MetaData metaData) {}
        @Override
        public SnapshotInfo finalizeSnapshot(SnapshotId snapshotId, List<IndexId> indices, long startTime, String failure, int totalShards, List<SnapshotShardFailure> shardFailures) {
            return null;
        }
        @Override
        public void deleteSnapshot(SnapshotId snapshotId) {}
        @Override
        public long getSnapshotThrottleTimeInNanos() {
            return 0;
        }
        @Override
        public long getRestoreThrottleTimeInNanos() {
            return 0;
        }
        @Override
        public String startVerification() {
            return null;
        }
        @Override
        public void endVerification(String verificationToken) {}
        @Override
        public boolean isReadOnly() {
            return false;
        }
        @Override
        public void snapshotShard(IndexShard shard, SnapshotId snapshotId, IndexId indexId, IndexCommit snapshotIndexCommit, IndexShardSnapshotStatus snapshotStatus) {}
        @Override
        public IndexShardSnapshotStatus getShardSnapshotStatus(SnapshotId snapshotId, Version version, IndexId indexId, ShardId shardId) {
            return null;
        }
        @Override
        public void verify(String verificationToken, DiscoveryNode localNode) {}
    }
}
