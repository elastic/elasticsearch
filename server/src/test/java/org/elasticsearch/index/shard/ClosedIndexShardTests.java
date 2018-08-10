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

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.search.Sort;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.cache.query.DisabledQueryCache;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.InternalEngineFactory;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.index.store.DirectoryService;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.test.DummyShardLock;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.cluster.routing.TestShardRouting.newShardRouting;
import static org.elasticsearch.index.MapperTestUtils.newMapperService;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class ClosedIndexShardTests extends IndexShardTestCase {

    /**
     * Tests IndexShard -> ClosedIndexShard
     */
    public void testFromIndexShardToClosedIndexShard() throws Exception {
        final IndexShard shard = newStartedShard();
        for (int i = 0; i < randomIntBetween(0, 20); i++) {
            indexDoc(shard, "_doc", String.valueOf(i));
        }
        if (randomBoolean()) {
            flushShard(shard, randomBoolean());
        }

        assertThat("index shard is started", shard.routingEntry().state(), is(ShardRoutingState.STARTED));
        assertThat("index shard state is started", shard.state(), is(IndexShardState.STARTED));
        assertThat("index shard has an engine", shard.getEngine(), notNullValue());
        assertThat("store has two refs, 1 for the store itself and 1 for the engine", shard.store().refCount(), equalTo(2));

        final ClosedIndexShard closedShard = moveToClosedIndexShard(shard);

        assertThat("index shard has been closed when moving to a closed instance", shard.state(), is(IndexShardState.CLOSED));
        assertThat("index shard store has not been closed", shard.store().refCount(), greaterThan(0));
        expectThrows(AlreadyClosedException.class, shard::getEngine);

        assertThat("closed shard is started", closedShard.routingEntry().state(), is(ShardRoutingState.STARTED));
        assertThat("closed shard state is started", closedShard.state(), is(IndexShardState.STARTED));
        assertSame("closed shard store is the same as the original index shard store", shard.store(), closedShard.store());
        assertThat("closed shard store has 1 ref", closedShard.store().refCount(), equalTo(1));

        IOUtils.close(closedShard.store());

        assertThat("index shard store is closed", shard.store().refCount(), equalTo(0));
        assertThat("closed shard store is closed", closedShard.store().refCount(), equalTo(0));
    }

    /**
     * Tests ClosedIndexShard -> IndexShard
     */
    public void testFromClosedIndexShardToIndexShard() throws Exception {
        final IndexShard source;

        final IndexShard primary = newStartedShard(true);
        for (int i = 0; i < randomIntBetween(0, 20); i++) {
            indexDoc(primary, "_doc", String.valueOf(i));
        }
        final IndexShard replica;
        if (randomBoolean()) {
            replica =  newShard(false);
            recoverReplica(replica, primary, true);
            source = replica;
        } else {
            source = primary;
        }
        assertNotNull(source);
        closeShards(source);

        final ShardId shardId = source.shardId();
        final IndexSettings settings = source.indexSettings();
        final ShardPath path = source.shardPath();
        final ShardRouting routing = source.routingEntry();
        final Directory directory = newFSDirectory(path.resolveIndex());
        final Store store = new Store(shardId, source.indexSettings(), new DirectoryService(shardId, settings) {
            @Override
            public Directory newDirectory() {
                return directory;
            }
        }, new DummyShardLock(shardId));

        final ClosedIndexShard closedShard = new ClosedIndexShard(shardId, settings, path, store, routing, IndexShardState.STARTED);

        assertThat("closed index shard is started", closedShard.routingEntry().state(), is(ShardRoutingState.STARTED));
        assertThat("closed index shard state is started", closedShard.state(), is(IndexShardState.STARTED));
        assertThat("closed shard store has 1 ref", closedShard.store().refCount(), equalTo(1));

        final IndexShard shard = executeMoveToIndexShard(closedShard, reopen(closedShard.routingEntry()));
        if (shard.routingEntry().primary()) {
            recoverShardFromStore(shard);
        } else {
            recoverReplica(shard, primary, true);
            closeShards(primary);
        }

        assertThat("closed index shard has been closed when moving to an opened instance", closedShard.state(), is(IndexShardState.CLOSED));
        assertThat("closed shard store has not been closed", closedShard.store().refCount(), greaterThan(0));

        assertThat("index shard is started", shard.routingEntry().state(), is(ShardRoutingState.STARTED));
        assertThat("index shard state is started", shard.state(), is(IndexShardState.STARTED));
        assertThat("index shard has an engine", shard.getEngine(), notNullValue());
        assertThat("store has two refs, 1 for the store itself and 1 for the engine", shard.store().refCount(), equalTo(2));

        closeShards(shard);

        assertThat("index shard store is closed", shard.store().refCount(), equalTo(0));
        assertThat("closed shard store is closed", closedShard.store().refCount(), equalTo(0));
    }

    /**
     * Tests IndexShard -> ClosedIndexShard -> IndexShard like an index reopening
     */
    public void testCloseAndReopen() throws IOException {
        final int nbDocs = randomIntBetween(0, 20);

        final IndexShard primary = newStartedShard(true);
        for (int i = 0; i < nbDocs; i++) {
            indexDoc(primary, "_doc", String.valueOf(i));
        }

        final IndexShard replica =  randomBoolean() ? newShard(false) : null;
        if (replica != null) {
            recoverReplica(replica, primary, true);
        }

        final IndexShard shard = (replica != null) ? randomFrom(primary, replica) : primary;
        assertDocCount(shard, nbDocs);
        if (randomBoolean()) {
            flushShard(shard, randomBoolean());
        }

        final ClosedIndexShard closedShard = moveToClosedIndexShard(shard);
        assertThat("index shard has been closed when moving to a closed instance", shard.state(), is(IndexShardState.CLOSED));

        final IndexShard reopenedShard = executeMoveToIndexShard(closedShard, reopen(closedShard.routingEntry()));
        assertThat("closed index shard has been closed when moving to an opened instance", closedShard.state(), is(IndexShardState.CLOSED));

        if (reopenedShard.routingEntry().primary()) {
            recoverShardFromStore(reopenedShard);
        } else {
            recoverReplica(reopenedShard, primary, true);
        }

        assertThat("index shard is started", reopenedShard.routingEntry().state(), is(ShardRoutingState.STARTED));
        assertDocCount(reopenedShard, nbDocs);

        closeShards(reopenedShard, primary, replica);
    }

    /**
     * Moves an {@link IndexShard} to a {@link ClosedIndexShard} instance and closes the index shard. Assumes that the index shard
     * cannot be closed during the execution of the method.
     */
    ClosedIndexShard moveToClosedIndexShard(final IndexShard shard) {
        final Store store = shard.store();
        store.incRef();

        boolean success = false;
        try {
            final IndexShardState state = shard.state();
            if (state != IndexShardState.STARTED) {
                throw new IndexShardNotStartedException(shard.shardId(), state);
            }

            final ClosedIndexShard closedShard =
                new ClosedIndexShard(shard.shardId(), shard.indexSettings(), shard.shardPath(), store, shard.routingEntry(), state);
            success = true;
            return closedShard;
        } finally {
            store.decRef();
            if (success) {
                try {
                    shard.close("moving index shard to closed index shard", true);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("[{}] failed to close index shard", shard.shardId()), e);
                }
            }
        }
    }

    /**
     * Moves an {@link ClosedIndexShard} to a {@link IndexShard} instance and closes the closed index shard. Assumes that the closed
     * index shard cannot be closed during the execution of the method.
     */
    IndexShard moveToIndexShard(final ClosedIndexShard shard,
                                final ShardRouting newRouting,
                                Supplier<Sort> indexSortSupplier,
                                IndexCache indexCache,
                                MapperService mapperService,
                                SimilarityService similarityService,
                                @Nullable EngineFactory engineFactory,
                                IndexEventListener indexEventListener,
                                IndexSearcherWrapper indexSearcherWrapper,
                                ThreadPool threadPool,
                                BigArrays bigArrays,
                                Engine.Warmer warmer,
                                List<SearchOperationListener> searchOperationListener,
                                List<IndexingOperationListener> listeners,
                                Runnable globalCheckpointSyncer,
                                CircuitBreakerService circuitBreakerService) throws IOException {
        final Store store = shard.store();
        store.incRef();

        boolean success = false;
        try {
            final IndexSettings indexSettings = shard.indexSettings();
            final ShardPath path = shard.shardPath();
            final IndexShardState state = shard.state();
            if (state != IndexShardState.STARTED) {
                throw new IndexShardNotStartedException(shard.shardId(), state);
            }

            IndexShard indexShard = new IndexShard(newRouting, indexSettings, path, store, indexSortSupplier, indexCache, mapperService,
                similarityService, engineFactory, indexEventListener, indexSearcherWrapper, threadPool,
                bigArrays, warmer, searchOperationListener, listeners, globalCheckpointSyncer, circuitBreakerService);

            success = true;
            return indexShard;
        } catch (IOException e) {
            logger.error(() -> new ParameterizedMessage("[{}] failed to move closed index shard to index shard", shard.shardId()), e);
            throw e;
        } finally {
            store.decRef();
            if (success) {
                try {
                    shard.close("moving closed index shard to index shard", () -> {});
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("[{}] failed to close index shard", shard.shardId()), e);
                }
            }
        }
    }

    /** Utility method to execute moveToIndexShard() by passing all the necessary stuff that is normally maintained in the IndexService **/
    private IndexShard executeMoveToIndexShard(final ClosedIndexShard shard, final ShardRouting newRouting) throws IOException {
        IndexSettings indexSettings = shard.indexSettings();

        // Those are maintained by the IndexService, we recreate them as IndexShardTestCase would do
        Supplier<Sort> indexSortSupplier = () -> null;
        IndexCache indexCache = new IndexCache(indexSettings, new DisabledQueryCache(indexSettings), null);
        MapperService mapperService = newMapperService(xContentRegistry(), createTempDir(), indexSettings.getSettings(), "index");
        mapperService.merge(indexSettings.getIndexMetaData(), MapperService.MergeReason.MAPPING_RECOVERY);
        SimilarityService similarityService = new SimilarityService(indexSettings, null, Collections.emptyMap());
        EngineFactory engineFactory = new InternalEngineFactory();
        IndexEventListener indexEventListener = EMPTY_EVENT_LISTENER;
        IndexSearcherWrapper indexSearcherWrapper = null;
        BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
        final Engine.Warmer warmer = searcher -> {};
        List<SearchOperationListener> searchOperationListener = Collections.emptyList();
        List<IndexingOperationListener> listeners = Collections.emptyList();
        Runnable globalCheckpointSyncer = () -> {};
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        CircuitBreakerService circuitBreakerService = new HierarchyCircuitBreakerService(Settings.EMPTY, clusterSettings);

        return moveToIndexShard(shard,
                                newRouting,
                                indexSortSupplier,
                                indexCache,
                                mapperService,
                                similarityService,
                                engineFactory,
                                indexEventListener,
                                indexSearcherWrapper,
                                threadPool,
                                bigArrays,
                                warmer,
                                searchOperationListener,
                                listeners,
                                globalCheckpointSyncer,
                                circuitBreakerService);
    }

    /** Updates a shard routing entry as if the index was reopened **/
    private static ShardRouting reopen(final ShardRouting closedShardRouting) {
        assert closedShardRouting.started();
        final ShardId shardId = closedShardRouting.shardId();
        final boolean primary = closedShardRouting.primary();
        final RecoverySource recoverySource;
        if (primary) {
            recoverySource = RecoverySource.StoreRecoverySource.EXISTING_STORE_INSTANCE;
        } else {
            recoverySource = RecoverySource.PeerRecoverySource.INSTANCE;
        }
        return newShardRouting(shardId, closedShardRouting.currentNodeId(), primary, recoverySource, ShardRoutingState.INITIALIZING);
    }
}
