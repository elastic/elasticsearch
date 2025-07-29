/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.snapshots.sourceonly;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.Bits;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.engine.InternalEngineFactory;
import org.elasticsearch.index.fieldvisitor.FieldsVisitor;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.seqno.RetentionLeaseSyncer;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.repositories.FinalizeSnapshotContext;
import org.elasticsearch.repositories.FinalizeSnapshotContext.UpdatedShardGenerations;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.repositories.ShardSnapshotResult;
import org.elasticsearch.repositories.SnapshotIndexCommit;
import org.elasticsearch.repositories.SnapshotShardContext;
import org.elasticsearch.repositories.blobstore.BlobStoreTestUtil;
import org.elasticsearch.repositories.blobstore.ESBlobStoreRepositoryIntegTestCase;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.routing.TestShardRouting.shardRoutingBuilder;

public class SourceOnlySnapshotShardTests extends IndexShardTestCase {

    public void testSourceIncomplete() throws IOException {
        ShardRouting shardRouting = shardRoutingBuilder(
            new ShardId("index", "_na_", 0),
            randomAlphaOfLength(10),
            true,
            ShardRoutingState.INITIALIZING
        ).withRecoverySource(RecoverySource.EmptyStoreRecoverySource.INSTANCE).build();
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        IndexMetadata metadata = IndexMetadata.builder(shardRouting.getIndexName())
            .settings(settings)
            .primaryTerm(0, primaryTerm)
            .putMapping("{\"_source\":{\"enabled\": false}}")
            .build();
        IndexShard shard = newShard(shardRouting, metadata, null, new InternalEngineFactory());
        recoverShardFromStore(shard);

        for (int i = 0; i < 1; i++) {
            final String id = Integer.toString(i);
            indexDoc(shard, "_doc", id);
        }
        SnapshotId snapshotId = new SnapshotId("test", "test");
        IndexId indexId = new IndexId(shard.shardId().getIndexName(), shard.shardId().getIndex().getUUID());
        SourceOnlySnapshotRepository repository = new SourceOnlySnapshotRepository(createRepository());
        repository.start();
        try (Engine.IndexCommitRef snapshotRef = shard.acquireLastIndexCommit(true)) {
            IndexShardSnapshotStatus indexShardSnapshotStatus = IndexShardSnapshotStatus.newInitializing(new ShardGeneration(-1L));
            final PlainActionFuture<ShardSnapshotResult> future = new PlainActionFuture<>();
            runAsSnapshot(
                shard.getThreadPool(),
                () -> repository.snapshotShard(
                    new SnapshotShardContext(
                        shard.store(),
                        shard.mapperService(),
                        snapshotId,
                        indexId,
                        new SnapshotIndexCommit(snapshotRef),
                        null,
                        indexShardSnapshotStatus,
                        IndexVersion.current(),
                        randomMillisUpToYear9999(),
                        future
                    )
                )
            );
            IllegalStateException illegalStateException = expectThrows(IllegalStateException.class, future::actionGet);
            assertEquals(
                "Can't snapshot _source only on an index that has incomplete source ie. has _source disabled or filters the source",
                illegalStateException.getMessage()
            );
        }
        closeShards(shard);
    }

    public void testSourceIncompleteSyntheticSourceNoDoc() throws IOException {
        ShardRouting shardRouting = shardRoutingBuilder(
            new ShardId("index", "_na_", 0),
            randomAlphaOfLength(10),
            true,
            ShardRoutingState.INITIALIZING
        ).withRecoverySource(RecoverySource.EmptyStoreRecoverySource.INSTANCE).build();
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), "synthetic")
            .build();
        IndexMetadata metadata = IndexMetadata.builder(shardRouting.getIndexName()).settings(settings).primaryTerm(0, primaryTerm).build();
        IndexShard shard = newShard(shardRouting, metadata, null, new InternalEngineFactory());
        recoverShardFromStore(shard);
        SnapshotId snapshotId = new SnapshotId("test", "test");
        IndexId indexId = new IndexId(shard.shardId().getIndexName(), shard.shardId().getIndex().getUUID());
        SourceOnlySnapshotRepository repository = new SourceOnlySnapshotRepository(createRepository());
        repository.start();
        try (Engine.IndexCommitRef snapshotRef = shard.acquireLastIndexCommit(true)) {
            IndexShardSnapshotStatus indexShardSnapshotStatus = IndexShardSnapshotStatus.newInitializing(new ShardGeneration(-1L));
            final PlainActionFuture<ShardSnapshotResult> future = new PlainActionFuture<>();
            runAsSnapshot(
                shard.getThreadPool(),
                () -> repository.snapshotShard(
                    new SnapshotShardContext(
                        shard.store(),
                        shard.mapperService(),
                        snapshotId,
                        indexId,
                        new SnapshotIndexCommit(snapshotRef),
                        null,
                        indexShardSnapshotStatus,
                        IndexVersion.current(),
                        randomMillisUpToYear9999(),
                        future
                    )
                )
            );
            IllegalStateException illegalStateException = expectThrows(IllegalStateException.class, future::actionGet);
            assertEquals(
                "Can't snapshot _source only on an index that has incomplete source ie. has _source disabled or filters the source",
                illegalStateException.getMessage()
            );
        }
        closeShards(shard);
    }

    public void testIncrementalSnapshot() throws IOException {
        IndexShard shard = newStartedShard();
        for (int i = 0; i < 10; i++) {
            final String id = Integer.toString(i);
            indexDoc(shard, "_doc", id);
        }

        IndexId indexId = new IndexId(shard.shardId().getIndexName(), shard.shardId().getIndex().getUUID());
        SourceOnlySnapshotRepository repository = new SourceOnlySnapshotRepository(createRepository());
        repository.start();
        int totalFileCount;
        ShardGeneration shardGeneration;
        try (Engine.IndexCommitRef snapshotRef = shard.acquireLastIndexCommit(true)) {
            IndexShardSnapshotStatus indexShardSnapshotStatus = IndexShardSnapshotStatus.newInitializing(null);
            SnapshotId snapshotId = new SnapshotId("test", "test");
            final PlainActionFuture<ShardSnapshotResult> future = new PlainActionFuture<>();
            runAsSnapshot(
                shard.getThreadPool(),
                () -> repository.snapshotShard(
                    new SnapshotShardContext(
                        shard.store(),
                        shard.mapperService(),
                        snapshotId,
                        indexId,
                        new SnapshotIndexCommit(snapshotRef),
                        null,
                        indexShardSnapshotStatus,
                        IndexVersion.current(),
                        randomMillisUpToYear9999(),
                        future
                    )
                )
            );
            shardGeneration = future.actionGet().getGeneration();
            IndexShardSnapshotStatus.Copy copy = indexShardSnapshotStatus.asCopy();
            assertEquals(copy.getTotalFileCount(), copy.getIncrementalFileCount());
            totalFileCount = copy.getTotalFileCount();
            assertEquals(copy.getStage(), IndexShardSnapshotStatus.Stage.DONE);
        }

        indexDoc(shard, "_doc", Integer.toString(10));
        indexDoc(shard, "_doc", Integer.toString(11));
        try (Engine.IndexCommitRef snapshotRef = shard.acquireLastIndexCommit(true)) {
            SnapshotId snapshotId = new SnapshotId("test_1", "test_1");

            IndexShardSnapshotStatus indexShardSnapshotStatus = IndexShardSnapshotStatus.newInitializing(shardGeneration);
            final PlainActionFuture<ShardSnapshotResult> future = new PlainActionFuture<>();
            runAsSnapshot(
                shard.getThreadPool(),
                () -> repository.snapshotShard(
                    new SnapshotShardContext(
                        shard.store(),
                        shard.mapperService(),
                        snapshotId,
                        indexId,
                        new SnapshotIndexCommit(snapshotRef),
                        null,
                        indexShardSnapshotStatus,
                        IndexVersion.current(),
                        randomMillisUpToYear9999(),
                        future
                    )
                )
            );
            shardGeneration = future.actionGet().getGeneration();
            IndexShardSnapshotStatus.Copy copy = indexShardSnapshotStatus.asCopy();
            // we processed the segments_N file plus _1.si, _1.fnm, _1.fdx, _1.fdt, _1.fdm
            assertEquals(6, copy.getIncrementalFileCount());
            // in total we have 5 more files than the previous snap since we don't count the segments_N twice
            assertEquals(totalFileCount + 5, copy.getTotalFileCount());
            assertEquals(copy.getStage(), IndexShardSnapshotStatus.Stage.DONE);
        }
        deleteDoc(shard, Integer.toString(10));
        try (Engine.IndexCommitRef snapshotRef = shard.acquireLastIndexCommit(true)) {
            SnapshotId snapshotId = new SnapshotId("test_2", "test_2");

            IndexShardSnapshotStatus indexShardSnapshotStatus = IndexShardSnapshotStatus.newInitializing(shardGeneration);
            final PlainActionFuture<ShardSnapshotResult> future = new PlainActionFuture<>();
            runAsSnapshot(
                shard.getThreadPool(),
                () -> repository.snapshotShard(
                    new SnapshotShardContext(
                        shard.store(),
                        shard.mapperService(),
                        snapshotId,
                        indexId,
                        new SnapshotIndexCommit(snapshotRef),
                        null,
                        indexShardSnapshotStatus,
                        IndexVersion.current(),
                        randomMillisUpToYear9999(),
                        future
                    )
                )
            );
            future.actionGet();
            IndexShardSnapshotStatus.Copy copy = indexShardSnapshotStatus.asCopy();
            // we processed the segments_N file plus _1_1.liv
            assertEquals(2, copy.getIncrementalFileCount());
            // in total we have 6 more files than the previous snap since we don't count the segments_N twice
            assertEquals(totalFileCount + 6, copy.getTotalFileCount());
            assertEquals(copy.getStage(), IndexShardSnapshotStatus.Stage.DONE);
        }
        closeShards(shard);
    }

    private String randomDoc() {
        return "{ \"value\" : \"" + randomAlphaOfLength(10) + "\"}";
    }

    public void testRestoreMinimal() throws IOException {
        IndexShard shard = newStartedShard(true);
        int numInitialDocs = randomIntBetween(10, 100);
        for (int i = 0; i < numInitialDocs; i++) {
            final String id = Integer.toString(i);
            indexDoc(shard, id, randomDoc());
            if (randomBoolean()) {
                shard.refresh("test");
            }
        }
        for (int i = 0; i < numInitialDocs; i++) {
            final String id = Integer.toString(i);
            if (randomBoolean()) {
                if (rarely()) {
                    deleteDoc(shard, id);
                } else {
                    indexDoc(shard, id, randomDoc());
                }
            }
            if (frequently()) {
                shard.refresh("test");
            }
        }
        SnapshotId snapshotId = new SnapshotId("test", "test");
        IndexId indexId = new IndexId(shard.shardId().getIndexName(), shard.shardId().getIndex().getUUID());
        SourceOnlySnapshotRepository repository = new SourceOnlySnapshotRepository(createRepository());
        repository.start();
        try (Engine.IndexCommitRef snapshotRef = shard.acquireLastIndexCommit(true)) {
            IndexShardSnapshotStatus indexShardSnapshotStatus = IndexShardSnapshotStatus.newInitializing(null);
            final PlainActionFuture<ShardSnapshotResult> future = new PlainActionFuture<>();
            runAsSnapshot(shard.getThreadPool(), () -> {
                repository.snapshotShard(
                    new SnapshotShardContext(
                        shard.store(),
                        shard.mapperService(),
                        snapshotId,
                        indexId,
                        new SnapshotIndexCommit(snapshotRef),
                        null,
                        indexShardSnapshotStatus,
                        IndexVersion.current(),
                        randomMillisUpToYear9999(),
                        future
                    )
                );
                future.actionGet();
                final PlainActionFuture<SnapshotInfo> finFuture = new PlainActionFuture<>();
                final ShardGenerations shardGenerations = ShardGenerations.builder()
                    .put(indexId, 0, indexShardSnapshotStatus.generation())
                    .build();
                repository.finalizeSnapshot(
                    new FinalizeSnapshotContext(
                        new UpdatedShardGenerations(shardGenerations, ShardGenerations.EMPTY),
                        ESBlobStoreRepositoryIntegTestCase.getRepositoryData(repository).getGenId(),
                        Metadata.builder().put(shard.indexSettings().getIndexMetadata(), false).build(),
                        new SnapshotInfo(
                            new Snapshot(repository.getMetadata().name(), snapshotId),
                            shardGenerations.indices().stream().map(IndexId::getName).collect(Collectors.toList()),
                            Collections.emptyList(),
                            Collections.emptyList(),
                            null,
                            1L,
                            shardGenerations.totalShards(),
                            Collections.emptyList(),
                            true,
                            Collections.emptyMap(),
                            0L,
                            Collections.emptyMap()
                        ),
                        IndexVersion.current(),
                        new ActionListener<>() {
                            @Override
                            public void onResponse(RepositoryData repositoryData) {
                                // nothing will resolve in the onDone callback below
                            }

                            @Override
                            public void onFailure(Exception e) {
                                finFuture.onFailure(e);
                            }
                        },
                        finFuture::onResponse
                    )
                );
                finFuture.actionGet();
            });
            IndexShardSnapshotStatus.Copy copy = indexShardSnapshotStatus.asCopy();
            assertEquals(copy.getTotalFileCount(), copy.getIncrementalFileCount());
            assertEquals(copy.getStage(), IndexShardSnapshotStatus.Stage.DONE);
        }
        shard.refresh("test");
        ShardRouting shardRouting = shardRoutingBuilder(
            new ShardId("index", "_na_", 0),
            randomAlphaOfLength(10),
            true,
            ShardRoutingState.INITIALIZING
        ).withRecoverySource(
            new RecoverySource.SnapshotRecoverySource(
                UUIDs.randomBase64UUID(),
                new Snapshot("src_only", snapshotId),
                IndexVersion.current(),
                indexId
            )
        ).build();

        IndexMetadata metadata = repository.getSnapshotIndexMetaData(
            safeAwait(listener -> repository.getRepositoryData(EsExecutors.DIRECT_EXECUTOR_SERVICE, listener)),
            snapshotId,
            indexId
        );
        IndexShard restoredShard = newShard(
            shardRouting,
            metadata,
            null,
            SourceOnlySnapshotRepository.getEngineFactory(),
            NOOP_GCP_SYNCER,
            RetentionLeaseSyncer.EMPTY
        );
        DiscoveryNode discoveryNode = DiscoveryNodeUtils.create("node_g");
        restoredShard.markAsRecovering("test from snap", new RecoveryState(restoredShard.routingEntry(), discoveryNode, null));
        runAsSnapshot(shard.getThreadPool(), () -> {
            final PlainActionFuture<Boolean> future = new PlainActionFuture<>();
            restoredShard.restoreFromRepository(repository, future);
            assertTrue(future.actionGet());
        });
        assertEquals(restoredShard.recoveryState().getStage(), RecoveryState.Stage.DONE);
        assertEquals(restoredShard.recoveryState().getTranslog().recoveredOperations(), 0);
        assertEquals(IndexShardState.POST_RECOVERY, restoredShard.state());
        restoredShard.refresh("test");
        assertEquals(restoredShard.docStats().getCount(), shard.docStats().getCount());
        EngineException engineException = expectThrows(
            EngineException.class,
            () -> restoredShard.get(new Engine.Get(false, false, Integer.toString(0)))
        );
        assertEquals(engineException.getCause().getMessage(), "_source only indices can't be searched or filtered");
        SeqNoStats seqNoStats = restoredShard.seqNoStats();
        assertEquals(seqNoStats.getMaxSeqNo(), seqNoStats.getLocalCheckpoint());
        final IndexShard targetShard;
        try (Engine.Searcher searcher = restoredShard.acquireSearcher("test")) {
            assertEquals(searcher.getIndexReader().maxDoc(), seqNoStats.getLocalCheckpoint());
            TopDocs search = searcher.search(new MatchAllDocsQuery(), Integer.MAX_VALUE);
            assertEquals(searcher.getIndexReader().numDocs(), search.totalHits.value());
            search = searcher.search(
                new MatchAllDocsQuery(),
                Integer.MAX_VALUE,
                new Sort(new SortField(SeqNoFieldMapper.NAME, SortField.Type.LONG)),
                false
            );
            assertEquals(searcher.getIndexReader().numDocs(), search.totalHits.value());
            long previous = -1;
            for (ScoreDoc doc : search.scoreDocs) {
                FieldDoc fieldDoc = (FieldDoc) doc;
                assertEquals(1, fieldDoc.fields.length);
                long current = (Long) fieldDoc.fields[0];
                assertThat(previous, Matchers.lessThan(current));
                previous = current;
            }
            expectThrows(UnsupportedOperationException.class, () -> searcher.search(new TermQuery(new Term("boom", "boom")), 1));
            targetShard = reindex(
                searcher.getDirectoryReader(),
                new MappingMetadata("_doc", restoredShard.mapperService().documentMapper().mapping().getMeta())
            );
        }

        for (int i = 0; i < numInitialDocs; i++) {
            Engine.Get get = new Engine.Get(false, false, Integer.toString(i));
            Engine.GetResult original = shard.get(get);
            Engine.GetResult restored = targetShard.get(get);
            assertEquals(original.exists(), restored.exists());

            if (original.exists()) {
                StoredFields storedFields = original.docIdAndVersion().reader.storedFields();
                Document document = storedFields.document(original.docIdAndVersion().docId);
                Document restoredDocument = storedFields.document(restored.docIdAndVersion().docId);
                for (IndexableField field : document) {
                    assertEquals(document.get(field.name()), restoredDocument.get(field.name()));
                }
            }
            IOUtils.close(original, restored);
        }

        closeShards(shard, restoredShard, targetShard);
    }

    public IndexShard reindex(DirectoryReader reader, MappingMetadata mapping) throws IOException {
        ShardRouting targetShardRouting = shardRoutingBuilder(
            new ShardId("target", "_na_", 0),
            randomAlphaOfLength(10),
            true,
            ShardRoutingState.INITIALIZING
        ).withRecoverySource(RecoverySource.EmptyStoreRecoverySource.INSTANCE).build();
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        IndexMetadata.Builder metadata = IndexMetadata.builder(targetShardRouting.getIndexName())
            .settings(settings)
            .primaryTerm(0, primaryTerm);
        metadata.putMapping(mapping);
        IndexShard targetShard = newShard(targetShardRouting, metadata.build(), null, new InternalEngineFactory());
        boolean success = false;
        try {
            recoverShardFromStore(targetShard);
            String index = targetShard.shardId().getIndexName();
            FieldsVisitor rootFieldsVisitor = new FieldsVisitor(true);
            for (LeafReaderContext ctx : reader.leaves()) {
                LeafReader leafReader = ctx.reader();
                Bits liveDocs = leafReader.getLiveDocs();
                for (int i = 0; i < leafReader.maxDoc(); i++) {
                    if (liveDocs == null || liveDocs.get(i)) {
                        rootFieldsVisitor.reset();
                        leafReader.storedFields().document(i, rootFieldsVisitor);
                        rootFieldsVisitor.postProcess(targetShard.mapperService()::fieldType);
                        String id = rootFieldsVisitor.id();
                        BytesReference source = rootFieldsVisitor.source();
                        assert source != null : "_source is null but should have been filtered out at snapshot time";
                        Engine.Result result = targetShard.applyIndexOperationOnPrimary(
                            Versions.MATCH_ANY,
                            VersionType.INTERNAL,
                            new SourceToParse(id, source, XContentHelper.xContentType(source), rootFieldsVisitor.routing()),
                            SequenceNumbers.UNASSIGNED_SEQ_NO,
                            0,
                            IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP,
                            false
                        );
                        if (result.getResultType() != Engine.Result.Type.SUCCESS) {
                            throw new IllegalStateException(
                                "failed applying post restore operation result: " + result.getResultType(),
                                result.getFailure()
                            );
                        }
                    }
                }
            }
            targetShard.refresh("test");
            success = true;
        } finally {
            if (success == false) {
                closeShards(targetShard);
            }
        }
        return targetShard;
    }

    /** Create a {@link Environment} with random path.home and path.repo **/
    private Environment createEnvironment() {
        Path home = createTempDir();
        return TestEnvironment.newEnvironment(
            Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), home.toAbsolutePath())
                .put(Environment.PATH_REPO_SETTING.getKey(), home.resolve("repo").toAbsolutePath())
                .build()
        );
    }

    /** Create a {@link Repository} with a random name **/
    private Repository createRepository() {
        Settings settings = Settings.builder().put("location", randomAlphaOfLength(10)).build();
        RepositoryMetadata repositoryMetadata = new RepositoryMetadata(randomAlphaOfLength(10), FsRepository.TYPE, settings);
        final ClusterService clusterService = BlobStoreTestUtil.mockClusterService(repositoryMetadata);
        final Repository repository = new FsRepository(
            repositoryMetadata,
            createEnvironment(),
            xContentRegistry(),
            clusterService,
            MockBigArrays.NON_RECYCLING_INSTANCE,
            new RecoverySettings(settings, new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))
        );
        clusterService.addStateApplier(e -> repository.updateState(e.state()));
        // Apply state once to initialize repo properly like RepositoriesService would
        repository.updateState(clusterService.state());
        return repository;
    }

    private static void runAsSnapshot(ThreadPool pool, Runnable runnable) {
        runAsSnapshot(pool, (Callable<Void>) () -> {
            runnable.run();
            return null;
        });
    }

    private static <T> T runAsSnapshot(ThreadPool pool, Callable<T> runnable) {
        PlainActionFuture<T> future = new PlainActionFuture<>();
        pool.executor(ThreadPool.Names.SNAPSHOT).execute(() -> {
            try {
                future.onResponse(runnable.call());
            } catch (Exception e) {
                future.onFailure(e);
            }
        });
        try {
            return future.get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof Exception) {
                throw ExceptionsHelper.convertToRuntime((Exception) e.getCause());
            } else {
                throw new AssertionError(e.getCause());
            }
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
    }
}
