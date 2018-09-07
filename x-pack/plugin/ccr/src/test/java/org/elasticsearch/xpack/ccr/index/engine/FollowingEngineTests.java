/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.index.engine;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.elasticsearch.Version;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineTestCase;
import org.elasticsearch.index.engine.TranslogHandler;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.DirectoryService;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogConfig;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.DummyShardLock;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;

public class FollowingEngineTests extends ESTestCase {

    private ThreadPool threadPool;
    private Index index;
    private ShardId shardId;
    private AtomicLong primaryTerm = new AtomicLong();

    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("following-engine-tests");
        index = new Index("index", "uuid");
        shardId = new ShardId(index, 0);
        primaryTerm.set(randomLongBetween(1, Long.MAX_VALUE));
    }

    public void tearDown() throws Exception {
        terminate(threadPool);
        super.tearDown();
    }

    public void testFollowingEngineRejectsNonFollowingIndex() throws IOException {
        final Settings.Builder builder =
                Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put("index.version.created", Version.CURRENT);
        if (randomBoolean()) {
            builder.put("index.xpack.ccr.following_index", false);
        }
        final Settings settings = builder.build();
        final IndexMetaData indexMetaData = IndexMetaData.builder(index.getName()).settings(settings).build();
        final IndexSettings indexSettings = new IndexSettings(indexMetaData, settings);
        try (Store store = createStore(shardId, indexSettings, newDirectory())) {
            final EngineConfig engineConfig = engineConfig(shardId, indexSettings, threadPool, store, logger, xContentRegistry());
            final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new FollowingEngine(engineConfig));
            assertThat(e, hasToString(containsString("a following engine can not be constructed for a non-following index")));
        }
    }

    public void testIndexSeqNoIsMaintained() throws IOException {
        final long seqNo = randomIntBetween(0, Integer.MAX_VALUE);
        runIndexTest(
                seqNo,
                Engine.Operation.Origin.PRIMARY,
                (followingEngine, index) -> {
                    final Engine.IndexResult result = followingEngine.index(index);
                    assertThat(result.getSeqNo(), equalTo(seqNo));
                });
    }

    /*
     * A following engine (whether or not it is an engine for a primary or replica shard) needs to maintain ordering semantics as the
     * operations presented to it can arrive out of order (while a leader engine that is for a primary shard dictates the order). This test
     * ensures that these semantics are maintained.
     */
    public void testOutOfOrderDocuments() throws IOException {
        final Settings settings =
                Settings.builder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                        .put("index.version.created", Version.CURRENT)
                        .put("index.xpack.ccr.following_index", true)
                        .build();
        final IndexMetaData indexMetaData = IndexMetaData.builder(index.getName()).settings(settings).build();
        final IndexSettings indexSettings = new IndexSettings(indexMetaData, settings);
        try (Store store = createStore(shardId, indexSettings, newDirectory())) {
            final EngineConfig engineConfig = engineConfig(shardId, indexSettings, threadPool, store, logger, xContentRegistry());
            try (FollowingEngine followingEngine = createEngine(store, engineConfig)) {
                final VersionType versionType =
                        randomFrom(VersionType.INTERNAL, VersionType.EXTERNAL, VersionType.EXTERNAL_GTE, VersionType.FORCE);
                final List<Engine.Operation> ops = EngineTestCase.generateSingleDocHistory(true, versionType, 2, 2, 20, "id");
                EngineTestCase.assertOpsOnReplica(ops, followingEngine, true, logger);
            }
        }
    }

    public void runIndexTest(
            final long seqNo,
            final Engine.Operation.Origin origin,
            final CheckedBiConsumer<FollowingEngine, Engine.Index, IOException> consumer) throws IOException {
        final Settings settings =
                Settings.builder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                        .put("index.version.created", Version.CURRENT)
                        .put("index.xpack.ccr.following_index", true)
                        .build();
        final IndexMetaData indexMetaData = IndexMetaData.builder(index.getName()).settings(settings).build();
        final IndexSettings indexSettings = new IndexSettings(indexMetaData, settings);
        try (Store store = createStore(shardId, indexSettings, newDirectory())) {
            final EngineConfig engineConfig = engineConfig(shardId, indexSettings, threadPool, store, logger, xContentRegistry());
            try (FollowingEngine followingEngine = createEngine(store, engineConfig)) {
                final Engine.Index index = createIndexOp("id", seqNo, origin);
                consumer.accept(followingEngine, index);
            }
        }
    }

    public void testDeleteSeqNoIsMaintained() throws IOException {
        final long seqNo = randomIntBetween(0, Integer.MAX_VALUE);
        runDeleteTest(
                seqNo,
                Engine.Operation.Origin.PRIMARY,
                (followingEngine, delete) -> {
                    final Engine.DeleteResult result = followingEngine.delete(delete);
                    assertThat(result.getSeqNo(), equalTo(seqNo));
                });
    }

    public void runDeleteTest(
            final long seqNo,
            final Engine.Operation.Origin origin,
            final CheckedBiConsumer<FollowingEngine, Engine.Delete, IOException> consumer) throws IOException {
        final Settings settings =
                Settings.builder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                        .put("index.version.created", Version.CURRENT)
                        .put("index.xpack.ccr.following_index", true)
                        .build();
        final IndexMetaData indexMetaData = IndexMetaData.builder(index.getName()).settings(settings).build();
        final IndexSettings indexSettings = new IndexSettings(indexMetaData, settings);
        try (Store store = createStore(shardId, indexSettings, newDirectory())) {
            final EngineConfig engineConfig = engineConfig(shardId, indexSettings, threadPool, store, logger, xContentRegistry());
            try (FollowingEngine followingEngine = createEngine(store, engineConfig)) {
                final String id = "id";
                final Engine.Delete delete = new Engine.Delete(
                        "type",
                        id,
                        new Term("_id", id),
                        seqNo,
                        primaryTerm.get(),
                        randomNonNegativeLong(),
                        VersionType.EXTERNAL,
                        origin,
                        System.currentTimeMillis());

                consumer.accept(followingEngine, delete);
            }
        }
    }

    public void testDoNotFillSeqNoGaps() throws Exception {
        final Settings settings =
            Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put("index.version.created", Version.CURRENT)
                .put("index.xpack.ccr.following_index", true)
                .build();
        final IndexMetaData indexMetaData = IndexMetaData.builder(index.getName()).settings(settings).build();
        final IndexSettings indexSettings = new IndexSettings(indexMetaData, settings);
        try (Store store = createStore(shardId, indexSettings, newDirectory())) {
            final EngineConfig engineConfig = engineConfig(shardId, indexSettings, threadPool, store, logger, xContentRegistry());
            try (FollowingEngine followingEngine = createEngine(store, engineConfig)) {
                followingEngine.index(createIndexOp("id", 128, Engine.Operation.Origin.PRIMARY));
                int addedNoops = followingEngine.fillSeqNoGaps(primaryTerm.get());
                assertThat(addedNoops, equalTo(0));
            }
        }
    }

    private EngineConfig engineConfig(
            final ShardId shardId,
            final IndexSettings indexSettings,
            final ThreadPool threadPool,
            final Store store,
            final Logger logger,
            final NamedXContentRegistry xContentRegistry) throws IOException {
        final IndexWriterConfig indexWriterConfig = newIndexWriterConfig();
        final Path translogPath = createTempDir("translog");
        final TranslogConfig translogConfig = new TranslogConfig(shardId, translogPath, indexSettings, BigArrays.NON_RECYCLING_INSTANCE);
        return new EngineConfig(
                shardId,
                "allocation-id",
                threadPool,
                indexSettings,
                null,
                store,
                newMergePolicy(),
                indexWriterConfig.getAnalyzer(),
                indexWriterConfig.getSimilarity(),
                new CodecService(null, logger),
                new Engine.EventListener() {
                    @Override
                    public void onFailedEngine(String reason, Exception e) {

                    }
                },
                IndexSearcher.getDefaultQueryCache(),
                IndexSearcher.getDefaultQueryCachingPolicy(),
                translogConfig,
                TimeValue.timeValueMinutes(5),
                Collections.emptyList(),
                Collections.emptyList(),
                null,
                new NoneCircuitBreakerService(),
                () -> SequenceNumbers.NO_OPS_PERFORMED,
                () -> primaryTerm.get(),
                EngineTestCase.tombstoneDocSupplier()
        );
    }

    private static Store createStore(
            final ShardId shardId, final IndexSettings indexSettings, final Directory directory) throws IOException {
        final DirectoryService directoryService = new DirectoryService(shardId, indexSettings) {
            @Override
            public Directory newDirectory() throws IOException {
                return directory;
            }
        };
        return new Store(shardId, indexSettings, directoryService, new DummyShardLock(shardId));
    }

    private FollowingEngine createEngine(Store store, EngineConfig config) throws IOException {
        store.createEmpty();
        final String translogUuid = Translog.createEmptyTranslog(config.getTranslogConfig().getTranslogPath(),
                SequenceNumbers.NO_OPS_PERFORMED, shardId, 1L);
        store.associateIndexWithNewTranslog(translogUuid);
        FollowingEngine followingEngine = new FollowingEngine(config);
        TranslogHandler translogHandler = new TranslogHandler(xContentRegistry(), config.getIndexSettings());
        followingEngine.recoverFromTranslog(translogHandler, Long.MAX_VALUE);
        return followingEngine;
    }

    private Engine.Index createIndexOp(String id, long seqNo, Engine.Operation.Origin origin) {
        final Field uidField = new Field("_id", id, IdFieldMapper.Defaults.FIELD_TYPE);
        final String type = "type";
        final Field versionField = new NumericDocValuesField("_version", 0);
        final SeqNoFieldMapper.SequenceIDFields seqID = SeqNoFieldMapper.SequenceIDFields.emptySeqID();
        final ParseContext.Document document = new ParseContext.Document();
        document.add(uidField);
        document.add(versionField);
        document.add(seqID.seqNo);
        document.add(seqID.seqNoDocValue);
        document.add(seqID.primaryTerm);
        final BytesReference source = new BytesArray(new byte[]{1});
        final ParsedDocument parsedDocument = new ParsedDocument(
            versionField,
            seqID,
            id,
            type,
            "routing",
            Collections.singletonList(document),
            source,
            XContentType.JSON,
            null);

        final long version;
        final long autoGeneratedIdTimestamp;
        if (randomBoolean()) {
            version = 1;
            autoGeneratedIdTimestamp = System.currentTimeMillis();
        } else {
            version = randomNonNegativeLong();
            autoGeneratedIdTimestamp = IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP;
        }
        return new Engine.Index(
            new Term("_id", parsedDocument.id()),
            parsedDocument,
            seqNo,
            primaryTerm.get(),
            version,
            VersionType.EXTERNAL,
            origin,
            System.currentTimeMillis(),
            autoGeneratedIdTimestamp,
            randomBoolean());
    }

}
