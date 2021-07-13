/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.engine;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterCodecReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LazySoftDeletesDirectoryReaderWrapper;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.LiveIndexWriterConfig;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.Version;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MapperTestUtils;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.VersionFieldMapper;
import org.elasticsearch.index.seqno.LocalCheckpointTracker;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.index.seqno.RetentionLeases;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.SearcherHelper;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogConfig;
import org.elasticsearch.index.translog.TranslogDeletionPolicy;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.DummyShardLock;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.function.ToLongBiFunction;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.shuffle;
import static org.elasticsearch.index.engine.Engine.Operation.Origin.PEER_RECOVERY;
import static org.elasticsearch.index.engine.Engine.Operation.Origin.PRIMARY;
import static org.elasticsearch.index.engine.Engine.Operation.Origin.REPLICA;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

public abstract class EngineTestCase extends ESTestCase {

    protected final ShardId shardId = new ShardId(new Index("index", "_na_"), 0);
    protected final AllocationId allocationId = AllocationId.newInitializing();
    protected static final IndexSettings INDEX_SETTINGS = IndexSettingsModule.newIndexSettings("index", Settings.EMPTY);

    protected ThreadPool threadPool;
    protected TranslogHandler translogHandler;

    protected Store store;
    protected Store storeReplica;

    protected InternalEngine engine;
    protected InternalEngine replicaEngine;

    protected IndexSettings defaultSettings;
    protected String codecName;
    protected Path primaryTranslogDir;
    protected Path replicaTranslogDir;
    // A default primary term is used by engine instances created in this test.
    protected final PrimaryTermSupplier primaryTerm = new PrimaryTermSupplier(1L);

    protected static void assertVisibleCount(Engine engine, int numDocs) throws IOException {
        assertVisibleCount(engine, numDocs, true);
    }

    protected static void assertVisibleCount(Engine engine, int numDocs, boolean refresh) throws IOException {
        if (refresh) {
            engine.refresh("test");
        }
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            final TotalHitCountCollector collector = new TotalHitCountCollector();
            searcher.search(new MatchAllDocsQuery(), collector);
            assertThat(collector.getTotalHits(), equalTo(numDocs));
        }
    }

    protected Settings indexSettings() {
        // TODO randomize more settings
        return Settings.builder()
            .put(IndexSettings.INDEX_GC_DELETES_SETTING.getKey(), "1h") // make sure this doesn't kick in on us
            .put(EngineConfig.INDEX_CODEC_SETTING.getKey(), codecName)
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexSettings.MAX_REFRESH_LISTENERS_PER_SHARD.getKey(),
                between(10, 10 * IndexSettings.MAX_REFRESH_LISTENERS_PER_SHARD.get(Settings.EMPTY)))
            .put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING.getKey(), between(0, 1000))
            .build();
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        primaryTerm.set(randomLongBetween(1, Long.MAX_VALUE));
        CodecService codecService = new CodecService(null, logger);
        String name = Codec.getDefault().getName();
        if (Arrays.asList(codecService.availableCodecs()).contains(name)) {
            // some codecs are read only so we only take the ones that we have in the service and randomly
            // selected by lucene test case.
            codecName = name;
        } else {
            codecName = "default";
        }
        defaultSettings = IndexSettingsModule.newIndexSettings("test", indexSettings());
        threadPool = new TestThreadPool(getClass().getName());
        store = createStore();
        storeReplica = createStore();
        Lucene.cleanLuceneIndex(store.directory());
        Lucene.cleanLuceneIndex(storeReplica.directory());
        primaryTranslogDir = createTempDir("translog-primary");
        translogHandler = createTranslogHandler(defaultSettings);
        engine = createEngine(store, primaryTranslogDir);
        LiveIndexWriterConfig currentIndexWriterConfig = engine.getCurrentIndexWriterConfig();

        assertEquals(engine.config().getCodec().getName(), codecService.codec(codecName).getName());
        assertEquals(currentIndexWriterConfig.getCodec().getName(), codecService.codec(codecName).getName());
        if (randomBoolean()) {
            engine.config().setEnableGcDeletes(false);
        }
        replicaTranslogDir = createTempDir("translog-replica");
        replicaEngine = createEngine(storeReplica, replicaTranslogDir);
        currentIndexWriterConfig = replicaEngine.getCurrentIndexWriterConfig();

        assertEquals(replicaEngine.config().getCodec().getName(), codecService.codec(codecName).getName());
        assertEquals(currentIndexWriterConfig.getCodec().getName(), codecService.codec(codecName).getName());
        if (randomBoolean()) {
            engine.config().setEnableGcDeletes(false);
        }
    }

    public EngineConfig copy(EngineConfig config, LongSupplier globalCheckpointSupplier) {
        return new EngineConfig(config.getShardId(), config.getThreadPool(), config.getIndexSettings(),
            config.getWarmer(), config.getStore(), config.getMergePolicy(), config.getAnalyzer(), config.getSimilarity(),
            new CodecService(null, logger), config.getEventListener(), config.getQueryCache(), config.getQueryCachingPolicy(),
            config.getTranslogConfig(), config.getFlushMergesAfter(),
            config.getExternalRefreshListener(), Collections.emptyList(), config.getIndexSort(),
            config.getCircuitBreakerService(), globalCheckpointSupplier, config.retentionLeasesSupplier(),
                config.getPrimaryTermSupplier(), config.getSnapshotCommitSupplier());
    }

    public EngineConfig copy(EngineConfig config, Analyzer analyzer) {
        return new EngineConfig(config.getShardId(), config.getThreadPool(), config.getIndexSettings(),
                config.getWarmer(), config.getStore(), config.getMergePolicy(), analyzer, config.getSimilarity(),
                new CodecService(null, logger), config.getEventListener(), config.getQueryCache(), config.getQueryCachingPolicy(),
                config.getTranslogConfig(), config.getFlushMergesAfter(),
                config.getExternalRefreshListener(), Collections.emptyList(), config.getIndexSort(),
                config.getCircuitBreakerService(), config.getGlobalCheckpointSupplier(), config.retentionLeasesSupplier(),
                config.getPrimaryTermSupplier(), config.getSnapshotCommitSupplier());
    }

    public EngineConfig copy(EngineConfig config, MergePolicy mergePolicy) {
        return new EngineConfig(config.getShardId(), config.getThreadPool(), config.getIndexSettings(),
            config.getWarmer(), config.getStore(), mergePolicy, config.getAnalyzer(), config.getSimilarity(),
            new CodecService(null, logger), config.getEventListener(), config.getQueryCache(), config.getQueryCachingPolicy(),
            config.getTranslogConfig(), config.getFlushMergesAfter(),
            config.getExternalRefreshListener(), Collections.emptyList(), config.getIndexSort(),
            config.getCircuitBreakerService(), config.getGlobalCheckpointSupplier(), config.retentionLeasesSupplier(),
                config.getPrimaryTermSupplier(), config.getSnapshotCommitSupplier());
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        try {
            if (engine != null && engine.isClosed.get() == false) {
                engine.getTranslog().getDeletionPolicy().assertNoOpenTranslogRefs();
                assertNoInFlightDocuments(engine);
                assertConsistentHistoryBetweenTranslogAndLuceneIndex(engine);
                assertMaxSeqNoInCommitUserData(engine);
                assertAtMostOneLuceneDocumentPerSequenceNumber(engine);
            }
            if (replicaEngine != null && replicaEngine.isClosed.get() == false) {
                replicaEngine.getTranslog().getDeletionPolicy().assertNoOpenTranslogRefs();
                assertNoInFlightDocuments(replicaEngine);
                assertConsistentHistoryBetweenTranslogAndLuceneIndex(replicaEngine);
                assertMaxSeqNoInCommitUserData(replicaEngine);
                assertAtMostOneLuceneDocumentPerSequenceNumber(replicaEngine);
            }
        } finally {
            IOUtils.close(replicaEngine, storeReplica, engine, store, () -> terminate(threadPool));
        }
    }


    protected static LuceneDocument testDocumentWithTextField() {
        return testDocumentWithTextField("test");
    }

    protected static LuceneDocument testDocumentWithTextField(String value) {
        LuceneDocument document = testDocument();
        document.add(new TextField("value", value, Field.Store.YES));
        return document;
    }


    protected static LuceneDocument testDocument() {
        return new LuceneDocument();
    }

    public static ParsedDocument createParsedDoc(String id, String routing) {
        return testParsedDocument(id, routing, testDocumentWithTextField(), new BytesArray("{ \"value\" : \"test\" }"), null);
    }

    public static ParsedDocument createParsedDoc(String id, String routing, boolean recoverySource) {
        return testParsedDocument(id, routing, testDocumentWithTextField(), new BytesArray("{ \"value\" : \"test\" }"), null,
            recoverySource);
    }

    protected static ParsedDocument testParsedDocument(
            String id, String routing, LuceneDocument document, BytesReference source, Mapping mappingUpdate) {
        return testParsedDocument(id, routing, document, source, mappingUpdate, false);
    }
    protected static ParsedDocument testParsedDocument(
            String id, String routing, LuceneDocument document, BytesReference source, Mapping mappingUpdate,
            boolean recoverySource) {
        Field uidField = new Field("_id", Uid.encodeId(id), IdFieldMapper.Defaults.FIELD_TYPE);
        Field versionField = new NumericDocValuesField("_version", 0);
        SeqNoFieldMapper.SequenceIDFields seqID = SeqNoFieldMapper.SequenceIDFields.emptySeqID();
        document.add(uidField);
        document.add(versionField);
        document.add(seqID.seqNo);
        document.add(seqID.seqNoDocValue);
        document.add(seqID.primaryTerm);
        BytesRef ref = source.toBytesRef();
        if (recoverySource) {
            document.add(new StoredField(SourceFieldMapper.RECOVERY_SOURCE_NAME, ref.bytes, ref.offset, ref.length));
            document.add(new NumericDocValuesField(SourceFieldMapper.RECOVERY_SOURCE_NAME, 1));
        } else {
            document.add(new StoredField(SourceFieldMapper.NAME, ref.bytes, ref.offset, ref.length));
        }
        return new ParsedDocument(versionField, seqID, id, routing, Arrays.asList(document), source, XContentType.JSON,
                mappingUpdate);
    }

    public static CheckedBiFunction<String, Integer, ParsedDocument, IOException> nestedParsedDocFactory() throws Exception {
        final MapperService mapperService = createMapperService();
        final String nestedMapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("nested_field").field("type", "nested").endObject().endObject()
            .endObject().endObject());
        final DocumentMapper nestedMapper = mapperService.merge("type", new CompressedXContent(nestedMapping),
            MapperService.MergeReason.MAPPING_UPDATE);
        return (docId, nestedFieldValues) -> {
            final XContentBuilder source = XContentFactory.jsonBuilder().startObject().field("field", "value");
            if (nestedFieldValues > 0) {
                XContentBuilder nestedField = source.startObject("nested_field");
                for (int i = 0; i < nestedFieldValues; i++) {
                    nestedField.field("field-" + i, "value-" + i);
                }
                source.endObject();
            }
            source.endObject();
            return nestedMapper.parse(new SourceToParse("test", docId, BytesReference.bytes(source), XContentType.JSON));
        };
    }

    protected Store createStore() throws IOException {
        return createStore(newDirectory());
    }

    protected Store createStore(final Directory directory) throws IOException {
        return createStore(INDEX_SETTINGS, directory);
    }

    protected Store createStore(final IndexSettings indexSettings, final Directory directory) throws IOException {
        return new Store(shardId, indexSettings, directory, new DummyShardLock(shardId));
    }

    protected Translog createTranslog(LongSupplier primaryTermSupplier) throws IOException {
        return createTranslog(primaryTranslogDir, primaryTermSupplier);
    }

    protected Translog createTranslog(Path translogPath, LongSupplier primaryTermSupplier) throws IOException {
        TranslogConfig translogConfig = new TranslogConfig(shardId, translogPath, INDEX_SETTINGS, BigArrays.NON_RECYCLING_INSTANCE);
        String translogUUID = Translog.createEmptyTranslog(translogPath, SequenceNumbers.NO_OPS_PERFORMED, shardId,
            primaryTermSupplier.getAsLong());
        return new Translog(translogConfig, translogUUID, new TranslogDeletionPolicy(),
            () -> SequenceNumbers.NO_OPS_PERFORMED, primaryTermSupplier, seqNo -> {});
    }

    protected TranslogHandler createTranslogHandler(IndexSettings indexSettings) {
        return new TranslogHandler(xContentRegistry(), indexSettings);
    }

    protected InternalEngine createEngine(Store store, Path translogPath) throws IOException {
        return createEngine(defaultSettings, store, translogPath, newMergePolicy(), null);
    }

    protected InternalEngine createEngine(Store store, Path translogPath, LongSupplier globalCheckpointSupplier) throws IOException {
        return createEngine(defaultSettings, store, translogPath, newMergePolicy(), null, null, globalCheckpointSupplier);
    }

    protected InternalEngine createEngine(
            Store store,
            Path translogPath,
            BiFunction<Long, Long, LocalCheckpointTracker> localCheckpointTrackerSupplier) throws IOException {
        return createEngine(defaultSettings, store, translogPath, newMergePolicy(), null, localCheckpointTrackerSupplier, null);
    }

    protected InternalEngine createEngine(
            Store store,
            Path translogPath,
            BiFunction<Long, Long, LocalCheckpointTracker> localCheckpointTrackerSupplier,
            ToLongBiFunction<Engine, Engine.Operation> seqNoForOperation) throws IOException {
        return createEngine(
                defaultSettings, store, translogPath, newMergePolicy(), null, localCheckpointTrackerSupplier, null, seqNoForOperation);
    }

    protected InternalEngine createEngine(
            IndexSettings indexSettings, Store store, Path translogPath, MergePolicy mergePolicy) throws IOException {
        return createEngine(indexSettings, store, translogPath, mergePolicy, null);

    }

    protected InternalEngine createEngine(IndexSettings indexSettings, Store store, Path translogPath, MergePolicy mergePolicy,
                                          @Nullable IndexWriterFactory indexWriterFactory) throws IOException {
        return createEngine(indexSettings, store, translogPath, mergePolicy, indexWriterFactory, null, null);
    }

    protected InternalEngine createEngine(
            IndexSettings indexSettings,
            Store store,
            Path translogPath,
            MergePolicy mergePolicy,
            @Nullable IndexWriterFactory indexWriterFactory,
            @Nullable BiFunction<Long, Long, LocalCheckpointTracker> localCheckpointTrackerSupplier,
            @Nullable LongSupplier globalCheckpointSupplier) throws IOException {
        return createEngine(
                indexSettings, store, translogPath, mergePolicy, indexWriterFactory, localCheckpointTrackerSupplier, null, null,
                globalCheckpointSupplier);
    }

    protected InternalEngine createEngine(
            IndexSettings indexSettings,
            Store store,
            Path translogPath,
            MergePolicy mergePolicy,
            @Nullable IndexWriterFactory indexWriterFactory,
            @Nullable BiFunction<Long, Long, LocalCheckpointTracker> localCheckpointTrackerSupplier,
            @Nullable LongSupplier globalCheckpointSupplier,
            @Nullable ToLongBiFunction<Engine, Engine.Operation> seqNoForOperation) throws IOException {
        return createEngine(
                indexSettings,
                store,
                translogPath,
                mergePolicy,
                indexWriterFactory,
                localCheckpointTrackerSupplier,
                seqNoForOperation,
                null,
                globalCheckpointSupplier);
    }

    protected InternalEngine createEngine(
            IndexSettings indexSettings,
            Store store,
            Path translogPath,
            MergePolicy mergePolicy,
            @Nullable IndexWriterFactory indexWriterFactory,
            @Nullable BiFunction<Long, Long, LocalCheckpointTracker> localCheckpointTrackerSupplier,
            @Nullable ToLongBiFunction<Engine, Engine.Operation> seqNoForOperation,
            @Nullable Sort indexSort,
            @Nullable LongSupplier globalCheckpointSupplier) throws IOException {
        EngineConfig config = config(indexSettings, store, translogPath, mergePolicy, null, indexSort, globalCheckpointSupplier);
        return createEngine(indexWriterFactory, localCheckpointTrackerSupplier, seqNoForOperation, config);
    }

    protected InternalEngine createEngine(EngineConfig config) throws IOException {
        return createEngine(null, null, null, config);
    }

    protected InternalEngine createEngine(@Nullable IndexWriterFactory indexWriterFactory,
                                          @Nullable BiFunction<Long, Long, LocalCheckpointTracker> localCheckpointTrackerSupplier,
                                          @Nullable ToLongBiFunction<Engine, Engine.Operation> seqNoForOperation,
                                          EngineConfig config) throws IOException {
        final Store store = config.getStore();
        final Directory directory = store.directory();
        if (Lucene.indexExists(directory) == false) {
            store.createEmpty();
            final String translogUuid = Translog.createEmptyTranslog(config.getTranslogConfig().getTranslogPath(),
                SequenceNumbers.NO_OPS_PERFORMED, shardId, primaryTerm.get());
            store.associateIndexWithNewTranslog(translogUuid);

        }
        InternalEngine internalEngine = createInternalEngine(indexWriterFactory, localCheckpointTrackerSupplier, seqNoForOperation, config);
        internalEngine.recoverFromTranslog(translogHandler, Long.MAX_VALUE);
        return internalEngine;
    }

    public static InternalEngine createEngine(EngineConfig engineConfig, int maxDocs) {
        return new InternalEngine(engineConfig, maxDocs, LocalCheckpointTracker::new);
    }

    @FunctionalInterface
    public interface IndexWriterFactory {

        IndexWriter createWriter(Directory directory, IndexWriterConfig iwc) throws IOException;
    }

    /**
     * Generate a new sequence number and return it. Only works on InternalEngines
     */
    public static long generateNewSeqNo(final Engine engine) {
        assert engine instanceof InternalEngine : "expected InternalEngine, got: " + engine.getClass();
        InternalEngine internalEngine = (InternalEngine) engine;
        return internalEngine.getLocalCheckpointTracker().generateSeqNo();
    }

    public static InternalEngine createInternalEngine(
            @Nullable final IndexWriterFactory indexWriterFactory,
            @Nullable final BiFunction<Long, Long, LocalCheckpointTracker> localCheckpointTrackerSupplier,
            @Nullable final ToLongBiFunction<Engine, Engine.Operation> seqNoForOperation,
            final EngineConfig config) {
        if (localCheckpointTrackerSupplier == null) {
            return new InternalTestEngine(config) {
                @Override
                IndexWriter createWriter(Directory directory, IndexWriterConfig iwc) throws IOException {
                    return (indexWriterFactory != null) ?
                            indexWriterFactory.createWriter(directory, iwc) :
                            super.createWriter(directory, iwc);
                }

                @Override
                protected long doGenerateSeqNoForOperation(final Operation operation) {
                    return seqNoForOperation != null
                            ? seqNoForOperation.applyAsLong(this, operation)
                            : super.doGenerateSeqNoForOperation(operation);
                }
            };
        } else {
            return new InternalTestEngine(config, IndexWriter.MAX_DOCS, localCheckpointTrackerSupplier) {
                @Override
                IndexWriter createWriter(Directory directory, IndexWriterConfig iwc) throws IOException {
                    return (indexWriterFactory != null) ?
                            indexWriterFactory.createWriter(directory, iwc) :
                            super.createWriter(directory, iwc);
                }

                @Override
                protected long doGenerateSeqNoForOperation(final Operation operation) {
                    return seqNoForOperation != null
                            ? seqNoForOperation.applyAsLong(this, operation)
                            : super.doGenerateSeqNoForOperation(operation);
                }
            };
        }

    }

    public EngineConfig config(IndexSettings indexSettings, Store store, Path translogPath, MergePolicy mergePolicy,
                               ReferenceManager.RefreshListener refreshListener) {
        return config(indexSettings, store, translogPath, mergePolicy, refreshListener, null, () -> SequenceNumbers.NO_OPS_PERFORMED);
    }

    public EngineConfig config(IndexSettings indexSettings, Store store, Path translogPath, MergePolicy mergePolicy,
                               ReferenceManager.RefreshListener refreshListener, Sort indexSort, LongSupplier globalCheckpointSupplier) {
        return config(
                indexSettings,
                store,
                translogPath,
                mergePolicy,
                refreshListener,
                indexSort,
                globalCheckpointSupplier,
                globalCheckpointSupplier == null ? null : () -> RetentionLeases.EMPTY);
    }

    public EngineConfig config(
            final IndexSettings indexSettings,
            final Store store,
            final Path translogPath,
            final MergePolicy mergePolicy,
            final ReferenceManager.RefreshListener refreshListener,
            final Sort indexSort,
            final LongSupplier globalCheckpointSupplier,
            final Supplier<RetentionLeases> retentionLeasesSupplier) {
        return config(
                indexSettings,
                store,
                translogPath,
                mergePolicy,
                refreshListener,
                null,
                indexSort,
                globalCheckpointSupplier,
                retentionLeasesSupplier,
                new NoneCircuitBreakerService());
    }

    public EngineConfig config(IndexSettings indexSettings, Store store, Path translogPath, MergePolicy mergePolicy,
                               ReferenceManager.RefreshListener externalRefreshListener,
                               ReferenceManager.RefreshListener internalRefreshListener,
                               Sort indexSort, @Nullable LongSupplier maybeGlobalCheckpointSupplier,
                               CircuitBreakerService breakerService) {
        return config(
                indexSettings,
                store,
                translogPath,
                mergePolicy,
                externalRefreshListener,
                internalRefreshListener,
                indexSort,
                maybeGlobalCheckpointSupplier,
                maybeGlobalCheckpointSupplier == null ? null : () -> RetentionLeases.EMPTY,
                breakerService);
    }

    public EngineConfig config(
            final IndexSettings indexSettings,
            final Store store,
            final Path translogPath,
            final MergePolicy mergePolicy,
            final ReferenceManager.RefreshListener externalRefreshListener,
            final ReferenceManager.RefreshListener internalRefreshListener,
            final Sort indexSort,
            final @Nullable LongSupplier maybeGlobalCheckpointSupplier,
            final @Nullable Supplier<RetentionLeases> maybeRetentionLeasesSupplier,
            final CircuitBreakerService breakerService) {
        final IndexWriterConfig iwc = newIndexWriterConfig();
        final TranslogConfig translogConfig = new TranslogConfig(shardId, translogPath, indexSettings, BigArrays.NON_RECYCLING_INSTANCE);
        final Engine.EventListener eventListener = new Engine.EventListener() {}; // we don't need to notify anybody in this test
        final List<ReferenceManager.RefreshListener> extRefreshListenerList =
                externalRefreshListener == null ? emptyList() : Collections.singletonList(externalRefreshListener);
        final List<ReferenceManager.RefreshListener> intRefreshListenerList =
                internalRefreshListener == null ? emptyList() : Collections.singletonList(internalRefreshListener);
        final LongSupplier globalCheckpointSupplier;
        final Supplier<RetentionLeases> retentionLeasesSupplier;
        if (maybeGlobalCheckpointSupplier == null) {
            assert maybeRetentionLeasesSupplier == null;
            final ReplicationTracker replicationTracker = new ReplicationTracker(
                    shardId,
                    allocationId.getId(),
                    indexSettings,
                    randomNonNegativeLong(),
                    SequenceNumbers.NO_OPS_PERFORMED,
                    update -> {},
                    () -> 0L,
                    (leases, listener) -> listener.onResponse(new ReplicationResponse()),
                    () -> SafeCommitInfo.EMPTY);
            globalCheckpointSupplier = replicationTracker;
            retentionLeasesSupplier = replicationTracker::getRetentionLeases;
        } else {
            assert maybeRetentionLeasesSupplier != null;
            globalCheckpointSupplier = maybeGlobalCheckpointSupplier;
            retentionLeasesSupplier = maybeRetentionLeasesSupplier;
        }
        return new EngineConfig(
                shardId,
                threadPool,
                indexSettings,
                null,
                store,
                mergePolicy,
                iwc.getAnalyzer(),
                iwc.getSimilarity(),
                new CodecService(null, logger),
                eventListener,
                IndexSearcher.getDefaultQueryCache(),
                IndexSearcher.getDefaultQueryCachingPolicy(),
                translogConfig,
                TimeValue.timeValueMinutes(5),
                extRefreshListenerList,
                intRefreshListenerList,
                indexSort,
                breakerService,
                globalCheckpointSupplier,
                retentionLeasesSupplier,
                primaryTerm,
                IndexModule.DEFAULT_SNAPSHOT_COMMIT_SUPPLIER);
    }

    protected EngineConfig config(EngineConfig config, Store store, Path translogPath) {
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test",
            Settings.builder().put(config.getIndexSettings().getSettings())
                .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true).build());
        TranslogConfig translogConfig = new TranslogConfig(shardId, translogPath, indexSettings, BigArrays.NON_RECYCLING_INSTANCE);
        return new EngineConfig(config.getShardId(), config.getThreadPool(),
            indexSettings, config.getWarmer(), store, config.getMergePolicy(), config.getAnalyzer(), config.getSimilarity(),
            new CodecService(null, logger), config.getEventListener(), config.getQueryCache(), config.getQueryCachingPolicy(),
            translogConfig, config.getFlushMergesAfter(), config.getExternalRefreshListener(),
            config.getInternalRefreshListener(), config.getIndexSort(), config.getCircuitBreakerService(),
            config.getGlobalCheckpointSupplier(), config.retentionLeasesSupplier(),
            config.getPrimaryTermSupplier(), config.getSnapshotCommitSupplier());
    }

    protected EngineConfig noOpConfig(IndexSettings indexSettings, Store store, Path translogPath) {
        return noOpConfig(indexSettings, store, translogPath, null);
    }

    protected EngineConfig noOpConfig(IndexSettings indexSettings, Store store, Path translogPath, LongSupplier globalCheckpointSupplier) {
        return config(indexSettings, store, translogPath, newMergePolicy(), null, null, globalCheckpointSupplier);
    }

    protected static final BytesReference B_1 = new BytesArray(new byte[]{1});
    protected static final BytesReference B_2 = new BytesArray(new byte[]{2});
    protected static final BytesReference B_3 = new BytesArray(new byte[]{3});
    protected static final BytesArray SOURCE = bytesArray("{}");

    protected static BytesArray bytesArray(String string) {
        return new BytesArray(string.getBytes(Charset.defaultCharset()));
    }

    public static Term newUid(String id) {
        return new Term("_id", Uid.encodeId(id));
    }

    public static Term newUid(ParsedDocument doc) {
        return newUid(doc.id());
    }

    protected Engine.Get newGet(boolean realtime, ParsedDocument doc) {
        return new Engine.Get(realtime, realtime, doc.id());
    }

    protected Engine.Index indexForDoc(ParsedDocument doc) {
        return new Engine.Index(newUid(doc), primaryTerm.get(), doc);
    }

    protected Engine.Index replicaIndexForDoc(ParsedDocument doc, long version, long seqNo,
                                            boolean isRetry) {
        return new Engine.Index(newUid(doc), doc, seqNo, primaryTerm.get(), version, null, Engine.Operation.Origin.REPLICA,
            System.nanoTime(), IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, isRetry, SequenceNumbers.UNASSIGNED_SEQ_NO, 0);
    }

    protected Engine.Delete replicaDeleteForDoc(String id, long version, long seqNo, long startTime) {
        return new Engine.Delete(id, newUid(id), seqNo, 1, version, null, Engine.Operation.Origin.REPLICA, startTime,
            SequenceNumbers.UNASSIGNED_SEQ_NO, 0);
    }
    protected static void assertVisibleCount(InternalEngine engine, int numDocs) throws IOException {
        assertVisibleCount(engine, numDocs, true);
    }

    protected static void assertVisibleCount(InternalEngine engine, int numDocs, boolean refresh) throws IOException {
        if (refresh) {
            engine.refresh("test");
        }
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            final TotalHitCountCollector collector = new TotalHitCountCollector();
            searcher.search(new MatchAllDocsQuery(), collector);
            assertThat(collector.getTotalHits(), equalTo(numDocs));
        }
    }

    public static List<Engine.Operation> generateSingleDocHistory(boolean forReplica, VersionType versionType,
                                                                  long primaryTerm, int minOpCount, int maxOpCount, String docId) {
        final int numOfOps = randomIntBetween(minOpCount, maxOpCount);
        final List<Engine.Operation> ops = new ArrayList<>();
        final Term id = newUid(docId);
        final int startWithSeqNo = 0;
        final String valuePrefix = (forReplica ? "r_" : "p_" ) + docId + "_";
        final boolean incrementTermWhenIntroducingSeqNo = randomBoolean();
        for (int i = 0; i < numOfOps; i++) {
            final Engine.Operation op;
            final long version;
            switch (versionType) {
                case INTERNAL:
                    version = forReplica ? i : Versions.MATCH_ANY;
                    break;
                case EXTERNAL:
                    version = i;
                    break;
                case EXTERNAL_GTE:
                    version = randomBoolean() ? Math.max(i - 1, 0) : i;
                    break;
                default:
                    throw new UnsupportedOperationException("unknown version type: " + versionType);
            }
            if (randomBoolean()) {
                op = new Engine.Index(id, testParsedDocument(docId, null, testDocumentWithTextField(valuePrefix + i), SOURCE, null),
                    forReplica && i >= startWithSeqNo ? i * 2 : SequenceNumbers.UNASSIGNED_SEQ_NO,
                    forReplica && i >= startWithSeqNo && incrementTermWhenIntroducingSeqNo ? primaryTerm + 1 : primaryTerm,
                    version,
                    forReplica ? null : versionType,
                    forReplica ? REPLICA : PRIMARY,
                    System.currentTimeMillis(), -1, false,
                    SequenceNumbers.UNASSIGNED_SEQ_NO, 0);
            } else {
                op = new Engine.Delete(docId, id,
                    forReplica && i >= startWithSeqNo ? i * 2 : SequenceNumbers.UNASSIGNED_SEQ_NO,
                    forReplica && i >= startWithSeqNo && incrementTermWhenIntroducingSeqNo ? primaryTerm + 1 : primaryTerm,
                    version,
                    forReplica ? null : versionType,
                    forReplica ? REPLICA : PRIMARY,
                    System.currentTimeMillis(), SequenceNumbers.UNASSIGNED_SEQ_NO, 0);
            }
            ops.add(op);
        }
        return ops;
    }

    public List<Engine.Operation> generateHistoryOnReplica(int numOps, boolean allowGapInSeqNo, boolean allowDuplicate,
                                                           boolean includeNestedDocs) throws Exception {
        return generateHistoryOnReplica(numOps, 0L, allowGapInSeqNo, allowDuplicate, includeNestedDocs);
    }

    public List<Engine.Operation> generateHistoryOnReplica(int numOps, long startingSeqNo, boolean allowGapInSeqNo, boolean allowDuplicate,
                                                           boolean includeNestedDocs) throws Exception {
        long seqNo = startingSeqNo;
        final int maxIdValue = randomInt(numOps * 2);
        final List<Engine.Operation> operations = new ArrayList<>(numOps);
        CheckedBiFunction<String, Integer, ParsedDocument, IOException> nestedParsedDocFactory = nestedParsedDocFactory();
        for (int i = 0; i < numOps; i++) {
            final String id = Integer.toString(randomInt(maxIdValue));
            final Engine.Operation.TYPE opType = randomFrom(Engine.Operation.TYPE.values());
            final boolean isNestedDoc = includeNestedDocs && opType == Engine.Operation.TYPE.INDEX && randomBoolean();
            final int nestedValues = between(0, 3);
            final long startTime = threadPool.relativeTimeInNanos();
            final int copies = allowDuplicate && rarely() ? between(2, 4) : 1;
            for (int copy = 0; copy < copies; copy++) {
                final ParsedDocument doc = isNestedDoc ? nestedParsedDocFactory.apply(id, nestedValues) : createParsedDoc(id, null);
                switch (opType) {
                    case INDEX:
                        operations.add(new Engine.Index(EngineTestCase.newUid(doc), doc, seqNo, primaryTerm.get(),
                            i, null, randomFrom(REPLICA, PEER_RECOVERY), startTime, -1, true, SequenceNumbers.UNASSIGNED_SEQ_NO, 0));
                        break;
                    case DELETE:
                        operations.add(new Engine.Delete(doc.id(), EngineTestCase.newUid(doc), seqNo, primaryTerm.get(),
                            i, null, randomFrom(REPLICA, PEER_RECOVERY), startTime, SequenceNumbers.UNASSIGNED_SEQ_NO, 0));
                        break;
                    case NO_OP:
                        operations.add(new Engine.NoOp(seqNo, primaryTerm.get(),
                            randomFrom(REPLICA, PEER_RECOVERY), startTime, "test-" + i));
                        break;
                    default:
                        throw new IllegalStateException("Unknown operation type [" + opType + "]");
                }
            }
            seqNo++;
            if (allowGapInSeqNo && rarely()) {
                seqNo++;
            }
        }
        Randomness.shuffle(operations);
        return operations;
    }

    public static void assertOpsOnReplica(
            final List<Engine.Operation> ops,
            final InternalEngine replicaEngine,
            boolean shuffleOps,
            final Logger logger) throws IOException {
        final Engine.Operation lastOp = ops.get(ops.size() - 1);
        final String lastFieldValue;
        if (lastOp instanceof Engine.Index) {
            Engine.Index index = (Engine.Index) lastOp;
            lastFieldValue = index.docs().get(0).get("value");
        } else {
            // delete
            lastFieldValue = null;
        }
        if (shuffleOps) {
            int firstOpWithSeqNo = 0;
            while (firstOpWithSeqNo < ops.size() && ops.get(firstOpWithSeqNo).seqNo() < 0) {
                firstOpWithSeqNo++;
            }
            // shuffle ops but make sure legacy ops are first
            shuffle(ops.subList(0, firstOpWithSeqNo), random());
            shuffle(ops.subList(firstOpWithSeqNo, ops.size()), random());
        }
        boolean firstOp = true;
        for (Engine.Operation op : ops) {
            logger.info("performing [{}], v [{}], seq# [{}], term [{}]",
                    op.operationType().name().charAt(0), op.version(), op.seqNo(), op.primaryTerm());
            if (op instanceof Engine.Index) {
                Engine.IndexResult result = replicaEngine.index((Engine.Index) op);
                // replicas don't really care to about creation status of documents
                // this allows to ignore the case where a document was found in the live version maps in
                // a delete state and return false for the created flag in favor of code simplicity
                // as deleted or not. This check is just signal regression so a decision can be made if it's
                // intentional
                assertThat(result.isCreated(), equalTo(firstOp));
                assertThat(result.getVersion(), equalTo(op.version()));
                assertThat(result.getResultType(), equalTo(Engine.Result.Type.SUCCESS));

            } else {
                Engine.DeleteResult result = replicaEngine.delete((Engine.Delete) op);
                // Replicas don't really care to about found status of documents
                // this allows to ignore the case where a document was found in the live version maps in
                // a delete state and return true for the found flag in favor of code simplicity
                // his check is just signal regression so a decision can be made if it's
                // intentional
                assertThat(result.isFound(), equalTo(firstOp == false));
                assertThat(result.getVersion(), equalTo(op.version()));
                assertThat(result.getResultType(), equalTo(Engine.Result.Type.SUCCESS));
            }
            if (randomBoolean()) {
                replicaEngine.refresh("test");
            }
            if (randomBoolean()) {
                replicaEngine.flush();
                replicaEngine.refresh("test");
            }
            firstOp = false;
        }

        assertVisibleCount(replicaEngine, lastFieldValue == null ? 0 : 1);
        if (lastFieldValue != null) {
            try (Engine.Searcher searcher = replicaEngine.acquireSearcher("test")) {
                final TotalHitCountCollector collector = new TotalHitCountCollector();
                searcher.search(new TermQuery(new Term("value", lastFieldValue)), collector);
                assertThat(collector.getTotalHits(), equalTo(1));
            }
        }
    }

    public static void concurrentlyApplyOps(List<Engine.Operation> ops, InternalEngine engine) throws InterruptedException {
        Thread[] thread = new Thread[randomIntBetween(3, 5)];
        CountDownLatch startGun = new CountDownLatch(thread.length);
        AtomicInteger offset = new AtomicInteger(-1);
        for (int i = 0; i < thread.length; i++) {
            thread[i] = new Thread(() -> {
                startGun.countDown();
                try {
                    startGun.await();
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
                int docOffset;
                while ((docOffset = offset.incrementAndGet()) < ops.size()) {
                    try {
                        applyOperation(engine, ops.get(docOffset));
                        if ((docOffset + 1) % 4 == 0) {
                            engine.refresh("test");
                        }
                        if (rarely()) {
                            engine.flush();
                        }
                    } catch (IOException e) {
                        throw new AssertionError(e);
                    }
                }
            });
            thread[i].start();
        }
        for (int i = 0; i < thread.length; i++) {
            thread[i].join();
        }
    }

    public static void applyOperations(Engine engine, List<Engine.Operation> operations) throws IOException {
        for (Engine.Operation operation : operations) {
            applyOperation(engine, operation);
            if (randomInt(100) < 10) {
                engine.refresh("test");
            }
            if (rarely()) {
                engine.flush();
            }
        }
    }

    public static Engine.Result applyOperation(Engine engine, Engine.Operation operation) throws IOException {
        final Engine.Result result;
        switch (operation.operationType()) {
            case INDEX:
                result = engine.index((Engine.Index) operation);
                break;
            case DELETE:
                result = engine.delete((Engine.Delete) operation);
                break;
            case NO_OP:
                result = engine.noOp((Engine.NoOp) operation);
                break;
            default:
                throw new IllegalStateException("No operation defined for [" + operation + "]");
        }
        return result;
    }

    /**
     * Gets a collection of tuples of docId, sequence number, and primary term of all live documents in the provided engine.
     */
    public static List<DocIdSeqNoAndSource> getDocIds(Engine engine, boolean refresh) throws IOException {
        if (refresh) {
            engine.refresh("test_get_doc_ids");
        }
        try (Engine.Searcher searcher = engine.acquireSearcher("test_get_doc_ids", Engine.SearcherScope.INTERNAL)) {
            List<DocIdSeqNoAndSource> docs = new ArrayList<>();
            for (LeafReaderContext leafContext : searcher.getIndexReader().leaves()) {
                LeafReader reader = leafContext.reader();
                NumericDocValues seqNoDocValues = reader.getNumericDocValues(SeqNoFieldMapper.NAME);
                NumericDocValues primaryTermDocValues = reader.getNumericDocValues(SeqNoFieldMapper.PRIMARY_TERM_NAME);
                NumericDocValues versionDocValues = reader.getNumericDocValues(VersionFieldMapper.NAME);
                Bits liveDocs = reader.getLiveDocs();
                for (int i = 0; i < reader.maxDoc(); i++) {
                    if (liveDocs == null || liveDocs.get(i)) {
                        if (primaryTermDocValues.advanceExact(i) == false) {
                            // We have to skip non-root docs because its _id field is not stored (indexed only).
                            continue;
                        }
                        final long primaryTerm = primaryTermDocValues.longValue();
                        Document doc = reader.document(i, Set.of(IdFieldMapper.NAME, SourceFieldMapper.NAME));
                        BytesRef binaryID = doc.getBinaryValue(IdFieldMapper.NAME);
                        String id = Uid.decodeId(Arrays.copyOfRange(binaryID.bytes, binaryID.offset, binaryID.offset + binaryID.length));
                        final BytesRef source = doc.getBinaryValue(SourceFieldMapper.NAME);
                        if (seqNoDocValues.advanceExact(i) == false) {
                            throw new AssertionError("seqNoDocValues not found for doc[" + i + "] id[" + id + "]");
                        }
                        final long seqNo = seqNoDocValues.longValue();
                        if (versionDocValues.advanceExact(i) == false) {
                            throw new AssertionError("versionDocValues not found for doc[" + i + "] id[" + id + "]");
                        }
                        final long version = versionDocValues.longValue();
                        docs.add(new DocIdSeqNoAndSource(id, source, seqNo, primaryTerm, version));
                    }
                }
            }
            docs.sort(Comparator.comparingLong(DocIdSeqNoAndSource::getSeqNo)
                .thenComparingLong(DocIdSeqNoAndSource::getPrimaryTerm)
                .thenComparing((DocIdSeqNoAndSource::getId)));
            return docs;
        }
    }

    /**
     * Reads all engine operations that have been processed by the engine from Lucene index.
     * The returned operations are sorted and de-duplicated, thus each sequence number will be have at most one operation.
     */
    public static List<Translog.Operation> readAllOperationsInLucene(Engine engine) throws IOException {
        final List<Translog.Operation> operations = new ArrayList<>();
        try (Translog.Snapshot snapshot = engine.newChangesSnapshot("test", 0, Long.MAX_VALUE, false, randomBoolean())) {
            Translog.Operation op;
            while ((op = snapshot.next()) != null){
                operations.add(op);
            }
        }
        return operations;
    }

    /**
     * Asserts the provided engine has a consistent document history between translog and Lucene index.
     */
    public static void assertConsistentHistoryBetweenTranslogAndLuceneIndex(Engine engine) throws IOException {
        if (engine instanceof InternalEngine == false) {
            return;
        }
        final List<Translog.Operation> translogOps = new ArrayList<>();
        try (Translog.Snapshot snapshot = EngineTestCase.getTranslog(engine).newSnapshot()) {
            Translog.Operation op;
            while ((op = snapshot.next()) != null) {
                translogOps.add(op);
            }
        }
        final Map<Long, Translog.Operation> luceneOps = readAllOperationsInLucene(engine).stream()
            .collect(Collectors.toMap(Translog.Operation::seqNo, Function.identity()));
        final long maxSeqNo = ((InternalEngine) engine).getLocalCheckpointTracker().getMaxSeqNo();
        for (Translog.Operation op : translogOps) {
            assertThat("translog operation [" + op + "] > max_seq_no[" + maxSeqNo + "]", op.seqNo(), lessThanOrEqualTo(maxSeqNo));
        }
        for (Translog.Operation op : luceneOps.values()) {
            assertThat("lucene operation [" + op + "] > max_seq_no[" + maxSeqNo + "]", op.seqNo(), lessThanOrEqualTo(maxSeqNo));
        }
        final long globalCheckpoint = EngineTestCase.getTranslog(engine).getLastSyncedGlobalCheckpoint();
        final long retainedOps = engine.config().getIndexSettings().getSoftDeleteRetentionOperations();
        final long minSeqNoToRetain;
        if (engine.config().getIndexSettings().isSoftDeleteEnabled()) {
            try (Engine.IndexCommitRef safeCommit = engine.acquireSafeIndexCommit()) {
                final long seqNoForRecovery = Long.parseLong(
                    safeCommit.getIndexCommit().getUserData().get(SequenceNumbers.LOCAL_CHECKPOINT_KEY)) + 1;
                minSeqNoToRetain = Math.min(seqNoForRecovery, globalCheckpoint + 1 - retainedOps);
            }
        } else {
            minSeqNoToRetain = engine.getMinRetainedSeqNo();
        }
        for (Translog.Operation translogOp : translogOps) {
            final Translog.Operation luceneOp = luceneOps.get(translogOp.seqNo());
            if (luceneOp == null) {
                if (minSeqNoToRetain <= translogOp.seqNo()) {
                    fail("Operation not found seq# [" + translogOp.seqNo() + "], global checkpoint [" + globalCheckpoint + "], " +
                        "retention policy [" + retainedOps + "], maxSeqNo [" + maxSeqNo + "], translog op [" + translogOp + "]");
                } else {
                    continue;
                }
            }
            assertThat(luceneOp, notNullValue());
            assertThat(luceneOp.toString(), luceneOp.primaryTerm(), equalTo(translogOp.primaryTerm()));
            assertThat(luceneOp.opType(), equalTo(translogOp.opType()));
            if (luceneOp.opType() == Translog.Operation.Type.INDEX) {
                assertThat(luceneOp.getSource().source, equalTo(translogOp.getSource().source));
            }
        }
    }

    /**
     * Asserts that the max_seq_no stored in the commit's user_data is never smaller than seq_no of any document in the commit.
     */
    public static void assertMaxSeqNoInCommitUserData(Engine engine) throws Exception {
        List<IndexCommit> commits = DirectoryReader.listCommits(engine.store.directory());
        for (IndexCommit commit : commits) {
            try (DirectoryReader reader = DirectoryReader.open(commit)) {
                assertThat(Long.parseLong(commit.getUserData().get(SequenceNumbers.MAX_SEQ_NO)),
                    greaterThanOrEqualTo(maxSeqNosInReader(reader)));
            }
        }
    }

    public static void assertAtMostOneLuceneDocumentPerSequenceNumber(Engine engine) throws IOException {
        if (engine instanceof InternalEngine) {
            try {
                engine.refresh("test");
                try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
                    assertAtMostOneLuceneDocumentPerSequenceNumber(engine.config().getIndexSettings(), searcher.getDirectoryReader());
                }
            } catch (AlreadyClosedException ignored) {
                // engine was closed
            }
        }
    }

    public static void assertAtMostOneLuceneDocumentPerSequenceNumber(IndexSettings indexSettings,
                                                                      DirectoryReader reader) throws IOException {
        Set<Long> seqNos = new HashSet<>();
        final DirectoryReader wrappedReader = indexSettings.isSoftDeleteEnabled() ? Lucene.wrapAllDocsLive(reader) : reader;
        for (LeafReaderContext leaf : wrappedReader.leaves()) {
            NumericDocValues primaryTermDocValues = leaf.reader().getNumericDocValues(SeqNoFieldMapper.PRIMARY_TERM_NAME);
            NumericDocValues seqNoDocValues = leaf.reader().getNumericDocValues(SeqNoFieldMapper.NAME);
            int docId;
            while ((docId = seqNoDocValues.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                assertTrue(seqNoDocValues.advanceExact(docId));
                long seqNo = seqNoDocValues.longValue();
                assertThat(seqNo, greaterThanOrEqualTo(0L));
                if (primaryTermDocValues.advanceExact(docId)) {
                    if (seqNos.add(seqNo) == false) {
                        IdStoredFieldLoader idLoader = new IdStoredFieldLoader(leaf.reader());
                        throw new AssertionError("found multiple documents for seq=" + seqNo + " id=" + idLoader.id(docId));
                    }
                }
            }
        }
    }

    public static MapperService createMapperService() throws IOException {
        IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
            .putMapping("{\"properties\": {}}")
            .build();
        MapperService mapperService = MapperTestUtils.newMapperService(new NamedXContentRegistry(ClusterModule.getNamedXWriteables()),
            createTempDir(), Settings.EMPTY, "test");
        mapperService.merge(indexMetadata, MapperService.MergeReason.MAPPING_UPDATE);
        return mapperService;
    }

    public static MappingLookup mappingLookup() {
        try {
            return createMapperService().mappingLookup();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Exposes a translog associated with the given engine for testing purpose.
     */
    public static Translog getTranslog(Engine engine) {
        assert engine instanceof InternalEngine : "only InternalEngines have translogs, got: " + engine.getClass();
        InternalEngine internalEngine = (InternalEngine) engine;
        return internalEngine.getTranslog();
    }

    /**
     * Waits for all operations up to the provided sequence number to complete in the given internal engine.
     *
     * @param seqNo the sequence number that the checkpoint must advance to before this method returns
     * @throws InterruptedException if the thread was interrupted while blocking on the condition
     */
    public static void waitForOpsToComplete(InternalEngine engine, long seqNo) throws Exception {
        assertBusy(() ->
                assertThat(engine.getLocalCheckpointTracker().getProcessedCheckpoint(), greaterThanOrEqualTo(seqNo)));
    }

    public static boolean hasSnapshottedCommits(Engine engine) {
        assert engine instanceof InternalEngine : "only InternalEngines have snapshotted commits, got: " + engine.getClass();
        InternalEngine internalEngine = (InternalEngine) engine;
        return internalEngine.hasSnapshottedCommits();
    }

    public static final class PrimaryTermSupplier implements LongSupplier {
        private final AtomicLong term;

        PrimaryTermSupplier(long initialTerm) {
            this.term = new AtomicLong(initialTerm);
        }

        public long get() {
            return term.get();
        }

        public void set(long newTerm) {
            this.term.set(newTerm);
        }

        @Override
        public long getAsLong() {
            return get();
        }
    }

    static long maxSeqNosInReader(DirectoryReader reader) throws IOException {
        long maxSeqNo = SequenceNumbers.NO_OPS_PERFORMED;
        for (LeafReaderContext leaf : reader.leaves()) {
            final NumericDocValues seqNoDocValues = leaf.reader().getNumericDocValues(SeqNoFieldMapper.NAME);
            while (seqNoDocValues.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                maxSeqNo = SequenceNumbers.max(maxSeqNo, seqNoDocValues.longValue());
            }
        }
        return maxSeqNo;
    }

    /**
     * Returns the number of times a version was looked up either from version map or from the index.
     */
    public static long getNumVersionLookups(Engine engine) {
        return ((InternalEngine) engine).getNumVersionLookups();
    }

    public static long getInFlightDocCount(Engine engine) {
        if (engine instanceof InternalEngine) {
            return ((InternalEngine) engine).getInFlightDocCount();
        } else {
            return 0;
        }
    }

    public static void assertNoInFlightDocuments(Engine engine) throws Exception {
        assertBusy(() -> assertThat(getInFlightDocCount(engine), equalTo(0L)));
    }

    public static final class MatchingDirectoryReader extends FilterDirectoryReader {
        private final Query query;

        public MatchingDirectoryReader(DirectoryReader in, Query query) throws IOException {
            super(in, new SubReaderWrapper() {
                @Override
                public LeafReader wrap(LeafReader leaf) {
                    try {
                        final IndexSearcher searcher = new IndexSearcher(leaf);
                        searcher.setQueryCache(null);
                        final Weight weight = searcher.createWeight(query, ScoreMode.COMPLETE_NO_SCORES, 1.0f);
                        final Scorer scorer = weight.scorer(leaf.getContext());
                        final DocIdSetIterator iterator = scorer != null ? scorer.iterator() : null;
                        final FixedBitSet liveDocs = new FixedBitSet(leaf.maxDoc());
                        if (iterator != null) {
                            for (int docId = iterator.nextDoc(); docId != DocIdSetIterator.NO_MORE_DOCS; docId = iterator.nextDoc()) {
                                if (leaf.getLiveDocs() == null || leaf.getLiveDocs().get(docId)) {
                                    liveDocs.set(docId);
                                }
                            }
                        }
                        return new FilterLeafReader(leaf) {
                            @Override
                            public Bits getLiveDocs() {
                                return liveDocs;
                            }

                            @Override
                            public CacheHelper getCoreCacheHelper() {
                                return leaf.getCoreCacheHelper();
                            }

                            @Override
                            public CacheHelper getReaderCacheHelper() {
                                return null; // modify liveDocs
                            }
                        };
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
            });
            this.query = query;
        }

        @Override
        protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
            return new MatchingDirectoryReader(in, query);
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            // TODO: We should not return the ReaderCacheHelper if we modify the liveDocs,
            // but some caching components (e.g., global ordinals) require this cache key.
            return in.getReaderCacheHelper();
        }
    }

    public static CheckedFunction<DirectoryReader, DirectoryReader, IOException> randomReaderWrapper() {
        if (randomBoolean()) {
            return reader -> reader;
        } else {
            return reader -> new MatchingDirectoryReader(reader, new MatchAllDocsQuery());
        }
    }

    public static Function<Engine.Searcher, Engine.Searcher> randomSearcherWrapper() {
        if (randomBoolean()) {
            return Function.identity();
        } else {
            final CheckedFunction<DirectoryReader, DirectoryReader, IOException> readerWrapper = randomReaderWrapper();
            return searcher -> SearcherHelper.wrapSearcher(searcher, readerWrapper);
        }
    }

    public static void checkNoSoftDeletesLoaded(ReadOnlyEngine readOnlyEngine) {
        if (readOnlyEngine.lazilyLoadSoftDeletes == false) {
            throw new IllegalStateException("method should only be called when lazily loading soft-deletes is enabled");
        }
        try (Engine.Searcher searcher = readOnlyEngine.acquireSearcher("soft-deletes-check", Engine.SearcherScope.INTERNAL)) {
            for (LeafReaderContext ctx : searcher.getIndexReader().getContext().leaves()) {
                LazySoftDeletesDirectoryReaderWrapper.LazyBits lazyBits = lazyBits(ctx.reader());
                if (lazyBits != null && lazyBits.initialized()) {
                    throw new IllegalStateException("soft-deletes loaded");
                }
            }
        }
    }

    @Nullable
    private static LazySoftDeletesDirectoryReaderWrapper.LazyBits lazyBits(LeafReader reader) {
        if (reader instanceof LazySoftDeletesDirectoryReaderWrapper.LazySoftDeletesFilterLeafReader) {
            return ((LazySoftDeletesDirectoryReaderWrapper.LazySoftDeletesFilterLeafReader) reader).getLiveDocs();
        } else if (reader instanceof LazySoftDeletesDirectoryReaderWrapper.LazySoftDeletesFilterCodecReader) {
            return ((LazySoftDeletesDirectoryReaderWrapper.LazySoftDeletesFilterCodecReader) reader).getLiveDocs();
        } else if (reader instanceof FilterLeafReader) {
            final FilterLeafReader fReader = (FilterLeafReader) reader;
            return lazyBits(FilterLeafReader.unwrap(fReader));
        } else if (reader instanceof FilterCodecReader) {
            final FilterCodecReader fReader = (FilterCodecReader) reader;
            return lazyBits(FilterCodecReader.unwrap(fReader));
        } else if (reader instanceof SegmentReader) {
            return null;
        }
        // hard fail - we can't get the lazybits
        throw new IllegalStateException("Can not extract lazy bits from given index reader [" + reader + "]");
    }
}
