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

package org.elasticsearch.index.engine;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import com.carrotsearch.randomizedtesting.annotations.Seed;
import com.google.common.collect.ImmutableMap;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.*;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase.SuppressFileSystems;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.deletionpolicy.KeepOnlyLastDeletionPolicy;
import org.elasticsearch.index.deletionpolicy.SnapshotDeletionPolicy;
import org.elasticsearch.index.deletionpolicy.SnapshotIndexCommit;
import org.elasticsearch.index.engine.Engine.Searcher;
import org.elasticsearch.index.indexing.ShardIndexingService;
import org.elasticsearch.index.indexing.slowlog.ShardSlowLogIndexingService;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.Mapper.BuilderContext;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.index.mapper.internal.SourceFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.mapper.object.RootObjectMapper;
import org.elasticsearch.index.merge.policy.LogByteSizeMergePolicyProvider;
import org.elasticsearch.index.merge.policy.MergePolicyProvider;
import org.elasticsearch.index.merge.scheduler.ConcurrentMergeSchedulerProvider;
import org.elasticsearch.index.merge.scheduler.MergeSchedulerProvider;
import org.elasticsearch.index.settings.IndexDynamicSettingsModule;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardUtils;
import org.elasticsearch.index.shard.TranslogRecoveryPerformer;
import org.elasticsearch.index.similarity.SimilarityLookupService;
import org.elasticsearch.index.store.DirectoryService;
import org.elasticsearch.index.store.DirectoryUtils;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogSizeMatcher;
import org.elasticsearch.index.translog.fs.FsTranslog;
import org.elasticsearch.test.DummyShardLock;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.common.settings.ImmutableSettings.Builder.EMPTY_SETTINGS;
import static org.elasticsearch.index.engine.Engine.Operation.Origin.PRIMARY;
import static org.elasticsearch.index.engine.Engine.Operation.Origin.REPLICA;
import static org.hamcrest.Matchers.*;

// TODO: this guy isn't ready for mock filesystems yet
@SuppressFileSystems("*")
public class InternalEngineTests extends ElasticsearchTestCase {

    protected final ShardId shardId = new ShardId(new Index("index"), 1);

    protected ThreadPool threadPool;

    private Store store;
    private Store storeReplica;

    protected Translog translog;
    protected Translog replicaTranslog;

    protected InternalEngine engine;
    protected InternalEngine replicaEngine;

    private Settings defaultSettings;
    private int indexConcurrency;
    private String codecName;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        CodecService codecService = new CodecService(shardId.index());
        indexConcurrency = randomIntBetween(1, 20);
        String name = Codec.getDefault().getName();
        if (Arrays.asList(codecService.availableCodecs()).contains(name)) {
            // some codecs are read only so we only take the ones that we have in the service and randomly
            // selected by lucene test case.
            codecName = name;
        } else {
            codecName = "default";
        }
        defaultSettings = ImmutableSettings.builder()
                .put(EngineConfig.INDEX_COMPOUND_ON_FLUSH, randomBoolean())
                .put(EngineConfig.INDEX_GC_DELETES_SETTING, "1h") // make sure this doesn't kick in on us
                .put(EngineConfig.INDEX_CODEC_SETTING, codecName)
                .put(EngineConfig.INDEX_CONCURRENCY_SETTING, indexConcurrency)
                .build(); // TODO randomize more settings
        threadPool = new ThreadPool(getClass().getName());
        store = createStore();
        storeReplica = createStore();
        Lucene.cleanLuceneIndex(store.directory());
        Lucene.cleanLuceneIndex(storeReplica.directory());
        translog = createTranslog();
        engine = createEngine(store, translog);
        LiveIndexWriterConfig currentIndexWriterConfig = engine.getCurrentIndexWriterConfig();

        assertEquals(engine.config().getCodec().getName(), codecService.codec(codecName).getName());
        assertEquals(currentIndexWriterConfig.getCodec().getName(), codecService.codec(codecName).getName());
        if (randomBoolean()) {
            engine.config().setEnableGcDeletes(false);
        }
        replicaTranslog = createTranslogReplica();
        replicaEngine = createEngine(storeReplica, replicaTranslog);
        currentIndexWriterConfig = replicaEngine.getCurrentIndexWriterConfig();

        assertEquals(replicaEngine.config().getCodec().getName(), codecService.codec(codecName).getName());
        assertEquals(currentIndexWriterConfig.getCodec().getName(), codecService.codec(codecName).getName());
        if (randomBoolean()) {
            engine.config().setEnableGcDeletes(false);
        }
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        IOUtils.close(
                replicaEngine, storeReplica, replicaTranslog,
                engine, store, translog);

        terminate(threadPool);
    }

    private Document testDocumentWithTextField() {
        Document document = testDocument();
        document.add(new TextField("value", "test", Field.Store.YES));
        return document;
    }

    private Document testDocument() {
        return new Document();
    }


    private ParsedDocument testParsedDocument(String uid, String id, String type, String routing, long timestamp, long ttl, Document document, BytesReference source, Mapping mappingUpdate) {
        Field uidField = new Field("_uid", uid, UidFieldMapper.Defaults.FIELD_TYPE);
        Field versionField = new NumericDocValuesField("_version", 0);
        document.add(uidField);
        document.add(versionField);
        return new ParsedDocument(uidField, versionField, id, type, routing, timestamp, ttl, Arrays.asList(document), source, mappingUpdate);
    }

    protected Store createStore() throws IOException {
        return createStore(newDirectory());
    }

    protected Store createStore(final Directory directory) throws IOException {
        final DirectoryService directoryService = new DirectoryService(shardId, EMPTY_SETTINGS) {
            @Override
            public Directory newDirectory() throws IOException {
                return directory;
            }

            @Override
            public long throttleTimeInNanos() {
                return 0;
            }
        };
        return new Store(shardId, EMPTY_SETTINGS, directoryService, new DummyShardLock(shardId));
    }

    protected Translog createTranslog() throws IOException {
        return new FsTranslog(shardId, EMPTY_SETTINGS, createTempDir("translog-primary"));
    }

    protected Translog createTranslogReplica() throws IOException {
        return new FsTranslog(shardId, EMPTY_SETTINGS, createTempDir("translog-replica"));
    }

    protected IndexDeletionPolicy createIndexDeletionPolicy() {
        return new KeepOnlyLastDeletionPolicy(shardId, EMPTY_SETTINGS);
    }

    protected SnapshotDeletionPolicy createSnapshotDeletionPolicy() {
        return new SnapshotDeletionPolicy(createIndexDeletionPolicy());
    }

    protected MergePolicyProvider<?> createMergePolicy() {
        return new LogByteSizeMergePolicyProvider(store, new IndexSettingsService(new Index("test"), EMPTY_SETTINGS));
    }

    protected MergeSchedulerProvider createMergeScheduler(IndexSettingsService indexSettingsService) {
        return new ConcurrentMergeSchedulerProvider(shardId, EMPTY_SETTINGS, threadPool, indexSettingsService);
    }

    protected InternalEngine createEngine(Store store, Translog translog) {
        IndexSettingsService indexSettingsService = new IndexSettingsService(shardId.index(), ImmutableSettings.builder().put(defaultSettings).put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build());
        return createEngine(indexSettingsService, store, translog, createMergeScheduler(indexSettingsService));
    }

    protected InternalEngine createEngine(IndexSettingsService indexSettingsService, Store store, Translog translog, MergeSchedulerProvider mergeSchedulerProvider) {
        return new InternalEngine(config(indexSettingsService, store, translog, mergeSchedulerProvider), false);
    }

    public EngineConfig config(IndexSettingsService indexSettingsService, Store store, Translog translog, MergeSchedulerProvider mergeSchedulerProvider) {
        IndexWriterConfig iwc = newIndexWriterConfig();
        EngineConfig config = new EngineConfig(shardId, threadPool, new ShardIndexingService(shardId, EMPTY_SETTINGS, new ShardSlowLogIndexingService(shardId, EMPTY_SETTINGS, indexSettingsService)), indexSettingsService
                , null, store, createSnapshotDeletionPolicy(), translog, createMergePolicy(), mergeSchedulerProvider,
                iwc.getAnalyzer(), iwc.getSimilarity(), new CodecService(shardId.index()), new Engine.FailedEngineListener() {
            @Override
            public void onFailedEngine(ShardId shardId, String reason, @Nullable Throwable t) {
                // we don't need to notify anybody in this test
            }
        }, new TranslogHandler(shardId.index().getName()));


        return config;
    }

    protected static final BytesReference B_1 = new BytesArray(new byte[]{1});
    protected static final BytesReference B_2 = new BytesArray(new byte[]{2});
    protected static final BytesReference B_3 = new BytesArray(new byte[]{3});

    @Test
    public void testSegments() throws Exception {
        List<Segment> segments = engine.segments(false);
        assertThat(segments.isEmpty(), equalTo(true));
        assertThat(engine.segmentsStats().getCount(), equalTo(0l));
        assertThat(engine.segmentsStats().getMemoryInBytes(), equalTo(0l));
        final boolean defaultCompound = defaultSettings.getAsBoolean(EngineConfig.INDEX_COMPOUND_ON_FLUSH, true);

        // create a doc and refresh
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocumentWithTextField(), B_1, null);
        engine.create(new Engine.Create(null, newUid("1"), doc));

        ParsedDocument doc2 = testParsedDocument("2", "2", "test", null, -1, -1, testDocumentWithTextField(), B_2, null);
        engine.create(new Engine.Create(null, newUid("2"), doc2));
        engine.refresh("test");

        segments = engine.segments(false);
        assertThat(segments.size(), equalTo(1));
        SegmentsStats stats = engine.segmentsStats();
        assertThat(stats.getCount(), equalTo(1l));
        assertThat(stats.getTermsMemoryInBytes(), greaterThan(0l));
        assertThat(stats.getStoredFieldsMemoryInBytes(), greaterThan(0l));
        assertThat(stats.getTermVectorsMemoryInBytes(), equalTo(0l));
        assertThat(stats.getNormsMemoryInBytes(), greaterThan(0l));
        assertThat(stats.getDocValuesMemoryInBytes(), greaterThan(0l));
        assertThat(segments.get(0).isCommitted(), equalTo(false));
        assertThat(segments.get(0).isSearch(), equalTo(true));
        assertThat(segments.get(0).getNumDocs(), equalTo(2));
        assertThat(segments.get(0).getDeletedDocs(), equalTo(0));
        assertThat(segments.get(0).isCompound(), equalTo(defaultCompound));
        assertThat(segments.get(0).ramTree, nullValue());

        engine.flush();

        segments = engine.segments(false);
        assertThat(segments.size(), equalTo(1));
        assertThat(engine.segmentsStats().getCount(), equalTo(1l));
        assertThat(segments.get(0).isCommitted(), equalTo(true));
        assertThat(segments.get(0).isSearch(), equalTo(true));
        assertThat(segments.get(0).getNumDocs(), equalTo(2));
        assertThat(segments.get(0).getDeletedDocs(), equalTo(0));
        assertThat(segments.get(0).isCompound(), equalTo(defaultCompound));

        ((InternalEngine) engine).config().setCompoundOnFlush(false);

        ParsedDocument doc3 = testParsedDocument("3", "3", "test", null, -1, -1, testDocumentWithTextField(), B_3, null);
        engine.create(new Engine.Create(null, newUid("3"), doc3));
        engine.refresh("test");

        segments = engine.segments(false);
        assertThat(segments.size(), equalTo(2));
        assertThat(engine.segmentsStats().getCount(), equalTo(2l));
        assertThat(engine.segmentsStats().getTermsMemoryInBytes(), greaterThan(stats.getTermsMemoryInBytes()));
        assertThat(engine.segmentsStats().getStoredFieldsMemoryInBytes(), greaterThan(stats.getStoredFieldsMemoryInBytes()));
        assertThat(engine.segmentsStats().getTermVectorsMemoryInBytes(), equalTo(0l));
        assertThat(engine.segmentsStats().getNormsMemoryInBytes(), greaterThan(stats.getNormsMemoryInBytes()));
        assertThat(engine.segmentsStats().getDocValuesMemoryInBytes(), greaterThan(stats.getDocValuesMemoryInBytes()));
        assertThat(segments.get(0).getGeneration() < segments.get(1).getGeneration(), equalTo(true));
        assertThat(segments.get(0).isCommitted(), equalTo(true));
        assertThat(segments.get(0).isSearch(), equalTo(true));
        assertThat(segments.get(0).getNumDocs(), equalTo(2));
        assertThat(segments.get(0).getDeletedDocs(), equalTo(0));
        assertThat(segments.get(0).isCompound(), equalTo(defaultCompound));


        assertThat(segments.get(1).isCommitted(), equalTo(false));
        assertThat(segments.get(1).isSearch(), equalTo(true));
        assertThat(segments.get(1).getNumDocs(), equalTo(1));
        assertThat(segments.get(1).getDeletedDocs(), equalTo(0));
        assertThat(segments.get(1).isCompound(), equalTo(false));


        engine.delete(new Engine.Delete("test", "1", newUid("1")));
        engine.refresh("test");

        segments = engine.segments(false);
        assertThat(segments.size(), equalTo(2));
        assertThat(engine.segmentsStats().getCount(), equalTo(2l));
        assertThat(segments.get(0).getGeneration() < segments.get(1).getGeneration(), equalTo(true));
        assertThat(segments.get(0).isCommitted(), equalTo(true));
        assertThat(segments.get(0).isSearch(), equalTo(true));
        assertThat(segments.get(0).getNumDocs(), equalTo(1));
        assertThat(segments.get(0).getDeletedDocs(), equalTo(1));
        assertThat(segments.get(0).isCompound(), equalTo(defaultCompound));

        assertThat(segments.get(1).isCommitted(), equalTo(false));
        assertThat(segments.get(1).isSearch(), equalTo(true));
        assertThat(segments.get(1).getNumDocs(), equalTo(1));
        assertThat(segments.get(1).getDeletedDocs(), equalTo(0));
        assertThat(segments.get(1).isCompound(), equalTo(false));

        ((InternalEngine) engine).config().setCompoundOnFlush(true);
        ParsedDocument doc4 = testParsedDocument("4", "4", "test", null, -1, -1, testDocumentWithTextField(), B_3, null);
        engine.create(new Engine.Create(null, newUid("4"), doc4));
        engine.refresh("test");

        segments = engine.segments(false);
        assertThat(segments.size(), equalTo(3));
        assertThat(engine.segmentsStats().getCount(), equalTo(3l));
        assertThat(segments.get(0).getGeneration() < segments.get(1).getGeneration(), equalTo(true));
        assertThat(segments.get(0).isCommitted(), equalTo(true));
        assertThat(segments.get(0).isSearch(), equalTo(true));
        assertThat(segments.get(0).getNumDocs(), equalTo(1));
        assertThat(segments.get(0).getDeletedDocs(), equalTo(1));
        assertThat(segments.get(0).isCompound(), equalTo(defaultCompound));

        assertThat(segments.get(1).isCommitted(), equalTo(false));
        assertThat(segments.get(1).isSearch(), equalTo(true));
        assertThat(segments.get(1).getNumDocs(), equalTo(1));
        assertThat(segments.get(1).getDeletedDocs(), equalTo(0));
        assertThat(segments.get(1).isCompound(), equalTo(false));

        assertThat(segments.get(2).isCommitted(), equalTo(false));
        assertThat(segments.get(2).isSearch(), equalTo(true));
        assertThat(segments.get(2).getNumDocs(), equalTo(1));
        assertThat(segments.get(2).getDeletedDocs(), equalTo(0));
        assertThat(segments.get(2).isCompound(), equalTo(true));
    }

    public void testVerboseSegments() throws Exception {
        List<Segment> segments = engine.segments(true);
        assertThat(segments.isEmpty(), equalTo(true));

        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocumentWithTextField(), B_1, null);
        engine.create(new Engine.Create(null, newUid("1"), doc));
        engine.refresh("test");

        segments = engine.segments(true);
        assertThat(segments.size(), equalTo(1));
        assertThat(segments.get(0).ramTree, notNullValue());

        ParsedDocument doc2 = testParsedDocument("2", "2", "test", null, -1, -1, testDocumentWithTextField(), B_2, null);
        engine.create(new Engine.Create(null, newUid("2"), doc2));
        engine.refresh("test");
        ParsedDocument doc3 = testParsedDocument("3", "3", "test", null, -1, -1, testDocumentWithTextField(), B_3, null);
        engine.create(new Engine.Create(null, newUid("3"), doc3));
        engine.refresh("test");

        segments = engine.segments(true);
        assertThat(segments.size(), equalTo(3));
        assertThat(segments.get(0).ramTree, notNullValue());
        assertThat(segments.get(1).ramTree, notNullValue());
        assertThat(segments.get(2).ramTree, notNullValue());

    }


    @Test
    public void testSegmentsWithMergeFlag() throws Exception {
        ConcurrentMergeSchedulerProvider mergeSchedulerProvider = new ConcurrentMergeSchedulerProvider(shardId, EMPTY_SETTINGS, threadPool, new IndexSettingsService(shardId.index(), EMPTY_SETTINGS));
        IndexSettingsService indexSettingsService = new IndexSettingsService(shardId.index(), ImmutableSettings.builder().put(defaultSettings).put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build());
        try (Store store = createStore();
             Translog translog = createTranslog();
             Engine engine = createEngine(indexSettingsService, store, translog, mergeSchedulerProvider)) {

            ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), B_1, null);
            Engine.Index index = new Engine.Index(null, newUid("1"), doc);
            engine.index(index);
            engine.flush();
            assertThat(engine.segments(false).size(), equalTo(1));
            index = new Engine.Index(null, newUid("2"), doc);
            engine.index(index);
            engine.flush();
            List<Segment> segments = engine.segments(false);
            assertThat(segments.size(), equalTo(2));
            for (Segment segment : segments) {
                assertThat(segment.getMergeId(), nullValue());
            }
            index = new Engine.Index(null, newUid("3"), doc);
            engine.index(index);
            engine.flush();
            segments = engine.segments(false);
            assertThat(segments.size(), equalTo(3));
            for (Segment segment : segments) {
                assertThat(segment.getMergeId(), nullValue());
            }

            index = new Engine.Index(null, newUid("4"), doc);
            engine.index(index);
            engine.flush();
            final long gen1 = store.readLastCommittedSegmentsInfo().getGeneration();
            // now, optimize and wait for merges, see that we have no merge flag
            engine.forceMerge(true);

            for (Segment segment : engine.segments(false)) {
                assertThat(segment.getMergeId(), nullValue());
            }
            // we could have multiple underlying merges, so the generation may increase more than once
            assertTrue(store.readLastCommittedSegmentsInfo().getGeneration() > gen1);

            final boolean flush = randomBoolean();
            final long gen2 = store.readLastCommittedSegmentsInfo().getGeneration();
            engine.forceMerge(flush);
            for (Segment segment : engine.segments(false)) {
                assertThat(segment.getMergeId(), nullValue());
            }

            if (flush) {
                // we should have had just 1 merge, so last generation should be exact
                assertEquals(gen2 + 1, store.readLastCommittedSegmentsInfo().getLastGeneration());
            }
        }
    }

    public void testCommitStats() {
        Document document = testDocumentWithTextField();
        document.add(new Field(SourceFieldMapper.NAME, B_1.toBytes(), SourceFieldMapper.Defaults.FIELD_TYPE));
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, document, B_1, null);
        engine.create(new Engine.Create(null, newUid("1"), doc));

        CommitStats stats1 = engine.commitStats();
        assertThat(stats1.getGeneration(), greaterThan(0l));
        assertThat(stats1.getId(), notNullValue());
        assertThat(stats1.getUserData(), hasKey(Translog.TRANSLOG_ID_KEY));

        engine.flush(true, true);
        CommitStats stats2 = engine.commitStats();
        assertThat(stats2.getGeneration(), greaterThan(stats1.getGeneration()));
        assertThat(stats2.getId(), notNullValue());
        assertThat(stats2.getId(), not(equalTo(stats1.getId())));
        assertThat(stats2.getUserData(), hasKey(Translog.TRANSLOG_ID_KEY));
        assertThat(stats2.getUserData().get(Translog.TRANSLOG_ID_KEY), not(equalTo(stats1.getUserData().get(Translog.TRANSLOG_ID_KEY))));
    }

    @Test
    public void testSimpleOperations() throws Exception {
        Engine.Searcher searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(0));
        searchResult.close();

        // create a document
        Document document = testDocumentWithTextField();
        document.add(new Field(SourceFieldMapper.NAME, B_1.toBytes(), SourceFieldMapper.Defaults.FIELD_TYPE));
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, document, B_1, null);
        engine.create(new Engine.Create(null, newUid("1"), doc));

        // its not there...
        searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(0));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 0));
        searchResult.close();

        // but, we can still get it (in realtime)
        Engine.GetResult getResult = engine.get(new Engine.Get(true, newUid("1")));
        assertThat(getResult.exists(), equalTo(true));
        assertThat(getResult.source().source.toBytesArray(), equalTo(B_1.toBytesArray()));
        assertThat(getResult.docIdAndVersion(), nullValue());
        getResult.release();

        // but, not there non realtime
        getResult = engine.get(new Engine.Get(false, newUid("1")));
        assertThat(getResult.exists(), equalTo(false));
        getResult.release();
        // refresh and it should be there
        engine.refresh("test");

        // now its there...
        searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(1));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 1));
        searchResult.close();

        // also in non realtime
        getResult = engine.get(new Engine.Get(false, newUid("1")));
        assertThat(getResult.exists(), equalTo(true));
        assertThat(getResult.docIdAndVersion(), notNullValue());
        getResult.release();

        // now do an update
        document = testDocument();
        document.add(new TextField("value", "test1", Field.Store.YES));
        document.add(new Field(SourceFieldMapper.NAME, B_2.toBytes(), SourceFieldMapper.Defaults.FIELD_TYPE));
        doc = testParsedDocument("1", "1", "test", null, -1, -1, document, B_2, null);
        engine.index(new Engine.Index(null, newUid("1"), doc));

        // its not updated yet...
        searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(1));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 1));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test1")), 0));
        searchResult.close();

        // but, we can still get it (in realtime)
        getResult = engine.get(new Engine.Get(true, newUid("1")));
        assertThat(getResult.exists(), equalTo(true));
        assertThat(getResult.source().source.toBytesArray(), equalTo(B_2.toBytesArray()));
        assertThat(getResult.docIdAndVersion(), nullValue());
        getResult.release();

        // refresh and it should be updated
        engine.refresh("test");

        searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(1));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 0));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test1")), 1));
        searchResult.close();

        // now delete
        engine.delete(new Engine.Delete("test", "1", newUid("1")));

        // its not deleted yet
        searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(1));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 0));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test1")), 1));
        searchResult.close();

        // but, get should not see it (in realtime)
        getResult = engine.get(new Engine.Get(true, newUid("1")));
        assertThat(getResult.exists(), equalTo(false));
        getResult.release();

        // refresh and it should be deleted
        engine.refresh("test");

        searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(0));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 0));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test1")), 0));
        searchResult.close();

        // add it back
        document = testDocumentWithTextField();
        document.add(new Field(SourceFieldMapper.NAME, B_1.toBytes(), SourceFieldMapper.Defaults.FIELD_TYPE));
        doc = testParsedDocument("1", "1", "test", null, -1, -1, document, B_1, null);
        engine.create(new Engine.Create(null, newUid("1"), doc));

        // its not there...
        searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(0));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 0));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test1")), 0));
        searchResult.close();

        // refresh and it should be there
        engine.refresh("test");

        // now its there...
        searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(1));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 1));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test1")), 0));
        searchResult.close();

        // now flush
        engine.flush();

        // and, verify get (in real time)
        getResult = engine.get(new Engine.Get(true, newUid("1")));
        assertThat(getResult.exists(), equalTo(true));
        assertThat(getResult.source(), nullValue());
        assertThat(getResult.docIdAndVersion(), notNullValue());
        getResult.release();

        // make sure we can still work with the engine
        // now do an update
        document = testDocument();
        document.add(new TextField("value", "test1", Field.Store.YES));
        doc = testParsedDocument("1", "1", "test", null, -1, -1, document, B_1, null);
        engine.index(new Engine.Index(null, newUid("1"), doc));

        // its not updated yet...
        searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(1));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 1));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test1")), 0));
        searchResult.close();

        // refresh and it should be updated
        engine.refresh("test");

        searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(1));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 0));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test1")), 1));
        searchResult.close();

        engine.close();
    }

    @Test
    public void testSearchResultRelease() throws Exception {
        Engine.Searcher searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(0));
        searchResult.close();

        // create a document
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocumentWithTextField(), B_1, null);
        engine.create(new Engine.Create(null, newUid("1"), doc));

        // its not there...
        searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(0));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 0));
        searchResult.close();

        // refresh and it should be there
        engine.refresh("test");

        // now its there...
        searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(1));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 1));
        // don't release the search result yet...

        // delete, refresh and do a new search, it should not be there
        engine.delete(new Engine.Delete("test", "1", newUid("1")));
        engine.refresh("test");
        Engine.Searcher updateSearchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(updateSearchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(0));
        updateSearchResult.close();

        // the non release search result should not see the deleted yet...
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(1));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 1));
        searchResult.close();
    }

    @Test
    public void testFailEngineOnCorruption() {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocumentWithTextField(), B_1, null);
        engine.create(new Engine.Create(null, newUid("1"), doc));
        engine.flush();
        final int failInPhase = randomIntBetween(1, 3);
        try {
            engine.recover(new Engine.RecoveryHandler() {
                @Override
                public void phase1(SnapshotIndexCommit snapshot) throws EngineException {
                    if (failInPhase == 1) {
                        throw new RuntimeException("bar", new CorruptIndexException("Foo", "fake file description"));
                    }
                }

                @Override
                public void phase2(Translog.Snapshot snapshot) throws EngineException {
                    if (failInPhase == 2) {
                        throw new RuntimeException("bar", new CorruptIndexException("Foo", "fake file description"));
                    }
                }

                @Override
                public void phase3(Translog.Snapshot snapshot) throws EngineException {
                    if (failInPhase == 3) {
                        throw new RuntimeException("bar", new CorruptIndexException("Foo", "fake file description"));
                    }
                }
            });
            fail("exception expected");
        } catch (RuntimeException ex) {

        }
        try {
            Engine.Searcher searchResult = engine.acquireSearcher("test");
            MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(1));
            MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 1));
            searchResult.close();

            ParsedDocument doc2 = testParsedDocument("2", "2", "test", null, -1, -1, testDocumentWithTextField(), B_2, null);
            engine.create(new Engine.Create(null, newUid("2"), doc2));
            engine.refresh("foo");

            searchResult = engine.acquireSearcher("test");
            MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 2));
            MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(2));
            searchResult.close();
            fail("engine should have failed");
        } catch (EngineClosedException ex) {
            // expected
        }
    }


    @Test
    public void testSimpleRecover() throws Exception {
        final ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocumentWithTextField(), B_1, null);
        engine.create(new Engine.Create(null, newUid("1"), doc));
        engine.flush();

        engine.recover(new Engine.RecoveryHandler() {
            @Override
            public void phase1(SnapshotIndexCommit snapshot) throws EngineException {
                try {
                    engine.flush();
                    assertThat("flush is not allowed in phase 1", false, equalTo(true));
                } catch (FlushNotAllowedEngineException e) {
                    // all is well
                }
            }

            @Override
            public void phase2(Translog.Snapshot snapshot) throws EngineException {
                MatcherAssert.assertThat(snapshot, TranslogSizeMatcher.translogSize(0));
                try {
                    engine.flush();
                    assertThat("flush is not allowed in phase 2", false, equalTo(true));
                } catch (FlushNotAllowedEngineException e) {
                    // all is well
                }

                // but we can index
                engine.index(new Engine.Index(null, newUid("1"), doc));
            }

            @Override
            public void phase3(Translog.Snapshot snapshot) throws EngineException {
                MatcherAssert.assertThat(snapshot, TranslogSizeMatcher.translogSize(1));
                try {
                    // we can do this here since we are on the same thread
                    engine.flush();
                    assertThat("flush is not allowed in phase 3", false, equalTo(true));
                } catch (FlushNotAllowedEngineException e) {
                    // all is well
                }
            }
        });
        // post recovery should flush the translog
        try (Translog.Snapshot snapshot = translog.snapshot()) {
            MatcherAssert.assertThat(snapshot, TranslogSizeMatcher.translogSize(0));
        }
        // and we should not leak files
        assertThat("there are unreferenced translog files left", translog.clearUnreferenced(), equalTo(0));

        engine.flush();

        assertThat("there are unreferenced translog files left, post flush", translog.clearUnreferenced(), equalTo(0));

        engine.close();
    }

    @Test
    public void testRecoverWithOperationsBetweenPhase1AndPhase2() throws Exception {
        ParsedDocument doc1 = testParsedDocument("1", "1", "test", null, -1, -1, testDocumentWithTextField(), B_1, null);
        engine.create(new Engine.Create(null, newUid("1"), doc1));
        engine.flush();
        ParsedDocument doc2 = testParsedDocument("2", "2", "test", null, -1, -1, testDocumentWithTextField(), B_2, null);
        engine.create(new Engine.Create(null, newUid("2"), doc2));

        engine.recover(new Engine.RecoveryHandler() {
            @Override
            public void phase1(SnapshotIndexCommit snapshot) throws EngineException {
            }

            @Override
            public void phase2(Translog.Snapshot snapshot) throws EngineException {
                try {
                    Translog.Create create = (Translog.Create) snapshot.next();
                    assertThat("translog snapshot should not read null", create != null, equalTo(true));
                    assertThat(create.source().toBytesArray(), equalTo(B_2));
                    assertThat(snapshot.next(), equalTo(null));
                } catch (IOException ex) {
                    throw new ElasticsearchException("failed", ex);
                }
            }

            @Override
            public void phase3(Translog.Snapshot snapshot) throws EngineException {
                MatcherAssert.assertThat(snapshot, TranslogSizeMatcher.translogSize(0));
            }
        });

        engine.flush();
        engine.close();
    }

    @Test
    public void testRecoverWithOperationsBetweenPhase1AndPhase2AndPhase3() throws Exception {
        ParsedDocument doc1 = testParsedDocument("1", "1", "test", null, -1, -1, testDocumentWithTextField(), B_1, null);
        engine.create(new Engine.Create(null, newUid("1"), doc1));
        engine.flush();
        ParsedDocument doc2 = testParsedDocument("2", "2", "test", null, -1, -1, testDocumentWithTextField(), B_2, null);
        engine.create(new Engine.Create(null, newUid("2"), doc2));

        engine.recover(new Engine.RecoveryHandler() {
            @Override
            public void phase1(SnapshotIndexCommit snapshot) throws EngineException {
            }

            @Override
            public void phase2(Translog.Snapshot snapshot) throws EngineException {
                try {
                    Translog.Create create = (Translog.Create) snapshot.next();
                    assertThat(create != null, equalTo(true));
                    assertThat(snapshot.next(), equalTo(null));
                    assertThat(create.source().toBytesArray(), equalTo(B_2));

                    // add for phase3
                    ParsedDocument doc3 = testParsedDocument("3", "3", "test", null, -1, -1, testDocumentWithTextField(), B_3, null);
                    engine.create(new Engine.Create(null, newUid("3"), doc3));
                } catch (IOException ex) {
                    throw new ElasticsearchException("failed", ex);
                }
            }

            @Override
            public void phase3(Translog.Snapshot snapshot) throws EngineException {
                try {
                    Translog.Create create = (Translog.Create) snapshot.next();
                    assertThat(create != null, equalTo(true));
                    assertThat(snapshot.next(), equalTo(null));
                    assertThat(create.source().toBytesArray(), equalTo(B_3));
                } catch (IOException ex) {
                    throw new ElasticsearchException("failed", ex);
                }
            }
        });

        engine.flush();
        engine.close();
    }

    @Test
    public void testVersioningNewCreate() {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), B_1, null);
        Engine.Create create = new Engine.Create(null, newUid("1"), doc);
        engine.create(create);
        assertThat(create.version(), equalTo(1l));

        create = new Engine.Create(null, newUid("1"), doc, create.version(), create.versionType().versionTypeForReplicationAndRecovery(), REPLICA, 0);
        replicaEngine.create(create);
        assertThat(create.version(), equalTo(1l));
    }

    @Test
    public void testExternalVersioningNewCreate() {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), B_1, null);
        Engine.Create create = new Engine.Create(null, newUid("1"), doc, 12, VersionType.EXTERNAL, Engine.Operation.Origin.PRIMARY, 0);
        engine.create(create);
        assertThat(create.version(), equalTo(12l));

        create = new Engine.Create(null, newUid("1"), doc, create.version(), create.versionType().versionTypeForReplicationAndRecovery(), REPLICA, 0);
        replicaEngine.create(create);
        assertThat(create.version(), equalTo(12l));
    }

    @Test
    public void testVersioningNewIndex() {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), B_1, null);
        Engine.Index index = new Engine.Index(null, newUid("1"), doc);
        engine.index(index);
        assertThat(index.version(), equalTo(1l));

        index = new Engine.Index(null, newUid("1"), doc, index.version(), index.versionType().versionTypeForReplicationAndRecovery(), REPLICA, 0);
        replicaEngine.index(index);
        assertThat(index.version(), equalTo(1l));
    }

    @Test
    public void testExternalVersioningNewIndex() {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), B_1, null);
        Engine.Index index = new Engine.Index(null, newUid("1"), doc, 12, VersionType.EXTERNAL, PRIMARY, 0);
        engine.index(index);
        assertThat(index.version(), equalTo(12l));

        index = new Engine.Index(null, newUid("1"), doc, index.version(), index.versionType().versionTypeForReplicationAndRecovery(), REPLICA, 0);
        replicaEngine.index(index);
        assertThat(index.version(), equalTo(12l));
    }

    @Test
    public void testVersioningIndexConflict() {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), B_1, null);
        Engine.Index index = new Engine.Index(null, newUid("1"), doc);
        engine.index(index);
        assertThat(index.version(), equalTo(1l));

        index = new Engine.Index(null, newUid("1"), doc);
        engine.index(index);
        assertThat(index.version(), equalTo(2l));

        index = new Engine.Index(null, newUid("1"), doc, 1l, VersionType.INTERNAL, Engine.Operation.Origin.PRIMARY, 0);
        try {
            engine.index(index);
            fail();
        } catch (VersionConflictEngineException e) {
            // all is well
        }

        // future versions should not work as well
        index = new Engine.Index(null, newUid("1"), doc, 3l, VersionType.INTERNAL, PRIMARY, 0);
        try {
            engine.index(index);
            fail();
        } catch (VersionConflictEngineException e) {
            // all is well
        }
    }

    @Test
    public void testExternalVersioningIndexConflict() {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), B_1, null);
        Engine.Index index = new Engine.Index(null, newUid("1"), doc, 12, VersionType.EXTERNAL, PRIMARY, 0);
        engine.index(index);
        assertThat(index.version(), equalTo(12l));

        index = new Engine.Index(null, newUid("1"), doc, 14, VersionType.EXTERNAL, PRIMARY, 0);
        engine.index(index);
        assertThat(index.version(), equalTo(14l));

        index = new Engine.Index(null, newUid("1"), doc, 13, VersionType.EXTERNAL, PRIMARY, 0);
        try {
            engine.index(index);
            fail();
        } catch (VersionConflictEngineException e) {
            // all is well
        }
    }

    @Test
    public void testVersioningIndexConflictWithFlush() {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), B_1, null);
        Engine.Index index = new Engine.Index(null, newUid("1"), doc);
        engine.index(index);
        assertThat(index.version(), equalTo(1l));

        index = new Engine.Index(null, newUid("1"), doc);
        engine.index(index);
        assertThat(index.version(), equalTo(2l));

        engine.flush();

        index = new Engine.Index(null, newUid("1"), doc, 1l, VersionType.INTERNAL, PRIMARY, 0);
        try {
            engine.index(index);
            fail();
        } catch (VersionConflictEngineException e) {
            // all is well
        }

        // future versions should not work as well
        index = new Engine.Index(null, newUid("1"), doc, 3l, VersionType.INTERNAL, PRIMARY, 0);
        try {
            engine.index(index);
            fail();
        } catch (VersionConflictEngineException e) {
            // all is well
        }
    }

    @Test
    public void testExternalVersioningIndexConflictWithFlush() {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), B_1, null);
        Engine.Index index = new Engine.Index(null, newUid("1"), doc, 12, VersionType.EXTERNAL, PRIMARY, 0);
        engine.index(index);
        assertThat(index.version(), equalTo(12l));

        index = new Engine.Index(null, newUid("1"), doc, 14, VersionType.EXTERNAL, PRIMARY, 0);
        engine.index(index);
        assertThat(index.version(), equalTo(14l));

        engine.flush();

        index = new Engine.Index(null, newUid("1"), doc, 13, VersionType.EXTERNAL, PRIMARY, 0);
        try {
            engine.index(index);
            fail();
        } catch (VersionConflictEngineException e) {
            // all is well
        }
    }

    public void testForceMerge() {
        int numDocs = randomIntBetween(10, 100);
        for (int i = 0; i < numDocs; i++) {
            ParsedDocument doc = testParsedDocument(Integer.toString(i), Integer.toString(i), "test", null, -1, -1, testDocument(), B_1, null);
            Engine.Index index = new Engine.Index(null, newUid(Integer.toString(i)), doc);
            engine.index(index);
            engine.refresh("test");
        }
        try (Engine.Searcher test = engine.acquireSearcher("test")) {
            assertEquals(numDocs, test.reader().numDocs());
        }
        engine.forceMerge(true, 1, false, false, false);
        assertEquals(engine.segments(true).size(), 1);

        ParsedDocument doc = testParsedDocument(Integer.toString(0), Integer.toString(0), "test", null, -1, -1, testDocument(), B_1, null);
        Engine.Index index = new Engine.Index(null, newUid(Integer.toString(0)), doc);
        engine.delete(new Engine.Delete(index.type(), index.id(), index.uid()));
        engine.forceMerge(true, 10, true, false, false); //expunge deletes

        assertEquals(engine.segments(true).size(), 1);
        try (Engine.Searcher test = engine.acquireSearcher("test")) {
            assertEquals(numDocs - 1, test.reader().numDocs());
            assertEquals(numDocs - 1, test.reader().maxDoc());
        }

        doc = testParsedDocument(Integer.toString(1), Integer.toString(1), "test", null, -1, -1, testDocument(), B_1, null);
        index = new Engine.Index(null, newUid(Integer.toString(1)), doc);
        engine.delete(new Engine.Delete(index.type(), index.id(), index.uid()));
        engine.forceMerge(true, 10, false, false, false); //expunge deletes

        assertEquals(engine.segments(true).size(), 1);
        try (Engine.Searcher test = engine.acquireSearcher("test")) {
            assertEquals(numDocs - 2, test.reader().numDocs());
            assertEquals(numDocs - 1, test.reader().maxDoc());
        }
    }

    public void testForceMergeAndClose() throws IOException, InterruptedException {
        int numIters = randomIntBetween(2, 10);
        for (int j = 0; j < numIters; j++) {
            try (Store store = createStore()) {
                final Translog translog = createTranslog();
                final InternalEngine engine = createEngine(store, translog);
                final CountDownLatch startGun = new CountDownLatch(1);
                final CountDownLatch indexed = new CountDownLatch(1);

                Thread thread = new Thread() {
                    public void run() {
                        try {
                            try {
                                startGun.await();
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                            int i = 0;
                            while (true) {
                                int numDocs = randomIntBetween(1, 20);
                                for (int j = 0; j < numDocs; j++) {
                                    i++;
                                    ParsedDocument doc = testParsedDocument(Integer.toString(i), Integer.toString(i), "test", null, -1, -1, testDocument(), B_1, null);
                                    Engine.Index index = new Engine.Index(null, newUid(Integer.toString(i)), doc);
                                    engine.index(index);
                                }
                                engine.refresh("test");
                                indexed.countDown();
                                try {
                                    engine.forceMerge(randomBoolean(), 1, false, randomBoolean(), randomBoolean());
                                } catch (ForceMergeFailedEngineException ex) {
                                    // ok
                                    return;
                                }
                            }
                        } catch (AlreadyClosedException | EngineClosedException ex) {
                            // fine
                        }
                    }
                };

                thread.start();
                startGun.countDown();
                int someIters = randomIntBetween(1, 10);
                for (int i = 0; i < someIters; i++) {
                    engine.forceMerge(randomBoolean(), 1, false, randomBoolean(), randomBoolean());
                }
                indexed.await();
                IOUtils.close(engine, translog);
            }
        }

    }

    @Test
    public void testVersioningDeleteConflict() {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), B_1, null);
        Engine.Index index = new Engine.Index(null, newUid("1"), doc);
        engine.index(index);
        assertThat(index.version(), equalTo(1l));

        index = new Engine.Index(null, newUid("1"), doc);
        engine.index(index);
        assertThat(index.version(), equalTo(2l));

        Engine.Delete delete = new Engine.Delete("test", "1", newUid("1"), 1l, VersionType.INTERNAL, PRIMARY, 0, false);
        try {
            engine.delete(delete);
            fail();
        } catch (VersionConflictEngineException e) {
            // all is well
        }

        // future versions should not work as well
        delete = new Engine.Delete("test", "1", newUid("1"), 3l, VersionType.INTERNAL, PRIMARY, 0, false);
        try {
            engine.delete(delete);
            fail();
        } catch (VersionConflictEngineException e) {
            // all is well
        }

        // now actually delete
        delete = new Engine.Delete("test", "1", newUid("1"), 2l, VersionType.INTERNAL, PRIMARY, 0, false);
        engine.delete(delete);
        assertThat(delete.version(), equalTo(3l));

        // now check if we can index to a delete doc with version
        index = new Engine.Index(null, newUid("1"), doc, 2l, VersionType.INTERNAL, PRIMARY, 0);
        try {
            engine.index(index);
            fail();
        } catch (VersionConflictEngineException e) {
            // all is well
        }

        // we shouldn't be able to create as well
        Engine.Create create = new Engine.Create(null, newUid("1"), doc, 2l, VersionType.INTERNAL, PRIMARY, 0);
        try {
            engine.create(create);
        } catch (VersionConflictEngineException e) {
            // all is well
        }
    }

    @Test
    public void testVersioningDeleteConflictWithFlush() {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), B_1, null);
        Engine.Index index = new Engine.Index(null, newUid("1"), doc);
        engine.index(index);
        assertThat(index.version(), equalTo(1l));

        index = new Engine.Index(null, newUid("1"), doc);
        engine.index(index);
        assertThat(index.version(), equalTo(2l));

        engine.flush();

        Engine.Delete delete = new Engine.Delete("test", "1", newUid("1"), 1l, VersionType.INTERNAL, PRIMARY, 0, false);
        try {
            engine.delete(delete);
            fail();
        } catch (VersionConflictEngineException e) {
            // all is well
        }

        // future versions should not work as well
        delete = new Engine.Delete("test", "1", newUid("1"), 3l, VersionType.INTERNAL, PRIMARY, 0, false);
        try {
            engine.delete(delete);
            fail();
        } catch (VersionConflictEngineException e) {
            // all is well
        }

        engine.flush();

        // now actually delete
        delete = new Engine.Delete("test", "1", newUid("1"), 2l, VersionType.INTERNAL, PRIMARY, 0, false);
        engine.delete(delete);
        assertThat(delete.version(), equalTo(3l));

        engine.flush();

        // now check if we can index to a delete doc with version
        index = new Engine.Index(null, newUid("1"), doc, 2l, VersionType.INTERNAL, PRIMARY, 0);
        try {
            engine.index(index);
            fail();
        } catch (VersionConflictEngineException e) {
            // all is well
        }

        // we shouldn't be able to create as well
        Engine.Create create = new Engine.Create(null, newUid("1"), doc, 2l, VersionType.INTERNAL, PRIMARY, 0);
        try {
            engine.create(create);
        } catch (VersionConflictEngineException e) {
            // all is well
        }
    }

    @Test
    public void testVersioningCreateExistsException() {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), B_1, null);
        Engine.Create create = new Engine.Create(null, newUid("1"), doc, Versions.MATCH_ANY, VersionType.INTERNAL, PRIMARY, 0);
        engine.create(create);
        assertThat(create.version(), equalTo(1l));

        create = new Engine.Create(null, newUid("1"), doc, Versions.MATCH_ANY, VersionType.INTERNAL, PRIMARY, 0);
        try {
            engine.create(create);
            fail();
        } catch (DocumentAlreadyExistsException e) {
            // all is well
        }
    }

    @Test
    public void testVersioningCreateExistsExceptionWithFlush() {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), B_1, null);
        Engine.Create create = new Engine.Create(null, newUid("1"), doc, Versions.MATCH_ANY, VersionType.INTERNAL, PRIMARY, 0);
        engine.create(create);
        assertThat(create.version(), equalTo(1l));

        engine.flush();

        create = new Engine.Create(null, newUid("1"), doc, Versions.MATCH_ANY, VersionType.INTERNAL, PRIMARY, 0);
        try {
            engine.create(create);
            fail();
        } catch (DocumentAlreadyExistsException e) {
            // all is well
        }
    }

    @Test
    public void testVersioningReplicaConflict1() {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), B_1, null);
        Engine.Index index = new Engine.Index(null, newUid("1"), doc);
        engine.index(index);
        assertThat(index.version(), equalTo(1l));

        index = new Engine.Index(null, newUid("1"), doc);
        engine.index(index);
        assertThat(index.version(), equalTo(2l));

        // apply the second index to the replica, should work fine
        index = new Engine.Index(null, newUid("1"), doc, index.version(), VersionType.INTERNAL.versionTypeForReplicationAndRecovery(), REPLICA, 0);
        replicaEngine.index(index);
        assertThat(index.version(), equalTo(2l));

        // now, the old one should not work
        index = new Engine.Index(null, newUid("1"), doc, 1l, VersionType.INTERNAL.versionTypeForReplicationAndRecovery(), REPLICA, 0);
        try {
            replicaEngine.index(index);
            fail();
        } catch (VersionConflictEngineException e) {
            // all is well
        }

        // second version on replica should fail as well
        try {
            index = new Engine.Index(null, newUid("1"), doc, 2l
                    , VersionType.INTERNAL.versionTypeForReplicationAndRecovery(), REPLICA, 0);
            replicaEngine.index(index);
            assertThat(index.version(), equalTo(2l));
        } catch (VersionConflictEngineException e) {
            // all is well
        }
    }

    @Test
    public void testVersioningReplicaConflict2() {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), B_1, null);
        Engine.Index index = new Engine.Index(null, newUid("1"), doc);
        engine.index(index);
        assertThat(index.version(), equalTo(1l));

        // apply the first index to the replica, should work fine
        index = new Engine.Index(null, newUid("1"), doc, 1l
                , VersionType.INTERNAL.versionTypeForReplicationAndRecovery(), REPLICA, 0);
        replicaEngine.index(index);
        assertThat(index.version(), equalTo(1l));

        // index it again
        index = new Engine.Index(null, newUid("1"), doc);
        engine.index(index);
        assertThat(index.version(), equalTo(2l));

        // now delete it
        Engine.Delete delete = new Engine.Delete("test", "1", newUid("1"));
        engine.delete(delete);
        assertThat(delete.version(), equalTo(3l));

        // apply the delete on the replica (skipping the second index)
        delete = new Engine.Delete("test", "1", newUid("1"), 3l
                , VersionType.INTERNAL.versionTypeForReplicationAndRecovery(), REPLICA, 0, false);
        replicaEngine.delete(delete);
        assertThat(delete.version(), equalTo(3l));

        // second time delete with same version should fail
        try {
            delete = new Engine.Delete("test", "1", newUid("1"), 3l
                    , VersionType.INTERNAL.versionTypeForReplicationAndRecovery(), REPLICA, 0, false);
            replicaEngine.delete(delete);
            fail("excepted VersionConflictEngineException to be thrown");
        } catch (VersionConflictEngineException e) {
            // all is well
        }

        // now do the second index on the replica, it should fail
        try {
            index = new Engine.Index(null, newUid("1"), doc, 2l, VersionType.INTERNAL.versionTypeForReplicationAndRecovery(), REPLICA, 0);
            replicaEngine.index(index);
            fail("excepted VersionConflictEngineException to be thrown");
        } catch (VersionConflictEngineException e) {
            // all is well
        }
    }


    @Test
    public void testBasicCreatedFlag() {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), B_1, null);
        Engine.Index index = new Engine.Index(null, newUid("1"), doc);
        assertTrue(engine.index(index));

        index = new Engine.Index(null, newUid("1"), doc);
        assertFalse(engine.index(index));

        engine.delete(new Engine.Delete(null, "1", newUid("1")));

        index = new Engine.Index(null, newUid("1"), doc);
        assertTrue(engine.index(index));
    }

    @Test
    public void testCreatedFlagAfterFlush() {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), B_1, null);
        Engine.Index index = new Engine.Index(null, newUid("1"), doc);
        assertTrue(engine.index(index));

        engine.delete(new Engine.Delete(null, "1", newUid("1")));

        engine.flush();

        index = new Engine.Index(null, newUid("1"), doc);
        assertTrue(engine.index(index));
    }

    private static class MockAppender extends AppenderSkeleton {
        public boolean sawIndexWriterMessage;

        public boolean sawIndexWriterIFDMessage;

        @Override
        protected void append(LoggingEvent event) {
            if (event.getLevel() == Level.TRACE && event.getMessage().toString().contains("[index][1] ")) {
                if (event.getLoggerName().endsWith("lucene.iw") &&
                        event.getMessage().toString().contains("IW: apply all deletes during flush")) {
                    sawIndexWriterMessage = true;
                }
                if (event.getLoggerName().endsWith("lucene.iw.ifd")) {
                    sawIndexWriterIFDMessage = true;
                }
            }
        }

        @Override
        public boolean requiresLayout() {
            return false;
        }

        @Override
        public void close() {
        }
    }

    // #5891: make sure IndexWriter's infoStream output is
    // sent to lucene.iw with log level TRACE:

    @Test
    public void testIndexWriterInfoStream() {
        assumeFalse("who tests the tester?", VERBOSE);
        MockAppender mockAppender = new MockAppender();

        Logger rootLogger = Logger.getRootLogger();
        Level savedLevel = rootLogger.getLevel();
        rootLogger.addAppender(mockAppender);
        rootLogger.setLevel(Level.DEBUG);

        try {
            // First, with DEBUG, which should NOT log IndexWriter output:
            ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocumentWithTextField(), B_1, null);
            engine.create(new Engine.Create(null, newUid("1"), doc));
            engine.flush();
            assertFalse(mockAppender.sawIndexWriterMessage);

            // Again, with TRACE, which should log IndexWriter output:
            rootLogger.setLevel(Level.TRACE);
            engine.create(new Engine.Create(null, newUid("2"), doc));
            engine.flush();
            assertTrue(mockAppender.sawIndexWriterMessage);

        } finally {
            rootLogger.removeAppender(mockAppender);
            rootLogger.setLevel(savedLevel);
        }
    }

    // #8603: make sure we can separately log IFD's messages
    public void testIndexWriterIFDInfoStream() {
        assumeFalse("who tests the tester?", VERBOSE);
        MockAppender mockAppender = new MockAppender();

        // Works when running this test inside Intellij:
        Logger iwIFDLogger = LogManager.exists("org.elasticsearch.index.engine.lucene.iw.ifd");
        if (iwIFDLogger == null) {
            // Works when running this test from command line:
            iwIFDLogger = LogManager.exists("index.engine.lucene.iw.ifd");
            assertNotNull(iwIFDLogger);
        }

        Level savedLevel = iwIFDLogger.getLevel();
        iwIFDLogger.addAppender(mockAppender);
        iwIFDLogger.setLevel(Level.DEBUG);

        try {
            // First, with DEBUG, which should NOT log IndexWriter output:
            ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocumentWithTextField(), B_1, null);
            engine.create(new Engine.Create(null, newUid("1"), doc));
            engine.flush();
            assertFalse(mockAppender.sawIndexWriterMessage);
            assertFalse(mockAppender.sawIndexWriterIFDMessage);

            // Again, with TRACE, which should only log IndexWriter IFD output:
            iwIFDLogger.setLevel(Level.TRACE);
            engine.create(new Engine.Create(null, newUid("2"), doc));
            engine.flush();
            assertFalse(mockAppender.sawIndexWriterMessage);
            assertTrue(mockAppender.sawIndexWriterIFDMessage);

        } finally {
            iwIFDLogger.removeAppender(mockAppender);
            iwIFDLogger.setLevel(null);
        }
    }

    @Slow
    @Test
    public void testEnableGcDeletes() throws Exception {
        IndexSettingsService indexSettingsService = new IndexSettingsService(shardId.index(), ImmutableSettings.builder().put(defaultSettings).put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build());
        try (Store store = createStore();
             Translog translog = createTranslog();
             Engine engine = new InternalEngine(config(indexSettingsService, store, translog, createMergeScheduler(indexSettingsService)), false)) {
            engine.config().setEnableGcDeletes(false);

            // Add document
            Document document = testDocument();
            document.add(new TextField("value", "test1", Field.Store.YES));

            ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, document, B_2, null);
            engine.index(new Engine.Index(null, newUid("1"), doc, 1, VersionType.EXTERNAL, Engine.Operation.Origin.PRIMARY, System.nanoTime(), false));

            // Delete document we just added:
            engine.delete(new Engine.Delete("test", "1", newUid("1"), 10, VersionType.EXTERNAL, Engine.Operation.Origin.PRIMARY, System.nanoTime(), false));

            // Get should not find the document
            Engine.GetResult getResult = engine.get(new Engine.Get(true, newUid("1")));
            assertThat(getResult.exists(), equalTo(false));

            // Give the gc pruning logic a chance to kick in
            Thread.sleep(1000);

            if (randomBoolean()) {
                engine.refresh("test");
            }

            // Delete non-existent document
            engine.delete(new Engine.Delete("test", "2", newUid("2"), 10, VersionType.EXTERNAL, Engine.Operation.Origin.PRIMARY, System.nanoTime(), false));

            // Get should not find the document (we never indexed uid=2):
            getResult = engine.get(new Engine.Get(true, newUid("2")));
            assertThat(getResult.exists(), equalTo(false));

            // Try to index uid=1 with a too-old version, should fail:
            try {
                engine.index(new Engine.Index(null, newUid("1"), doc, 2, VersionType.EXTERNAL, Engine.Operation.Origin.PRIMARY, System.nanoTime()));
                fail("did not hit expected exception");
            } catch (VersionConflictEngineException vcee) {
                // expected
            }

            // Get should still not find the document
            getResult = engine.get(new Engine.Get(true, newUid("1")));
            assertThat(getResult.exists(), equalTo(false));

            // Try to index uid=2 with a too-old version, should fail:
            try {
                engine.index(new Engine.Index(null, newUid("2"), doc, 2, VersionType.EXTERNAL, Engine.Operation.Origin.PRIMARY, System.nanoTime()));
                fail("did not hit expected exception");
            } catch (VersionConflictEngineException vcee) {
                // expected
            }

            // Get should not find the document
            getResult = engine.get(new Engine.Get(true, newUid("2")));
            assertThat(getResult.exists(), equalTo(false));
        }
    }

    protected Term newUid(String id) {
        return new Term("_uid", id);
    }

    @Test
    public void testExtractShardId() {
        try (Engine.Searcher test = this.engine.acquireSearcher("test")) {
            ShardId shardId = ShardUtils.extractShardId(test.reader());
            assertNotNull(shardId);
            assertEquals(shardId, ((InternalEngine) engine).config().getShardId());
        }
    }

    /**
     * Random test that throws random exception and ensures all references are
     * counted down / released and resources are closed.
     */
    @Test
    public void testFailStart() throws IOException {
        // this test fails if any reader, searcher or directory is not closed - MDW FTW
        final int iters = scaledRandomIntBetween(10, 100);
        for (int i = 0; i < iters; i++) {
            MockDirectoryWrapper wrapper = newMockDirectory();
            wrapper.setFailOnOpenInput(randomBoolean());
            wrapper.setAllowRandomFileNotFoundException(randomBoolean());
            wrapper.setRandomIOExceptionRate(randomDouble());
            wrapper.setRandomIOExceptionRateOnOpen(randomDouble());
            try (Store store = createStore(wrapper)) {
                int refCount = store.refCount();
                assertTrue("refCount: " + store.refCount(), store.refCount() > 0);
                Translog translog = createTranslog();
                InternalEngine holder;
                try {
                    holder = createEngine(store, translog);
                } catch (EngineCreationFailureException ex) {
                    assertEquals(store.refCount(), refCount);
                    continue;
                }
                assertEquals(store.refCount(), refCount + 1);
                final int numStarts = scaledRandomIntBetween(1, 5);
                for (int j = 0; j < numStarts; j++) {
                    try {
                        assertEquals(store.refCount(), refCount + 1);
                        holder.close();
                        holder = createEngine(store, translog);
                        assertEquals(store.refCount(), refCount + 1);
                    } catch (EngineCreationFailureException ex) {
                        // all is fine
                        assertEquals(store.refCount(), refCount);
                        break;
                    }
                }
                translog.close();
                holder.close();
                assertEquals(store.refCount(), refCount);
            }
        }
    }

    @Test
    public void testSettings() {
        InternalEngine engine = (InternalEngine) this.engine;
        CodecService codecService = new CodecService(shardId.index());
        LiveIndexWriterConfig currentIndexWriterConfig = engine.getCurrentIndexWriterConfig();

        assertEquals(engine.config().getCodec().getName(), codecService.codec(codecName).getName());
        assertEquals(currentIndexWriterConfig.getCodec().getName(), codecService.codec(codecName).getName());
        assertEquals(engine.config().getIndexConcurrency(), indexConcurrency);
        assertEquals(currentIndexWriterConfig.getMaxThreadStates(), indexConcurrency);


        IndexDynamicSettingsModule settings = new IndexDynamicSettingsModule();
        assertTrue(settings.containsSetting(EngineConfig.INDEX_COMPOUND_ON_FLUSH));
        assertTrue(settings.containsSetting(EngineConfig.INDEX_GC_DELETES_SETTING));
    }

    @Test
    public void testRetryWithAutogeneratedIdWorksAndNoDuplicateDocs() throws IOException {

        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), B_1, null);
        boolean canHaveDuplicates = false;
        boolean autoGeneratedId = true;

        Engine.Create index = new Engine.Create(null, newUid("1"), doc, Versions.MATCH_ANY, VersionType.INTERNAL, PRIMARY, System.nanoTime(), canHaveDuplicates, autoGeneratedId);
        engine.create(index);
        assertThat(index.version(), equalTo(1l));

        index = new Engine.Create(null, newUid("1"), doc, index.version(), index.versionType().versionTypeForReplicationAndRecovery(), REPLICA, System.nanoTime(), canHaveDuplicates, autoGeneratedId);
        replicaEngine.create(index);
        assertThat(index.version(), equalTo(1l));

        canHaveDuplicates = true;
        index = new Engine.Create(null, newUid("1"), doc, Versions.MATCH_ANY, VersionType.INTERNAL, PRIMARY, System.nanoTime(), canHaveDuplicates, autoGeneratedId);
        engine.create(index);
        assertThat(index.version(), equalTo(1l));
        engine.refresh("test");
        Engine.Searcher searcher = engine.acquireSearcher("test");
        TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), 10);
        assertThat(topDocs.totalHits, equalTo(1));

        index = new Engine.Create(null, newUid("1"), doc, index.version(), index.versionType().versionTypeForReplicationAndRecovery(), REPLICA, System.nanoTime(), canHaveDuplicates, autoGeneratedId);
        try {
            replicaEngine.create(index);
            fail();
        } catch (VersionConflictEngineException e) {
            // we ignore version conflicts on replicas, see TransportShardReplicationOperationAction.ignoreReplicaException
        }
        replicaEngine.refresh("test");
        Engine.Searcher replicaSearcher = replicaEngine.acquireSearcher("test");
        topDocs = replicaSearcher.searcher().search(new MatchAllDocsQuery(), 10);
        assertThat(topDocs.totalHits, equalTo(1));
        searcher.close();
        replicaSearcher.close();
    }

    @Test
    public void testRetryWithAutogeneratedIdsAndWrongOrderWorksAndNoDuplicateDocs() throws IOException {

        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), B_1, null);
        boolean canHaveDuplicates = true;
        boolean autoGeneratedId = true;

        Engine.Create firstIndexRequest = new Engine.Create(null, newUid("1"), doc, Versions.MATCH_ANY, VersionType.INTERNAL, PRIMARY, System.nanoTime(), canHaveDuplicates, autoGeneratedId);
        engine.create(firstIndexRequest);
        assertThat(firstIndexRequest.version(), equalTo(1l));

        Engine.Create firstIndexRequestReplica = new Engine.Create(null, newUid("1"), doc, firstIndexRequest.version(), firstIndexRequest.versionType().versionTypeForReplicationAndRecovery(), REPLICA, System.nanoTime(), canHaveDuplicates, autoGeneratedId);
        replicaEngine.create(firstIndexRequestReplica);
        assertThat(firstIndexRequestReplica.version(), equalTo(1l));

        canHaveDuplicates = false;
        Engine.Create secondIndexRequest = new Engine.Create(null, newUid("1"), doc, Versions.MATCH_ANY, VersionType.INTERNAL, PRIMARY, System.nanoTime(), canHaveDuplicates, autoGeneratedId);
        try {
            engine.create(secondIndexRequest);
            fail();
        } catch (DocumentAlreadyExistsException e) {
            // we can ignore the exception. In case this happens because the retry request arrived first then this error will not be sent back anyway.
            // in any other case this is an actual error
        }
        engine.refresh("test");
        Engine.Searcher searcher = engine.acquireSearcher("test");
        TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), 10);
        assertThat(topDocs.totalHits, equalTo(1));

        Engine.Create secondIndexRequestReplica = new Engine.Create(null, newUid("1"), doc, firstIndexRequest.version(), firstIndexRequest.versionType().versionTypeForReplicationAndRecovery(), REPLICA, System.nanoTime(), canHaveDuplicates, autoGeneratedId);
        try {
            replicaEngine.create(secondIndexRequestReplica);
            fail();
        } catch (VersionConflictEngineException e) {
            // we ignore version conflicts on replicas, see TransportShardReplicationOperationAction.ignoreReplicaException.
        }
        replicaEngine.refresh("test");
        Engine.Searcher replicaSearcher = replicaEngine.acquireSearcher("test");
        topDocs = replicaSearcher.searcher().search(new MatchAllDocsQuery(), 10);
        assertThat(topDocs.totalHits, equalTo(1));
        searcher.close();
        replicaSearcher.close();
    }

    // #10312
    @Test
    public void testDeletesAloneCanTriggerRefresh() throws Exception {
        // Tiny indexing buffer:
        Settings indexSettings = ImmutableSettings.builder().put(defaultSettings)
                .put(EngineConfig.INDEX_BUFFER_SIZE_SETTING, "1kb").build();
        IndexSettingsService indexSettingsService = new IndexSettingsService(shardId.index(), indexSettings);
        try (Store store = createStore();
             Translog translog = createTranslog();
             final Engine engine = new InternalEngine(config(indexSettingsService, store, translog, createMergeScheduler(indexSettingsService)), false)) {
            for (int i = 0; i < 100; i++) {
                String id = Integer.toString(i);
                ParsedDocument doc = testParsedDocument(id, id, "test", null, -1, -1, testDocument(), B_1, null);
                engine.index(new Engine.Index(null, newUid(id), doc, 2, VersionType.EXTERNAL, Engine.Operation.Origin.PRIMARY, System.nanoTime()));
            }

            // Force merge so we know all merges are done before we start deleting:
            engine.forceMerge(true, 1, false, false, false);

            Searcher s = engine.acquireSearcher("test");
            final long version1 = ((DirectoryReader) s.reader()).getVersion();
            s.close();
            for (int i = 0; i < 100; i++) {
                String id = Integer.toString(i);
                engine.delete(new Engine.Delete("test", id, newUid(id), 10, VersionType.EXTERNAL, Engine.Operation.Origin.PRIMARY, System.nanoTime(), false));
            }

            // We must assertBusy because refresh due to version map being full is done in background (REFRESH) thread pool:
            assertBusy(new Runnable() {
                @Override
                public void run() {
                    Searcher s2 = engine.acquireSearcher("test");
                    long version2 = ((DirectoryReader) s2.reader()).getVersion();
                    s2.close();

                    // 100 buffered deletes will easily exceed 25% of our 1 KB indexing buffer so it should have forced a refresh:
                    assertThat(version2, greaterThan(version1));
                }
            });
        }
    }

    public void testTranslogReplayWithFailure() throws IOException {
        boolean canHaveDuplicates = true;
        boolean autoGeneratedId = true;
        final int numDocs = randomIntBetween(1, 10);
        for (int i = 0; i < numDocs; i++) {
            ParsedDocument doc = testParsedDocument(Integer.toString(i), Integer.toString(i), "test", null, -1, -1, testDocument(), new BytesArray("{}"), null);
            Engine.Create firstIndexRequest = new Engine.Create(null, newUid(Integer.toString(i)), doc, Versions.MATCH_ANY, VersionType.INTERNAL, PRIMARY, System.nanoTime(), canHaveDuplicates, autoGeneratedId);
            engine.create(firstIndexRequest);
            assertThat(firstIndexRequest.version(), equalTo(1l));
        }
        engine.refresh("test");
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), randomIntBetween(numDocs, numDocs + 10));
            assertThat(topDocs.totalHits, equalTo(numDocs));
        }
        engine.close();
        boolean recoveredButFailed = false;
        final MockDirectoryWrapper directory = DirectoryUtils.getLeaf(store.directory(), MockDirectoryWrapper.class);
        if (directory != null) {
            // since we rollback the IW we are writing the same segment files again after starting IW but MDW prevents
            // this so we have to disable the check explicitly
            directory.setPreventDoubleWrite(false);
            boolean started = false;
            final int numIters = randomIntBetween(10, 20);
            for (int i = 0; i < numIters; i++) {
                directory.setRandomIOExceptionRateOnOpen(randomDouble());
                directory.setRandomIOExceptionRate(randomDouble());
                directory.setFailOnOpenInput(randomBoolean());
                directory.setAllowRandomFileNotFoundException(randomBoolean());
                try {
                    engine = createEngine(store, translog);
                    started = true;
                    break;
                } catch (EngineCreationFailureException ex) {
                    // sometimes we fail after we committed the recovered docs during the finaly refresh call
                    // that means hte index is consistent and recovered so we can't assert on the num recovered ops below.
                        try (IndexReader reader = DirectoryReader.open(directory.getDelegate())) {
                            if (reader.numDocs() == numDocs) {
                                recoveredButFailed = true;
                                break;
                            } else {
                                // skip - we just failed
                            }
                        }
                }
            }

            directory.setRandomIOExceptionRateOnOpen(0.0);
            directory.setRandomIOExceptionRate(0.0);
            directory.setFailOnOpenInput(false);
            directory.setAllowRandomFileNotFoundException(false);
            if (started == false) {
                engine = createEngine(store, translog);
            }
        } else {
            // no mock directory, no fun.
            engine = createEngine(store, translog);
        }
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), randomIntBetween(numDocs, numDocs + 10));
            assertThat(topDocs.totalHits, equalTo(numDocs));
        }
        if (recoveredButFailed == false) {
            TranslogHandler parser = (TranslogHandler) engine.config().getTranslogRecoveryPerformer();
            assertEquals(numDocs, parser.recoveredOps.get());
        }
    }

    @Test
    public void testSkipTranslogReplay() throws IOException {
        boolean canHaveDuplicates = true;
        boolean autoGeneratedId = true;
        final int numDocs = randomIntBetween(1, 10);
        for (int i = 0; i < numDocs; i++) {
            ParsedDocument doc = testParsedDocument(Integer.toString(i), Integer.toString(i), "test", null, -1, -1, testDocument(), new BytesArray("{}"), null);
            Engine.Create firstIndexRequest = new Engine.Create(null, newUid(Integer.toString(i)), doc, Versions.MATCH_ANY, VersionType.INTERNAL, PRIMARY, System.nanoTime(), canHaveDuplicates, autoGeneratedId);
            engine.create(firstIndexRequest);
            assertThat(firstIndexRequest.version(), equalTo(1l));
        }
        engine.refresh("test");
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), randomIntBetween(numDocs, numDocs + 10));
            assertThat(topDocs.totalHits, equalTo(numDocs));
        }
        final MockDirectoryWrapper directory = DirectoryUtils.getLeaf(store.directory(), MockDirectoryWrapper.class);
        if (directory != null) {
            // since we rollback the IW we are writing the same segment files again after starting IW but MDW prevents
            // this so we have to disable the check explicitly
            directory.setPreventDoubleWrite(false);
        }
        long currentTranslogId = translog.currentId();
        engine.close();
        engine = new InternalEngine(engine.config(), true);
        assertTrue(currentTranslogId + "<" + translog.currentId(), currentTranslogId < translog.currentId());
        assertEquals("translog ID must be incremented by 2 after initial recovery", currentTranslogId + 2, translog.currentId());

        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), randomIntBetween(numDocs, numDocs + 10));
            assertThat(topDocs.totalHits, equalTo(0));
        }

    }

    private Mapping dynamicUpdate() {
        BuilderContext context = new BuilderContext(ImmutableSettings.EMPTY, new ContentPath());
        final RootObjectMapper root = MapperBuilders.rootObject("some_type").build(context);
        return new Mapping(root, new RootMapper[0], new Mapping.SourceTransform[0], ImmutableMap.<String, Object>of());
    }

    public void testTranslogReplay() throws IOException {
        boolean canHaveDuplicates = true;
        boolean autoGeneratedId = true;
        final int numDocs = randomIntBetween(1, 10);
        for (int i = 0; i < numDocs; i++) {
            ParsedDocument doc = testParsedDocument(Integer.toString(i), Integer.toString(i), "test", null, -1, -1, testDocument(), new BytesArray("{}"), null);
            Engine.Create firstIndexRequest = new Engine.Create(null, newUid(Integer.toString(i)), doc, Versions.MATCH_ANY, VersionType.INTERNAL, PRIMARY, System.nanoTime(), canHaveDuplicates, autoGeneratedId);
            engine.create(firstIndexRequest);
            assertThat(firstIndexRequest.version(), equalTo(1l));
        }
        engine.refresh("test");
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), randomIntBetween(numDocs, numDocs + 10));
            assertThat(topDocs.totalHits, equalTo(numDocs));
        }
        final MockDirectoryWrapper directory = DirectoryUtils.getLeaf(store.directory(), MockDirectoryWrapper.class);
        if (directory != null) {
            // since we rollback the IW we are writing the same segment files again after starting IW but MDW prevents
            // this so we have to disable the check explicitly
            directory.setPreventDoubleWrite(false);
        }

        TranslogHandler parser = (TranslogHandler) engine.config().getTranslogRecoveryPerformer();
        parser.mappingUpdate = dynamicUpdate();

        long currentTranslogId = translog.currentId();
        engine.close();
        engine = new InternalEngine(engine.config(), false); // we need to reuse the engine config otherwise the parser.mappingModified won't work
        assertTrue(currentTranslogId + "<" + translog.currentId(), currentTranslogId < translog.currentId());
        assertEquals("translog ID must be incremented by 2 after initial recovery", currentTranslogId + 2, translog.currentId());

        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), randomIntBetween(numDocs, numDocs + 10));
            assertThat(topDocs.totalHits, equalTo(numDocs));
        }
        parser = (TranslogHandler) engine.config().getTranslogRecoveryPerformer();
        assertEquals(numDocs, parser.recoveredOps.get());
        if (parser.mappingUpdate != null) {
            assertEquals(1, parser.getRecoveredTypes().size());
            assertTrue(parser.getRecoveredTypes().containsKey("test"));
        } else {
            assertEquals(0, parser.getRecoveredTypes().size());
        }

        engine.close();
        engine = createEngine(store, translog);
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), randomIntBetween(numDocs, numDocs + 10));
            assertThat(topDocs.totalHits, equalTo(numDocs));
        }
        parser = (TranslogHandler) engine.config().getTranslogRecoveryPerformer();
        assertEquals(0, parser.recoveredOps.get());

        final boolean flush = randomBoolean();
        int randomId = randomIntBetween(numDocs + 1, numDocs + 10);
        String uuidValue = "test#" + Integer.toString(randomId);
        ParsedDocument doc = testParsedDocument(uuidValue, Integer.toString(randomId), "test", null, -1, -1, testDocument(), new BytesArray("{}"), null);
        Engine.Create firstIndexRequest = new Engine.Create(null, newUid(uuidValue), doc, 1, VersionType.EXTERNAL, PRIMARY, System.nanoTime(), canHaveDuplicates, autoGeneratedId);
        engine.create(firstIndexRequest);
        assertThat(firstIndexRequest.version(), equalTo(1l));
        if (flush) {
            engine.flush();
        }

        doc = testParsedDocument(uuidValue, Integer.toString(randomId), "test", null, -1, -1, testDocument(), new BytesArray("{}"), null);
        Engine.Index idxRequest = new Engine.Index(null, newUid(uuidValue), doc, 2, VersionType.EXTERNAL, PRIMARY, System.nanoTime());
        engine.index(idxRequest);
        engine.refresh("test");
        assertThat(idxRequest.version(), equalTo(2l));
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), numDocs + 1);
            assertThat(topDocs.totalHits, equalTo(numDocs + 1));
        }

        engine.close();
        engine = createEngine(store, translog);
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), numDocs + 1);
            assertThat(topDocs.totalHits, equalTo(numDocs + 1));
        }
        parser = (TranslogHandler) engine.config().getTranslogRecoveryPerformer();
        assertEquals(flush ? 1 : 2, parser.recoveredOps.get());
        engine.delete(new Engine.Delete("test", Integer.toString(randomId), newUid(uuidValue)));
        if (randomBoolean()) {
            engine.refresh("test");
        } else {
            engine.close();
            engine = createEngine(store, translog);
        }
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), numDocs);
            assertThat(topDocs.totalHits, equalTo(numDocs));
        }
    }

    public static class TranslogHandler extends TranslogRecoveryPerformer {

        private final DocumentMapper docMapper;
        public Mapping mappingUpdate = null;

        public final AtomicInteger recoveredOps = new AtomicInteger(0);

        public TranslogHandler(String index) {
            super(null, new MapperAnalyzer(null), null, null, null);
            Settings settings = ImmutableSettings.settingsBuilder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build();
            RootObjectMapper.Builder rootBuilder = new RootObjectMapper.Builder("test");
            DocumentMapper.Builder b = new DocumentMapper.Builder(index, settings, rootBuilder);
            DocumentMapperParser parser = new DocumentMapperParser(new Index(index), settings,
                    new AnalysisService(new Index(index), settings),
                    new SimilarityLookupService(new Index(index), settings), null);
            this.docMapper = b.build(parser);

        }

        @Override
        protected Tuple<DocumentMapper, Mapping> docMapper(String type) {
            return new Tuple<>(docMapper, mappingUpdate);
        }

        @Override
        protected void operationProcessed() {
            recoveredOps.incrementAndGet();
        }
    }

}
