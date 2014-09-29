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

package org.elasticsearch.index.engine.internal;

import com.google.common.base.Predicate;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
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
import org.elasticsearch.index.engine.*;
import org.elasticsearch.index.indexing.ShardIndexingService;
import org.elasticsearch.index.indexing.slowlog.ShardSlowLogIndexingService;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.internal.SourceFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.merge.OnGoingMerge;
import org.elasticsearch.index.merge.policy.LogByteSizeMergePolicyProvider;
import org.elasticsearch.index.merge.policy.MergePolicyProvider;
import org.elasticsearch.index.merge.scheduler.ConcurrentMergeSchedulerProvider;
import org.elasticsearch.index.merge.scheduler.MergeSchedulerProvider;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.index.store.DirectoryService;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.distributor.LeastUsedDistributor;
import org.elasticsearch.index.store.ram.RamDirectoryService;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogSizeMatcher;
import org.elasticsearch.index.translog.fs.FsTranslog;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.common.settings.ImmutableSettings.Builder.EMPTY_SETTINGS;
import static org.elasticsearch.index.engine.Engine.Operation.Origin.PRIMARY;
import static org.elasticsearch.index.engine.Engine.Operation.Origin.REPLICA;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class InternalEngineTests extends ElasticsearchTestCase {

    protected final ShardId shardId = new ShardId(new Index("index"), 1);

    protected ThreadPool threadPool;

    private Store store;
    private Store storeReplica;

    protected Engine engine;
    protected Engine replicaEngine;

    private IndexSettingsService engineSettingsService;

    private IndexSettingsService replicaSettingsService;

    private Settings defaultSettings;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        defaultSettings = ImmutableSettings.builder()
                .put(InternalEngine.INDEX_COMPOUND_ON_FLUSH, getRandom().nextBoolean())
                .put(InternalEngine.INDEX_CHECKSUM_ON_MERGE, getRandom().nextBoolean())
                .put(InternalEngine.INDEX_GC_DELETES, "1h") // make sure this doesn't kick in on us
                .put(InternalEngine.INDEX_FAIL_ON_CORRUPTION, randomBoolean())
                .build(); // TODO randomize more settings
        threadPool = new ThreadPool(getClass().getName());
        store = createStore();
        store.deleteContent();
        storeReplica = createStoreReplica();
        storeReplica.deleteContent();
        engineSettingsService = new IndexSettingsService(shardId.index(), EMPTY_SETTINGS);
        engine = createEngine(engineSettingsService, store, createTranslog());
        if (randomBoolean()) {
            engine.enableGcDeletes(false);
        }
        engine.start();
        replicaSettingsService = new IndexSettingsService(shardId.index(), EMPTY_SETTINGS);
        replicaEngine = createEngine(replicaSettingsService, storeReplica, createTranslogReplica());
        if (randomBoolean()) {
            replicaEngine.enableGcDeletes(false);
        }
        replicaEngine.start();
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        replicaEngine.close();
        storeReplica.close();

        engine.close();
        store.close();
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


    private ParsedDocument testParsedDocument(String uid, String id, String type, String routing, long timestamp, long ttl, Document document, Analyzer analyzer, BytesReference source, boolean mappingsModified) {
        Field uidField = new Field("_uid", uid, UidFieldMapper.Defaults.FIELD_TYPE);
        Field versionField = new NumericDocValuesField("_version", 0);
        document.add(uidField);
        document.add(versionField);
        return new ParsedDocument(uidField, versionField, id, type, routing, timestamp, ttl, Arrays.asList(document), analyzer, source, mappingsModified);
    }

    protected Store createStore() throws IOException {
        DirectoryService directoryService = new RamDirectoryService(shardId, EMPTY_SETTINGS);
        return new Store(shardId, EMPTY_SETTINGS, null, directoryService, new LeastUsedDistributor(directoryService));
    }

    protected Store createStoreReplica() throws IOException {
        DirectoryService directoryService = new RamDirectoryService(shardId, EMPTY_SETTINGS);
        return new Store(shardId, EMPTY_SETTINGS, null, directoryService, new LeastUsedDistributor(directoryService));
    }

    protected Translog createTranslog() {
        return new FsTranslog(shardId, EMPTY_SETTINGS, new File("work/fs-translog/primary"));
    }

    protected Translog createTranslogReplica() {
        return new FsTranslog(shardId, EMPTY_SETTINGS, new File("work/fs-translog/replica"));
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

    protected Engine createEngine(IndexSettingsService indexSettingsService, Store store, Translog translog) {
        return createEngine(indexSettingsService, store, translog, createMergeScheduler(indexSettingsService));
    }

    protected Engine createEngine(IndexSettingsService indexSettingsService, Store store, Translog translog, MergeSchedulerProvider mergeSchedulerProvider) {
        return new InternalEngine(shardId, defaultSettings, threadPool, indexSettingsService, new ShardIndexingService(shardId, EMPTY_SETTINGS, new ShardSlowLogIndexingService(shardId, EMPTY_SETTINGS, indexSettingsService)), null, store, createSnapshotDeletionPolicy(), translog, createMergePolicy(), mergeSchedulerProvider,
                new AnalysisService(shardId.index()), new SimilarityService(shardId.index()), new CodecService(shardId.index()));
    }

    protected static final BytesReference B_1 = new BytesArray(new byte[]{1});
    protected static final BytesReference B_2 = new BytesArray(new byte[]{2});
    protected static final BytesReference B_3 = new BytesArray(new byte[]{3});

    @Test
    public void testSegments() throws Exception {
        List<Segment> segments = engine.segments();
        assertThat(segments.isEmpty(), equalTo(true));
        assertThat(engine.segmentsStats().getCount(), equalTo(0l));
        final boolean defaultCompound = defaultSettings.getAsBoolean(InternalEngine.INDEX_COMPOUND_ON_FLUSH, true);

        // create a doc and refresh
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocumentWithTextField(), Lucene.STANDARD_ANALYZER, B_1, false);
        engine.create(new Engine.Create(null, newUid("1"), doc));

        ParsedDocument doc2 = testParsedDocument("2", "2", "test", null, -1, -1, testDocumentWithTextField(), Lucene.STANDARD_ANALYZER, B_2, false);
        engine.create(new Engine.Create(null, newUid("2"), doc2));
        engine.refresh(new Engine.Refresh("test").force(false));

        segments = engine.segments();
        assertThat(segments.size(), equalTo(1));
        assertThat(engine.segmentsStats().getCount(), equalTo(1l));
        assertThat(segments.get(0).isCommitted(), equalTo(false));
        assertThat(segments.get(0).isSearch(), equalTo(true));
        assertThat(segments.get(0).getNumDocs(), equalTo(2));
        assertThat(segments.get(0).getDeletedDocs(), equalTo(0));
        assertThat(segments.get(0).isCompound(), equalTo(defaultCompound));

        engine.flush(new Engine.Flush());

        segments = engine.segments();
        assertThat(segments.size(), equalTo(1));
        assertThat(engine.segmentsStats().getCount(), equalTo(1l));
        assertThat(segments.get(0).isCommitted(), equalTo(true));
        assertThat(segments.get(0).isSearch(), equalTo(true));
        assertThat(segments.get(0).getNumDocs(), equalTo(2));
        assertThat(segments.get(0).getDeletedDocs(), equalTo(0));
        assertThat(segments.get(0).isCompound(), equalTo(defaultCompound));

        engineSettingsService.refreshSettings(ImmutableSettings.builder().put(InternalEngine.INDEX_COMPOUND_ON_FLUSH, false).build());

        ParsedDocument doc3 = testParsedDocument("3", "3", "test", null, -1, -1, testDocumentWithTextField(), Lucene.STANDARD_ANALYZER, B_3, false);
        engine.create(new Engine.Create(null, newUid("3"), doc3));
        engine.refresh(new Engine.Refresh("test").force(false));

        segments = engine.segments();
        assertThat(segments.size(), equalTo(2));
        assertThat(engine.segmentsStats().getCount(), equalTo(2l));
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
        engine.refresh(new Engine.Refresh("test").force(false));

        segments = engine.segments();
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

        engineSettingsService.refreshSettings(ImmutableSettings.builder().put(InternalEngine.INDEX_COMPOUND_ON_FLUSH, true).build());
        ParsedDocument doc4 = testParsedDocument("4", "4", "test", null, -1, -1, testDocumentWithTextField(), Lucene.STANDARD_ANALYZER, B_3, false);
        engine.create(new Engine.Create(null, newUid("4"), doc4));
        engine.refresh(new Engine.Refresh("test").force(false));

        segments = engine.segments();
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

    public void testStartAndAcquireConcurrently() {
        ConcurrentMergeSchedulerProvider mergeSchedulerProvider = new ConcurrentMergeSchedulerProvider(shardId, EMPTY_SETTINGS, threadPool, new IndexSettingsService(shardId.index(), EMPTY_SETTINGS));
        final Engine engine = createEngine(engineSettingsService, store, createTranslog(), mergeSchedulerProvider);
        final AtomicBoolean startPending = new AtomicBoolean(true);
        Thread thread = new Thread() {
            public void run() {
                try {
                    Thread.yield();
                    engine.start();
                } finally {
                    startPending.set(false);
                }

            }
        };
        thread.start();
        while(startPending.get()) {
            try {
                engine.acquireSearcher("foobar").close();
                break;
            } catch (EngineClosedException ex) {
                // all good
            }
        }
        engine.close();
    }


    @Test
    public void testSegmentsWithMergeFlag() throws Exception {
        ConcurrentMergeSchedulerProvider mergeSchedulerProvider = new ConcurrentMergeSchedulerProvider(shardId, EMPTY_SETTINGS, threadPool, new IndexSettingsService(shardId.index(), EMPTY_SETTINGS));
        final AtomicReference<CountDownLatch> waitTillMerge = new AtomicReference<>();
        final AtomicReference<CountDownLatch> waitForMerge = new AtomicReference<>();
        mergeSchedulerProvider.addListener(new MergeSchedulerProvider.Listener() {
            @Override
            public void beforeMerge(OnGoingMerge merge) {
                try {
                    if (waitTillMerge.get() != null) {
                        waitTillMerge.get().countDown();
                    }
                    if (waitForMerge.get() != null) {
                        waitForMerge.get().await();
                    }
                } catch (InterruptedException e) {
                    throw ExceptionsHelper.convertToRuntime(e);
                }
            }

            @Override
            public void afterMerge(OnGoingMerge merge) {
            }
        });

        final Engine engine = createEngine(engineSettingsService, store, createTranslog(), mergeSchedulerProvider);
        engine.start();
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), Lucene.STANDARD_ANALYZER, B_1, false);
        Engine.Index index = new Engine.Index(null, newUid("1"), doc);
        engine.index(index);
        engine.flush(new Engine.Flush());
        assertThat(engine.segments().size(), equalTo(1));
        index = new Engine.Index(null, newUid("2"), doc);
        engine.index(index);
        engine.flush(new Engine.Flush());
        assertThat(engine.segments().size(), equalTo(2));
        for (Segment segment : engine.segments()) {
            assertThat(segment.getMergeId(), nullValue());
        }
        index = new Engine.Index(null, newUid("3"), doc);
        engine.index(index);
        engine.flush(new Engine.Flush());
        assertThat(engine.segments().size(), equalTo(3));
        for (Segment segment : engine.segments()) {
            assertThat(segment.getMergeId(), nullValue());
        }

        waitTillMerge.set(new CountDownLatch(1));
        waitForMerge.set(new CountDownLatch(1));
        engine.optimize(new Engine.Optimize().maxNumSegments(1).waitForMerge(false));
        waitTillMerge.get().await();

        for (Segment segment : engine.segments()) {
            assertThat(segment.getMergeId(), notNullValue());
        }

        waitForMerge.get().countDown();

        index = new Engine.Index(null, newUid("4"), doc);
        engine.index(index);
        engine.flush(new Engine.Flush());
        final long gen1 = store.readLastCommittedSegmentsInfo().getGeneration();
        // now, optimize and wait for merges, see that we have no merge flag
        engine.optimize(new Engine.Optimize().flush(true).maxNumSegments(1).waitForMerge(true));

        for (Segment segment : engine.segments()) {
            assertThat(segment.getMergeId(), nullValue());
        }
        // we could have multiple underlying merges, so the generation may increase more than once
        assertTrue(store.readLastCommittedSegmentsInfo().getGeneration() > gen1);

        // forcing an optimize will merge this single segment shard
        final boolean force = randomBoolean();
        if (force) {
            waitTillMerge.set(new CountDownLatch(1));
            waitForMerge.set(new CountDownLatch(1));
        }
        final boolean flush = randomBoolean();
        final long gen2 = store.readLastCommittedSegmentsInfo().getGeneration();
        engine.optimize(new Engine.Optimize().flush(flush).maxNumSegments(1).force(force).waitForMerge(false));
        waitTillMerge.get().await();
        for (Segment segment : engine.segments()) {
            assertThat(segment.getMergeId(), force ? notNullValue() : nullValue());
        }
        waitForMerge.get().countDown();
        
        if (flush) {
            awaitBusy(new Predicate<Object>() {
                @Override
                public boolean apply(Object o) {
                    try {
                        // we should have had just 1 merge, so last generation should be exact
                        return store.readLastCommittedSegmentsInfo().getLastGeneration() == gen2;
                    } catch (IOException e) {
                        throw ExceptionsHelper.convertToRuntime(e);
                    }
                }
            });
        }

        engine.close();
    }

    @Test
    public void testSimpleOperations() throws Exception {
        Engine.Searcher searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(0));
        searchResult.close();

        // create a document
        Document document = testDocumentWithTextField();
        document.add(new Field(SourceFieldMapper.NAME, B_1.toBytes(), SourceFieldMapper.Defaults.FIELD_TYPE));
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, document, Lucene.STANDARD_ANALYZER, B_1, false);
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
        engine.refresh(new Engine.Refresh("test").force(false));

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
        doc = testParsedDocument("1", "1", "test", null, -1, -1, document, Lucene.STANDARD_ANALYZER, B_2, false);
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
        engine.refresh(new Engine.Refresh("test").force(false));

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
        engine.refresh(new Engine.Refresh("test").force(false));

        searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(0));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 0));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test1")), 0));
        searchResult.close();

        // add it back
        document = testDocumentWithTextField();
        document.add(new Field(SourceFieldMapper.NAME, B_1.toBytes(), SourceFieldMapper.Defaults.FIELD_TYPE));
        doc = testParsedDocument("1", "1", "test", null, -1, -1, document, Lucene.STANDARD_ANALYZER, B_1, false);
        engine.create(new Engine.Create(null, newUid("1"), doc));

        // its not there...
        searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(0));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 0));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test1")), 0));
        searchResult.close();

        // refresh and it should be there
        engine.refresh(new Engine.Refresh("test").force(false));

        // now its there...
        searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(1));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 1));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test1")), 0));
        searchResult.close();

        // now flush
        engine.flush(new Engine.Flush());

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
        doc = testParsedDocument("1", "1", "test", null, -1, -1, document, Lucene.STANDARD_ANALYZER, B_1, false);
        engine.index(new Engine.Index(null, newUid("1"), doc));

        // its not updated yet...
        searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(1));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 1));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test1")), 0));
        searchResult.close();

        // refresh and it should be updated
        engine.refresh(new Engine.Refresh("test").force(false));

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
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocumentWithTextField(), Lucene.STANDARD_ANALYZER, B_1, false);
        engine.create(new Engine.Create(null, newUid("1"), doc));

        // its not there...
        searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(0));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 0));
        searchResult.close();

        // refresh and it should be there
        engine.refresh(new Engine.Refresh("test").force(false));

        // now its there...
        searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(1));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 1));
        // don't release the search result yet...

        // delete, refresh and do a new search, it should not be there
        engine.delete(new Engine.Delete("test", "1", newUid("1")));
        engine.refresh(new Engine.Refresh("test").force(false));
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
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocumentWithTextField(), Lucene.STANDARD_ANALYZER, B_1, false);
        engine.create(new Engine.Create(null, newUid("1"), doc));
        engine.flush(new Engine.Flush());
        final boolean failEngine = defaultSettings.getAsBoolean(InternalEngine.INDEX_FAIL_ON_CORRUPTION, false);
        final int failInPhase = randomIntBetween(1,3);
        try {
            engine.recover(new Engine.RecoveryHandler() {
                @Override
                public void phase1(SnapshotIndexCommit snapshot) throws EngineException {
                   if (failInPhase == 1) {
                       throw new RuntimeException("bar", new CorruptIndexException("Foo"));
                   }
                }

                @Override
                public void phase2(Translog.Snapshot snapshot) throws EngineException {
                    if (failInPhase == 2) {
                        throw new RuntimeException("bar", new CorruptIndexException("Foo"));
                    }
                }

                @Override
                public void phase3(Translog.Snapshot snapshot) throws EngineException {
                    if (failInPhase == 3) {
                        throw new RuntimeException("bar", new CorruptIndexException("Foo"));
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

            ParsedDocument doc2 = testParsedDocument("2", "2", "test", null, -1, -1, testDocumentWithTextField(), Lucene.STANDARD_ANALYZER, B_2, false);
            engine.create(new Engine.Create(null, newUid("2"), doc2));
            engine.refresh(new Engine.Refresh("foo"));

            searchResult = engine.acquireSearcher("test");
            MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 2));
            MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(2));
            searchResult.close();
            assertThat(failEngine, is(false));
        } catch (EngineClosedException ex) {
            assertThat(failEngine, is(true));
        }
    }


    @Test
    public void testSimpleRecover() throws Exception {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocumentWithTextField(), Lucene.STANDARD_ANALYZER, B_1, false);
        engine.create(new Engine.Create(null, newUid("1"), doc));
        engine.flush(new Engine.Flush());

        engine.recover(new Engine.RecoveryHandler() {
            @Override
            public void phase1(SnapshotIndexCommit snapshot) throws EngineException {
                try {
                    engine.flush(new Engine.Flush());
                    assertThat("flush is not allowed in phase 3", false, equalTo(true));
                } catch (FlushNotAllowedEngineException e) {
                    // all is well
                }
            }

            @Override
            public void phase2(Translog.Snapshot snapshot) throws EngineException {
                MatcherAssert.assertThat(snapshot, TranslogSizeMatcher.translogSize(0));
                try {
                    engine.flush(new Engine.Flush());
                    assertThat("flush is not allowed in phase 3", false, equalTo(true));
                } catch (FlushNotAllowedEngineException e) {
                    // all is well
                }
            }

            @Override
            public void phase3(Translog.Snapshot snapshot) throws EngineException {
                MatcherAssert.assertThat(snapshot, TranslogSizeMatcher.translogSize(0));
                try {
                    // we can do this here since we are on the same thread
                    engine.flush(new Engine.Flush());
                    assertThat("flush is not allowed in phase 3", false, equalTo(true));
                } catch (FlushNotAllowedEngineException e) {
                    // all is well
                }
            }
        });

        engine.flush(new Engine.Flush());
        engine.close();
    }

    @Test
    public void testRecoverWithOperationsBetweenPhase1AndPhase2() throws Exception {
        ParsedDocument doc1 = testParsedDocument("1", "1", "test", null, -1, -1, testDocumentWithTextField(), Lucene.STANDARD_ANALYZER, B_1, false);
        engine.create(new Engine.Create(null, newUid("1"), doc1));
        engine.flush(new Engine.Flush());
        ParsedDocument doc2 = testParsedDocument("2", "2", "test", null, -1, -1, testDocumentWithTextField(), Lucene.STANDARD_ANALYZER, B_2, false);
        engine.create(new Engine.Create(null, newUid("2"), doc2));

        engine.recover(new Engine.RecoveryHandler() {
            @Override
            public void phase1(SnapshotIndexCommit snapshot) throws EngineException {
            }

            @Override
            public void phase2(Translog.Snapshot snapshot) throws EngineException {
                Translog.Create create = (Translog.Create) snapshot.next();
                assertThat("translog snapshot should not read null", create != null, equalTo(true));
                assertThat(create.source().toBytesArray(), equalTo(B_2));
                assertThat(snapshot.next(), equalTo(null));
            }

            @Override
            public void phase3(Translog.Snapshot snapshot) throws EngineException {
                MatcherAssert.assertThat(snapshot, TranslogSizeMatcher.translogSize(0));
            }
        });

        engine.flush(new Engine.Flush());
        engine.close();
    }

    @Test
    public void testRecoverWithOperationsBetweenPhase1AndPhase2AndPhase3() throws Exception {
        ParsedDocument doc1 = testParsedDocument("1", "1", "test", null, -1, -1, testDocumentWithTextField(), Lucene.STANDARD_ANALYZER, B_1, false);
        engine.create(new Engine.Create(null, newUid("1"), doc1));
        engine.flush(new Engine.Flush());
        ParsedDocument doc2 = testParsedDocument("2", "2", "test", null, -1, -1, testDocumentWithTextField(), Lucene.STANDARD_ANALYZER, B_2, false);
        engine.create(new Engine.Create(null, newUid("2"), doc2));

        engine.recover(new Engine.RecoveryHandler() {
            @Override
            public void phase1(SnapshotIndexCommit snapshot) throws EngineException {
            }

            @Override
            public void phase2(Translog.Snapshot snapshot) throws EngineException {
                Translog.Create create = (Translog.Create) snapshot.next();
                assertThat(create != null, equalTo(true));
                assertThat(snapshot.next(), equalTo(null));
                assertThat(create.source().toBytesArray(), equalTo(B_2));

                // add for phase3
                ParsedDocument doc3 = testParsedDocument("3", "3", "test", null, -1, -1, testDocumentWithTextField(), Lucene.STANDARD_ANALYZER, B_3, false);
                engine.create(new Engine.Create(null, newUid("3"), doc3));
            }

            @Override
            public void phase3(Translog.Snapshot snapshot) throws EngineException {
                Translog.Create create = (Translog.Create) snapshot.next();
                assertThat(create != null, equalTo(true));
                assertThat(snapshot.next(), equalTo(null));
                assertThat(create.source().toBytesArray(), equalTo(B_3));
            }
        });

        engine.flush(new Engine.Flush());
        engine.close();
    }

    @Test
    public void testVersioningNewCreate() {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), Lucene.STANDARD_ANALYZER, B_1, false);
        Engine.Create create = new Engine.Create(null, newUid("1"), doc);
        engine.create(create);
        assertThat(create.version(), equalTo(1l));

        create = new Engine.Create(null, newUid("1"), doc, create.version(), create.versionType().versionTypeForReplicationAndRecovery(), REPLICA, 0);
        replicaEngine.create(create);
        assertThat(create.version(), equalTo(1l));
    }

    @Test
    public void testExternalVersioningNewCreate() {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), Lucene.STANDARD_ANALYZER, B_1, false);
        Engine.Create create = new Engine.Create(null, newUid("1"), doc, 12, VersionType.EXTERNAL, Engine.Operation.Origin.PRIMARY, 0);
        engine.create(create);
        assertThat(create.version(), equalTo(12l));

        create = new Engine.Create(null, newUid("1"), doc, create.version(), create.versionType().versionTypeForReplicationAndRecovery(), REPLICA, 0);
        replicaEngine.create(create);
        assertThat(create.version(), equalTo(12l));
    }

    @Test
    public void testVersioningNewIndex() {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), Lucene.STANDARD_ANALYZER, B_1, false);
        Engine.Index index = new Engine.Index(null, newUid("1"), doc);
        engine.index(index);
        assertThat(index.version(), equalTo(1l));

        index = new Engine.Index(null, newUid("1"), doc, index.version(), index.versionType().versionTypeForReplicationAndRecovery(), REPLICA, 0);
        replicaEngine.index(index);
        assertThat(index.version(), equalTo(1l));
    }

    @Test
    public void testExternalVersioningNewIndex() {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), Lucene.STANDARD_ANALYZER, B_1, false);
        Engine.Index index = new Engine.Index(null, newUid("1"), doc, 12, VersionType.EXTERNAL, PRIMARY, 0);
        engine.index(index);
        assertThat(index.version(), equalTo(12l));

        index = new Engine.Index(null, newUid("1"), doc, index.version(), index.versionType().versionTypeForReplicationAndRecovery(), REPLICA, 0);
        replicaEngine.index(index);
        assertThat(index.version(), equalTo(12l));
    }

    @Test
    public void testVersioningIndexConflict() {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), Lucene.STANDARD_ANALYZER, B_1, false);
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
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), Lucene.STANDARD_ANALYZER, B_1, false);
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
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), Lucene.STANDARD_ANALYZER, B_1, false);
        Engine.Index index = new Engine.Index(null, newUid("1"), doc);
        engine.index(index);
        assertThat(index.version(), equalTo(1l));

        index = new Engine.Index(null, newUid("1"), doc);
        engine.index(index);
        assertThat(index.version(), equalTo(2l));

        engine.flush(new Engine.Flush());

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
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), Lucene.STANDARD_ANALYZER, B_1, false);
        Engine.Index index = new Engine.Index(null, newUid("1"), doc, 12, VersionType.EXTERNAL, PRIMARY, 0);
        engine.index(index);
        assertThat(index.version(), equalTo(12l));

        index = new Engine.Index(null, newUid("1"), doc, 14, VersionType.EXTERNAL, PRIMARY, 0);
        engine.index(index);
        assertThat(index.version(), equalTo(14l));

        engine.flush(new Engine.Flush());

        index = new Engine.Index(null, newUid("1"), doc, 13, VersionType.EXTERNAL, PRIMARY, 0);
        try {
            engine.index(index);
            fail();
        } catch (VersionConflictEngineException e) {
            // all is well
        }
    }

    @Test
    public void testVersioningDeleteConflict() {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), Lucene.STANDARD_ANALYZER, B_1, false);
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
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), Lucene.STANDARD_ANALYZER, B_1, false);
        Engine.Index index = new Engine.Index(null, newUid("1"), doc);
        engine.index(index);
        assertThat(index.version(), equalTo(1l));

        index = new Engine.Index(null, newUid("1"), doc);
        engine.index(index);
        assertThat(index.version(), equalTo(2l));

        engine.flush(new Engine.Flush());

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

        engine.flush(new Engine.Flush());

        // now actually delete
        delete = new Engine.Delete("test", "1", newUid("1"), 2l, VersionType.INTERNAL, PRIMARY, 0, false);
        engine.delete(delete);
        assertThat(delete.version(), equalTo(3l));

        engine.flush(new Engine.Flush());

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
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), Lucene.STANDARD_ANALYZER, B_1, false);
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
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), Lucene.STANDARD_ANALYZER, B_1, false);
        Engine.Create create = new Engine.Create(null, newUid("1"), doc, Versions.MATCH_ANY, VersionType.INTERNAL, PRIMARY, 0);
        engine.create(create);
        assertThat(create.version(), equalTo(1l));

        engine.flush(new Engine.Flush());

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
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), Lucene.STANDARD_ANALYZER, B_1, false);
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
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), Lucene.STANDARD_ANALYZER, B_1, false);
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
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), Lucene.STANDARD_ANALYZER, B_1, false);
        Engine.Index index = new Engine.Index(null, newUid("1"), doc);
        engine.index(index);
        assertTrue(index.created());

        index = new Engine.Index(null, newUid("1"), doc);
        engine.index(index);
        assertFalse(index.created());

        engine.delete(new Engine.Delete(null, "1", newUid("1")));

        index = new Engine.Index(null, newUid("1"), doc);
        engine.index(index);
        assertTrue(index.created());
    }

    @Test
    public void testCreatedFlagAfterFlush() {
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocument(), Lucene.STANDARD_ANALYZER, B_1, false);
        Engine.Index index = new Engine.Index(null, newUid("1"), doc);
        engine.index(index);
        assertTrue(index.created());

        engine.delete(new Engine.Delete(null, "1", newUid("1")));

        engine.flush(new Engine.Flush());

        index = new Engine.Index(null, newUid("1"), doc);
        engine.index(index);
        assertTrue(index.created());
    }

    private static class MockAppender extends AppenderSkeleton {
      public boolean sawIndexWriterMessage;

      @Override
      protected void append(LoggingEvent event) {
        if (event.getLevel() == Level.TRACE &&
            event.getLoggerName().endsWith("lucene.iw") &&
            event.getMessage().toString().contains("IW: apply all deletes during flush") &&
            event.getMessage().toString().contains("[index][1] ")) {

          sawIndexWriterMessage = true;
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
        MockAppender mockAppender = new MockAppender();

        Logger rootLogger = Logger.getRootLogger();
        Level savedLevel = rootLogger.getLevel();
        rootLogger.addAppender(mockAppender);
        rootLogger.setLevel(Level.DEBUG);

        try {
            // First, with DEBUG, which should NOT log IndexWriter output:
            ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, testDocumentWithTextField(), Lucene.STANDARD_ANALYZER, B_1, false);
            engine.create(new Engine.Create(null, newUid("1"), doc));
            engine.flush(new Engine.Flush());        
            assertFalse(mockAppender.sawIndexWriterMessage);

            // Again, with TRACE, which should log IndexWriter output:
            rootLogger.setLevel(Level.TRACE);
            engine.create(new Engine.Create(null, newUid("2"), doc));
            engine.flush(new Engine.Flush());        
            assertTrue(mockAppender.sawIndexWriterMessage);

        } finally {
            rootLogger.removeAppender(mockAppender);
            rootLogger.setLevel(savedLevel);
        }
    }

    @Slow
    @Test
    public void testEnableGcDeletes() throws Exception {

        // Make sure enableGCDeletes == false works:
        Settings settings = ImmutableSettings.builder()
                .put(InternalEngine.INDEX_GC_DELETES, "0ms")
                .build();

        Engine engine = new InternalEngine(shardId, settings, threadPool,
                                           engineSettingsService,
                                           new ShardIndexingService(shardId, settings,
                                                                    new ShardSlowLogIndexingService(shardId, EMPTY_SETTINGS, engineSettingsService)),
                                           null, store, createSnapshotDeletionPolicy(), createTranslog(), createMergePolicy(), createMergeScheduler(engineSettingsService),
                                           new AnalysisService(shardId.index()), new SimilarityService(shardId.index()), new CodecService(shardId.index()));
        engine.start();
        engine.enableGcDeletes(false);

        // Add document
        Document document = testDocument();
        document.add(new TextField("value", "test1", Field.Store.YES));

        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, document, Lucene.STANDARD_ANALYZER, B_2, false);
        engine.index(new Engine.Index(null, newUid("1"), doc, 1, VersionType.EXTERNAL, Engine.Operation.Origin.PRIMARY, System.nanoTime(), false));

        // Delete document we just added:
        engine.delete(new Engine.Delete("test", "1", newUid("1"), 10, VersionType.EXTERNAL, Engine.Operation.Origin.PRIMARY, System.nanoTime(), false));
        
        // Get should not find the document
        Engine.GetResult getResult = engine.get(new Engine.Get(true, newUid("1")));
        assertThat(getResult.exists(), equalTo(false));

        // Give the gc pruning logic a chance to kick in
        Thread.sleep(1000);

        if (randomBoolean()) {
            engine.refresh(new Engine.Refresh("test"));
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

    protected Term newUid(String id) {
        return new Term("_uid", id);
    }
}
