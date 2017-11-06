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

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.filter.RegexFilter;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LiveIndexWriterConfig;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.TestUtil;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.engine.Engine.Searcher;
import org.elasticsearch.index.fieldvisitor.FieldsVisitor;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperForType;
import org.elasticsearch.index.mapper.Mapper.BuilderContext;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.RootObjectMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.shard.IndexSearcherWrapper;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardUtils;
import org.elasticsearch.index.shard.TranslogRecoveryPerformer;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.index.store.DirectoryUtils;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogConfig;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.mapper.MapperRegistry;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.OldIndexUtils;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Collections.emptyMap;
import static java.util.Collections.shuffle;
import static org.elasticsearch.index.engine.Engine.Operation.Origin.PRIMARY;
import static org.elasticsearch.index.engine.Engine.Operation.Origin.REPLICA;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class InternalEngineTests extends EngineTestCase {

    public void testSegments() throws Exception {
        try (Store store = createStore();
            Engine engine = createEngine(defaultSettings, store, createTempDir(), NoMergePolicy.INSTANCE)) {
            List<Segment> segments = engine.segments(false);
            assertThat(segments.isEmpty(), equalTo(true));
            assertThat(engine.segmentsStats(false).getCount(), equalTo(0L));
            assertThat(engine.segmentsStats(false).getMemoryInBytes(), equalTo(0L));

            // create two docs and refresh
            ParsedDocument doc = testParsedDocument("1", "test", null, -1, -1, testDocumentWithTextField(), B_1, null);
            Engine.Index first = indexForDoc(doc);
            Engine.IndexResult firstResult = engine.index(first);
            ParsedDocument doc2 = testParsedDocument("2", "test", null, -1, -1, testDocumentWithTextField(), B_2, null);
            Engine.Index second = indexForDoc(doc2);
            Engine.IndexResult secondResult = engine.index(second);
            assertThat(secondResult.getTranslogLocation(), greaterThan(firstResult.getTranslogLocation()));
            engine.refresh("test");

            segments = engine.segments(false);
            assertThat(segments.size(), equalTo(1));
            SegmentsStats stats = engine.segmentsStats(false);
            assertThat(stats.getCount(), equalTo(1L));
            assertThat(stats.getTermsMemoryInBytes(), greaterThan(0L));
            assertThat(stats.getStoredFieldsMemoryInBytes(), greaterThan(0L));
            assertThat(stats.getTermVectorsMemoryInBytes(), equalTo(0L));
            assertThat(stats.getNormsMemoryInBytes(), greaterThan(0L));
            assertThat(stats.getDocValuesMemoryInBytes(), greaterThan(0L));
            assertThat(segments.get(0).isCommitted(), equalTo(false));
            assertThat(segments.get(0).isSearch(), equalTo(true));
            assertThat(segments.get(0).getNumDocs(), equalTo(2));
            assertThat(segments.get(0).getDeletedDocs(), equalTo(0));
            assertThat(segments.get(0).isCompound(), equalTo(true));
            assertThat(segments.get(0).ramTree, nullValue());

            engine.flush();

            segments = engine.segments(false);
            assertThat(segments.size(), equalTo(1));
            assertThat(engine.segmentsStats(false).getCount(), equalTo(1L));
            assertThat(segments.get(0).isCommitted(), equalTo(true));
            assertThat(segments.get(0).isSearch(), equalTo(true));
            assertThat(segments.get(0).getNumDocs(), equalTo(2));
            assertThat(segments.get(0).getDeletedDocs(), equalTo(0));
            assertThat(segments.get(0).isCompound(), equalTo(true));

            ParsedDocument doc3 = testParsedDocument("3", "test", null, -1, -1, testDocumentWithTextField(), B_3, null);
            engine.index(indexForDoc(doc3));
            engine.refresh("test");

            segments = engine.segments(false);
            assertThat(segments.size(), equalTo(2));
            assertThat(engine.segmentsStats(false).getCount(), equalTo(2L));
            assertThat(engine.segmentsStats(false).getTermsMemoryInBytes(), greaterThan(stats.getTermsMemoryInBytes()));
            assertThat(engine.segmentsStats(false).getStoredFieldsMemoryInBytes(), greaterThan(stats.getStoredFieldsMemoryInBytes()));
            assertThat(engine.segmentsStats(false).getTermVectorsMemoryInBytes(), equalTo(0L));
            assertThat(engine.segmentsStats(false).getNormsMemoryInBytes(), greaterThan(stats.getNormsMemoryInBytes()));
            assertThat(engine.segmentsStats(false).getDocValuesMemoryInBytes(), greaterThan(stats.getDocValuesMemoryInBytes()));
            assertThat(segments.get(0).getGeneration() < segments.get(1).getGeneration(), equalTo(true));
            assertThat(segments.get(0).isCommitted(), equalTo(true));
            assertThat(segments.get(0).isSearch(), equalTo(true));
            assertThat(segments.get(0).getNumDocs(), equalTo(2));
            assertThat(segments.get(0).getDeletedDocs(), equalTo(0));
            assertThat(segments.get(0).isCompound(), equalTo(true));


            assertThat(segments.get(1).isCommitted(), equalTo(false));
            assertThat(segments.get(1).isSearch(), equalTo(true));
            assertThat(segments.get(1).getNumDocs(), equalTo(1));
            assertThat(segments.get(1).getDeletedDocs(), equalTo(0));
            assertThat(segments.get(1).isCompound(), equalTo(true));


            engine.delete(new Engine.Delete("test", "1", newUid(doc)));
            engine.refresh("test");

            segments = engine.segments(false);
            assertThat(segments.size(), equalTo(2));
            assertThat(engine.segmentsStats(false).getCount(), equalTo(2L));
            assertThat(segments.get(0).getGeneration() < segments.get(1).getGeneration(), equalTo(true));
            assertThat(segments.get(0).isCommitted(), equalTo(true));
            assertThat(segments.get(0).isSearch(), equalTo(true));
            assertThat(segments.get(0).getNumDocs(), equalTo(1));
            assertThat(segments.get(0).getDeletedDocs(), equalTo(1));
            assertThat(segments.get(0).isCompound(), equalTo(true));

            assertThat(segments.get(1).isCommitted(), equalTo(false));
            assertThat(segments.get(1).isSearch(), equalTo(true));
            assertThat(segments.get(1).getNumDocs(), equalTo(1));
            assertThat(segments.get(1).getDeletedDocs(), equalTo(0));
            assertThat(segments.get(1).isCompound(), equalTo(true));

            engine.onSettingsChanged();
            ParsedDocument doc4 = testParsedDocument("4", "test", null, -1, -1, testDocumentWithTextField(), B_3, null);
            engine.index(indexForDoc(doc4));
            engine.refresh("test");

            segments = engine.segments(false);
            assertThat(segments.size(), equalTo(3));
            assertThat(engine.segmentsStats(false).getCount(), equalTo(3L));
            assertThat(segments.get(0).getGeneration() < segments.get(1).getGeneration(), equalTo(true));
            assertThat(segments.get(0).isCommitted(), equalTo(true));
            assertThat(segments.get(0).isSearch(), equalTo(true));
            assertThat(segments.get(0).getNumDocs(), equalTo(1));
            assertThat(segments.get(0).getDeletedDocs(), equalTo(1));
            assertThat(segments.get(0).isCompound(), equalTo(true));

            assertThat(segments.get(1).isCommitted(), equalTo(false));
            assertThat(segments.get(1).isSearch(), equalTo(true));
            assertThat(segments.get(1).getNumDocs(), equalTo(1));
            assertThat(segments.get(1).getDeletedDocs(), equalTo(0));
            assertThat(segments.get(1).isCompound(), equalTo(true));

            assertThat(segments.get(2).isCommitted(), equalTo(false));
            assertThat(segments.get(2).isSearch(), equalTo(true));
            assertThat(segments.get(2).getNumDocs(), equalTo(1));
            assertThat(segments.get(2).getDeletedDocs(), equalTo(0));
            assertThat(segments.get(2).isCompound(), equalTo(true));
        }
    }

    public void testVerboseSegments() throws Exception {
        try (Store store = createStore();
             Engine engine = createEngine(defaultSettings, store, createTempDir(), NoMergePolicy.INSTANCE)) {
            List<Segment> segments = engine.segments(true);
            assertThat(segments.isEmpty(), equalTo(true));

            ParsedDocument doc = testParsedDocument("1", "test", null, -1, -1, testDocumentWithTextField(), B_1, null);
            engine.index(indexForDoc(doc));
            engine.refresh("test");

            segments = engine.segments(true);
            assertThat(segments.size(), equalTo(1));
            assertThat(segments.get(0).ramTree, notNullValue());

            ParsedDocument doc2 = testParsedDocument("2", "test", null, -1, -1, testDocumentWithTextField(), B_2, null);
            engine.index(indexForDoc(doc2));
            engine.refresh("test");
            ParsedDocument doc3 = testParsedDocument("3", "test", null, -1, -1, testDocumentWithTextField(), B_3, null);
            engine.index(indexForDoc(doc3));
            engine.refresh("test");

            segments = engine.segments(true);
            assertThat(segments.size(), equalTo(3));
            assertThat(segments.get(0).ramTree, notNullValue());
            assertThat(segments.get(1).ramTree, notNullValue());
            assertThat(segments.get(2).ramTree, notNullValue());
        }
    }

    public void testSegmentsWithMergeFlag() throws Exception {
        try (Store store = createStore();
            Engine engine = createEngine(defaultSettings, store, createTempDir(), new TieredMergePolicy())) {
            ParsedDocument doc = testParsedDocument("1", "test", null, -1, -1, testDocument(), B_1, null);
            Engine.Index index = indexForDoc(doc);
            engine.index(index);
            engine.flush();
            assertThat(engine.segments(false).size(), equalTo(1));
            index = indexForDoc(testParsedDocument("2", "test", null, -1, -1, testDocument(), B_1, null));
            engine.index(index);
            engine.flush();
            List<Segment> segments = engine.segments(false);
            assertThat(segments.size(), equalTo(2));
            for (Segment segment : segments) {
                assertThat(segment.getMergeId(), nullValue());
            }
            index = indexForDoc(testParsedDocument("3", "test", null, -1, -1, testDocument(), B_1, null));
            engine.index(index);
            engine.flush();
            segments = engine.segments(false);
            assertThat(segments.size(), equalTo(3));
            for (Segment segment : segments) {
                assertThat(segment.getMergeId(), nullValue());
            }

            index = indexForDoc(doc);
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
                assertEquals(gen2, store.readLastCommittedSegmentsInfo().getLastGeneration());
            }
        }
    }

    public void testSegmentsStatsIncludingFileSizes() throws Exception {
        try (Store store = createStore();
            Engine engine = createEngine(defaultSettings, store, createTempDir(), NoMergePolicy.INSTANCE)) {
            assertThat(engine.segmentsStats(true).getFileSizes().size(), equalTo(0));

            ParsedDocument doc = testParsedDocument("1", "test", null, -1, -1, testDocumentWithTextField(), B_1, null);
            engine.index(indexForDoc(doc));
            engine.refresh("test");

            SegmentsStats stats = engine.segmentsStats(true);
            assertThat(stats.getFileSizes().size(), greaterThan(0));
            assertThat(() -> stats.getFileSizes().valuesIt(), everyItem(greaterThan(0L)));

            ObjectObjectCursor<String, Long> firstEntry = stats.getFileSizes().iterator().next();

            ParsedDocument doc2 = testParsedDocument("2", "test", null, -1, -1, testDocumentWithTextField(), B_2, null);
            engine.index(indexForDoc(doc2));
            engine.refresh("test");

            assertThat(engine.segmentsStats(true).getFileSizes().get(firstEntry.key), greaterThan(firstEntry.value));
        }
    }

    public void testCommitStats() throws Exception {
        Document document = testDocumentWithTextField();
        document.add(new Field(SourceFieldMapper.NAME, BytesReference.toBytes(B_1), SourceFieldMapper.Defaults.FIELD_TYPE));
        ParsedDocument doc = testParsedDocument("1", "test", null, -1, -1, document, B_1, null);
        engine.index(indexForDoc(doc));

        CommitStats stats1 = engine.commitStats();
        assertThat(stats1.getGeneration(), greaterThan(0L));
        assertThat(stats1.getId(), notNullValue());
        assertThat(stats1.getUserData(), hasKey(Translog.TRANSLOG_GENERATION_KEY));

        engine.flush(true, true);
        CommitStats stats2 = engine.commitStats();
        assertThat(stats2.getGeneration(), greaterThan(stats1.getGeneration()));
        assertThat(stats2.getId(), notNullValue());
        assertThat(stats2.getId(), not(equalTo(stats1.getId())));
        assertThat(stats2.getUserData(), hasKey(Translog.TRANSLOG_GENERATION_KEY));
        assertThat(stats2.getUserData(), hasKey(Translog.TRANSLOG_UUID_KEY));
        assertThat(stats2.getUserData().get(Translog.TRANSLOG_GENERATION_KEY), not(equalTo(stats1.getUserData().get(Translog.TRANSLOG_GENERATION_KEY))));
        assertThat(stats2.getUserData().get(Translog.TRANSLOG_UUID_KEY), equalTo(stats1.getUserData().get(Translog.TRANSLOG_UUID_KEY)));
    }

    public void testIndexSearcherWrapper() throws Exception {
        final AtomicInteger counter = new AtomicInteger();
        IndexSearcherWrapper wrapper = new IndexSearcherWrapper() {

            @Override
            public DirectoryReader wrap(DirectoryReader reader) {
                counter.incrementAndGet();
                return reader;
            }

            @Override
            public IndexSearcher wrap(IndexSearcher searcher) throws EngineException {
                counter.incrementAndGet();
                return searcher;
            }
        };
        Store store = createStore();
        Path translog = createTempDir("translog-test");
        InternalEngine engine = createEngine(store, translog);
        engine.close();

        engine = new InternalEngine(copy(engine.config(), EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG));
        assertTrue(engine.isRecovering());
        engine.recoverFromTranslog();
        Engine.Searcher searcher = wrapper.wrap(engine.acquireSearcher("test"));
        assertThat(counter.get(), equalTo(2));
        searcher.close();
        IOUtils.close(store, engine);
    }

    public void testFlushIsDisabledDuringTranslogRecovery() throws IOException {
        assertFalse(engine.isRecovering());
        ParsedDocument doc = testParsedDocument("1", "test", null, -1, -1, testDocumentWithTextField(), SOURCE, null);
        engine.index(indexForDoc(doc));
        engine.close();

        engine = new InternalEngine(copy(engine.config(), EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG));
        expectThrows(IllegalStateException.class, () -> engine.flush(true, true));
        assertTrue(engine.isRecovering());
        engine.recoverFromTranslog();
        assertFalse(engine.isRecovering());
        doc = testParsedDocument("2", "test", null, -1, -1, testDocumentWithTextField(), SOURCE, null);
        engine.index(indexForDoc(doc));
        engine.flush();
    }

    public void testTranslogMultipleOperationsSameDocument() throws IOException {
        final int ops = randomIntBetween(1, 32);
        Engine initialEngine;
        final List<Engine.Operation> operations = new ArrayList<>();
        try {
            initialEngine = engine;
            for (int i = 0; i < ops; i++) {
                final ParsedDocument doc = testParsedDocument("1", "test", null, -1, -1, testDocumentWithTextField(), new BytesArray("{}".getBytes(Charset.defaultCharset())), null);
                if (randomBoolean()) {
                    final Engine.Index operation = new Engine.Index(newUid(doc), doc, i, VersionType.EXTERNAL, Engine.Operation.Origin.PRIMARY, System.nanoTime(), -1, false);
                    operations.add(operation);
                    initialEngine.index(operation);
                } else {
                    final Engine.Delete operation = new Engine.Delete("test", "1", newUid(doc), i, VersionType.EXTERNAL, Engine.Operation.Origin.PRIMARY, System.nanoTime());
                    operations.add(operation);
                    initialEngine.delete(operation);
                }
            }
        } finally {
            IOUtils.close(engine);
        }

        Engine recoveringEngine = null;
        try {
            recoveringEngine = new InternalEngine(copy(engine.config(), EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG));
            recoveringEngine.recoverFromTranslog();
            try (Engine.Searcher searcher = recoveringEngine.acquireSearcher("test")) {
                final TotalHitCountCollector collector = new TotalHitCountCollector();
                searcher.searcher().search(new MatchAllDocsQuery(), collector);
                assertThat(collector.getTotalHits(), equalTo(operations.get(operations.size() - 1) instanceof Engine.Delete ? 0 : 1));
            }
        } finally {
            IOUtils.close(recoveringEngine);
        }
    }

    public void testTranslogRecoveryDoesNotReplayIntoTranslog() throws IOException {
        final int docs = randomIntBetween(1, 32);
        Engine initialEngine = null;
        try {
            initialEngine = engine;
            for (int i = 0; i < docs; i++) {
                final String id = Integer.toString(i);
                final ParsedDocument doc = testParsedDocument(id, "test", null, -1, -1, testDocumentWithTextField(), new BytesArray("{}".getBytes(Charset.defaultCharset())), null);
                initialEngine.index(indexForDoc(doc));
            }
        } finally {
            IOUtils.close(initialEngine);
        }

        Engine recoveringEngine = null;
        try {
            final AtomicBoolean flushed = new AtomicBoolean();
            recoveringEngine = new InternalEngine(copy(engine.config(), EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG)) {
                @Override
                public CommitId flush(boolean force, boolean waitIfOngoing) throws EngineException {
                    assertThat(getTranslog().totalOperations(), equalTo(docs));
                    final CommitId commitId = super.flush(force, waitIfOngoing);
                    flushed.set(true);
                    return commitId;
                }
            };

            assertThat(recoveringEngine.getTranslog().totalOperations(), equalTo(docs));
            recoveringEngine.recoverFromTranslog();
            assertTrue(flushed.get());
        } finally {
            IOUtils.close(recoveringEngine);
        }
    }

    public void testConcurrentGetAndFlush() throws Exception {
        ParsedDocument doc = testParsedDocument("1", "test", null, -1, -1, testDocumentWithTextField(), B_1, null);
        engine.index(indexForDoc(doc));

        final AtomicReference<Engine.GetResult> latestGetResult = new AtomicReference<>();
        final Function<String, Searcher> searcherFactory = engine::acquireSearcher;
        latestGetResult.set(engine.get(new Engine.Get(true, doc.type(), doc.id(), newUid(doc)), searcherFactory));
        final AtomicBoolean flushFinished = new AtomicBoolean(false);
        final CyclicBarrier barrier = new CyclicBarrier(2);
        Thread getThread = new Thread(() -> {
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                throw new RuntimeException(e);
            }
            while (flushFinished.get() == false) {
                Engine.GetResult previousGetResult = latestGetResult.get();
                if (previousGetResult != null) {
                    previousGetResult.release();
                }
                latestGetResult.set(engine.get(new Engine.Get(true, doc.type(), doc.id(), newUid(doc)), searcherFactory));
                if (latestGetResult.get().exists() == false) {
                    break;
                }
            }
        });
        getThread.start();
        barrier.await();
        engine.flush();
        flushFinished.set(true);
        getThread.join();
        assertTrue(latestGetResult.get().exists());
        latestGetResult.get().release();
    }

    public void testSimpleOperations() throws Exception {
        Engine.Searcher searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(0));
        searchResult.close();

        final Function<String, Searcher> searcherFactory = engine::acquireSearcher;

        // create a document
        Document document = testDocumentWithTextField();
        document.add(new Field(SourceFieldMapper.NAME, BytesReference.toBytes(B_1), SourceFieldMapper.Defaults.FIELD_TYPE));
        ParsedDocument doc = testParsedDocument("1", "test", null, -1, -1, document, B_1, null);
        engine.index(indexForDoc(doc));

        // its not there...
        searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(0));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 0));
        searchResult.close();

        // but, not there non realtime
        Engine.GetResult getResult = engine.get(new Engine.Get(false, doc.type(), doc.id(), newUid(doc)), searcherFactory);
        assertThat(getResult.exists(), equalTo(false));
        getResult.release();

        // but, we can still get it (in realtime)
        getResult = engine.get(new Engine.Get(true, doc.type(), doc.id(), newUid(doc)), searcherFactory);
        assertThat(getResult.exists(), equalTo(true));
        assertThat(getResult.docIdAndVersion(), notNullValue());
        getResult.release();

        // refresh and it should be there
        engine.refresh("test");

        // now its there...
        searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(1));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 1));
        searchResult.close();

        // also in non realtime
        getResult = engine.get(new Engine.Get(false, doc.type(), doc.id(), newUid(doc)), searcherFactory);
        assertThat(getResult.exists(), equalTo(true));
        assertThat(getResult.docIdAndVersion(), notNullValue());
        getResult.release();

        // now do an update
        document = testDocument();
        document.add(new TextField("value", "test1", Field.Store.YES));
        document.add(new Field(SourceFieldMapper.NAME, BytesReference.toBytes(B_2), SourceFieldMapper.Defaults.FIELD_TYPE));
        doc = testParsedDocument("1", "test", null, -1, -1, document, B_2, null);
        engine.index(indexForDoc(doc));

        // its not updated yet...
        searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(1));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 1));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test1")), 0));
        searchResult.close();

        // but, we can still get it (in realtime)
        getResult = engine.get(new Engine.Get(true, doc.type(), doc.id(), newUid(doc)), searcherFactory);
        assertThat(getResult.exists(), equalTo(true));
        assertThat(getResult.docIdAndVersion(), notNullValue());
        getResult.release();

        // refresh and it should be updated
        engine.refresh("test");

        searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(1));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 0));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test1")), 1));
        searchResult.close();

        // now delete
        engine.delete(new Engine.Delete("test", "1", newUid(doc)));

        // its not deleted yet
        searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(1));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 0));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test1")), 1));
        searchResult.close();

        // but, get should not see it (in realtime)
        getResult = engine.get(new Engine.Get(true, doc.type(), doc.id(), newUid(doc)), searcherFactory);
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
        document.add(new Field(SourceFieldMapper.NAME, BytesReference.toBytes(B_1), SourceFieldMapper.Defaults.FIELD_TYPE));
        doc = testParsedDocument("1", "test", null, -1, -1, document, B_1, null);
        engine.index(new Engine.Index(newUid(doc), doc, Versions.MATCH_DELETED));

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
        getResult = engine.get(new Engine.Get(true, doc.type(), doc.id(), newUid(doc)), searcherFactory);
        assertThat(getResult.exists(), equalTo(true));
        assertThat(getResult.docIdAndVersion(), notNullValue());
        getResult.release();

        // make sure we can still work with the engine
        // now do an update
        document = testDocument();
        document.add(new TextField("value", "test1", Field.Store.YES));
        doc = testParsedDocument("1", "test", null, -1, -1, document, B_1, null);
        engine.index(indexForDoc(doc));

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
    }

    public void testSearchResultRelease() throws Exception {
        Engine.Searcher searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(0));
        searchResult.close();

        // create a document
        ParsedDocument doc = testParsedDocument("1", "test", null, -1, -1, testDocumentWithTextField(), B_1, null);
        engine.index(indexForDoc(doc));

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
        engine.delete(new Engine.Delete("test", "1", newUid(doc)));
        engine.refresh("test");
        Engine.Searcher updateSearchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(updateSearchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(0));
        updateSearchResult.close();

        // the non release search result should not see the deleted yet...
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(1));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 1));
        searchResult.close();
    }

    public void testSyncedFlush() throws IOException {
        try (Store store = createStore();
            Engine engine = new InternalEngine(config(defaultSettings, store, createTempDir(),
                     new LogByteSizeMergePolicy(), IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, null))) {
            final String syncId = randomUnicodeOfCodepointLengthBetween(10, 20);
            ParsedDocument doc = testParsedDocument("1", "test", null, -1, -1, testDocumentWithTextField(), B_1, null);
            engine.index(indexForDoc(doc));
            Engine.CommitId commitID = engine.flush();
            assertThat(commitID, equalTo(new Engine.CommitId(store.readLastCommittedSegmentsInfo().getId())));
            byte[] wrongBytes = Base64.getDecoder().decode(commitID.toString());
            wrongBytes[0] = (byte) ~wrongBytes[0];
            Engine.CommitId wrongId = new Engine.CommitId(wrongBytes);
            assertEquals("should fail to sync flush with wrong id (but no docs)", engine.syncFlush(syncId + "1", wrongId),
                    Engine.SyncedFlushResult.COMMIT_MISMATCH);
            engine.index(indexForDoc(doc));
            assertEquals("should fail to sync flush with right id but pending doc", engine.syncFlush(syncId + "2", commitID),
                    Engine.SyncedFlushResult.PENDING_OPERATIONS);
            commitID = engine.flush();
            assertEquals("should succeed to flush commit with right id and no pending doc", engine.syncFlush(syncId, commitID),
                    Engine.SyncedFlushResult.SUCCESS);
            assertEquals(store.readLastCommittedSegmentsInfo().getUserData().get(Engine.SYNC_COMMIT_ID), syncId);
            assertEquals(engine.getLastCommittedSegmentInfos().getUserData().get(Engine.SYNC_COMMIT_ID), syncId);
        }
    }

    public void testRenewSyncFlush() throws Exception {
        final int iters = randomIntBetween(2, 5); // run this a couple of times to get some coverage
        for (int i = 0; i < iters; i++) {
            try (Store store = createStore();
                 InternalEngine engine = new InternalEngine(config(defaultSettings, store, createTempDir(),
                         new LogDocMergePolicy(), IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, null))) {
                final String syncId = randomUnicodeOfCodepointLengthBetween(10, 20);
                ParsedDocument doc = testParsedDocument("1", "test", null, -1, -1, testDocumentWithTextField(), B_1, null);
                Engine.Index doc1 = indexForDoc(doc);
                engine.index(doc1);
                assertEquals(engine.getLastWriteNanos(), doc1.startTime());
                engine.flush();
                Engine.Index doc2 = indexForDoc(testParsedDocument("2", "test", null, -1, -1, testDocumentWithTextField(), B_1, null));
                engine.index(doc2);
                assertEquals(engine.getLastWriteNanos(), doc2.startTime());
                engine.flush();
                final boolean forceMergeFlushes = randomBoolean();
                final ParsedDocument parsedDoc3 = testParsedDocument("3", "test", null, -1, -1, testDocumentWithTextField(), B_1, null);
                if (forceMergeFlushes) {
                    engine.index(new Engine.Index(newUid(parsedDoc3), parsedDoc3, Versions.MATCH_ANY, VersionType.INTERNAL, Engine.Operation.Origin.PRIMARY, System.nanoTime() - engine.engineConfig.getFlushMergesAfter().nanos(), -1, false));
                } else {
                    engine.index(indexForDoc(parsedDoc3));
                }
                Engine.CommitId commitID = engine.flush();
                assertEquals("should succeed to flush commit with right id and no pending doc", engine.syncFlush(syncId, commitID),
                        Engine.SyncedFlushResult.SUCCESS);
                assertEquals(3, engine.segments(false).size());

                engine.forceMerge(forceMergeFlushes, 1, false, false, false);
                if (forceMergeFlushes == false) {
                    engine.refresh("make all segments visible");
                    assertEquals(4, engine.segments(false).size());
                    assertEquals(store.readLastCommittedSegmentsInfo().getUserData().get(Engine.SYNC_COMMIT_ID), syncId);
                    assertEquals(engine.getLastCommittedSegmentInfos().getUserData().get(Engine.SYNC_COMMIT_ID), syncId);
                    assertTrue(engine.tryRenewSyncCommit());
                    assertEquals(1, engine.segments(false).size());
                } else {
                    assertBusy(() -> assertEquals(1, engine.segments(false).size()));
                }
                assertEquals(store.readLastCommittedSegmentsInfo().getUserData().get(Engine.SYNC_COMMIT_ID), syncId);
                assertEquals(engine.getLastCommittedSegmentInfos().getUserData().get(Engine.SYNC_COMMIT_ID), syncId);

                if (randomBoolean()) {
                    Engine.Index doc4 = indexForDoc(testParsedDocument("4", "test", null, -1, -1, testDocumentWithTextField(), B_1, null));
                    engine.index(doc4);
                    assertEquals(engine.getLastWriteNanos(), doc4.startTime());
                } else {
                    Engine.Delete delete = new Engine.Delete(doc1.type(), doc1.id(), doc1.uid());
                    engine.delete(delete);
                    assertEquals(engine.getLastWriteNanos(), delete.startTime());
                }
                assertFalse(engine.tryRenewSyncCommit());
                engine.flush(false, true); // we might hit a concurrent flush from a finishing merge here - just wait if ongoing...
                assertNull(store.readLastCommittedSegmentsInfo().getUserData().get(Engine.SYNC_COMMIT_ID));
                assertNull(engine.getLastCommittedSegmentInfos().getUserData().get(Engine.SYNC_COMMIT_ID));
            }
        }
    }

    public void testSycnedFlushSurvivesEngineRestart() throws IOException {
        final String syncId = randomUnicodeOfCodepointLengthBetween(10, 20);
        ParsedDocument doc = testParsedDocument("1", "test", null, -1, -1, testDocumentWithTextField(), B_1, null);
        engine.index(indexForDoc(doc));
        final Engine.CommitId commitID = engine.flush();
        assertEquals("should succeed to flush commit with right id and no pending doc", engine.syncFlush(syncId, commitID),
                Engine.SyncedFlushResult.SUCCESS);
        assertEquals(store.readLastCommittedSegmentsInfo().getUserData().get(Engine.SYNC_COMMIT_ID), syncId);
        assertEquals(engine.getLastCommittedSegmentInfos().getUserData().get(Engine.SYNC_COMMIT_ID), syncId);
        EngineConfig config = engine.config();
        if (randomBoolean()) {
            engine.close();
        } else {
            engine.flushAndClose();
        }
        engine = new InternalEngine(copy(config, randomFrom(EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG, EngineConfig.OpenMode.OPEN_INDEX_CREATE_TRANSLOG)));

        if (engine.config().getOpenMode() == EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG && randomBoolean()) {
            engine.recoverFromTranslog();
        }
        assertEquals(engine.config().getOpenMode().toString(), engine.getLastCommittedSegmentInfos().getUserData().get(Engine.SYNC_COMMIT_ID), syncId);
    }

    public void testSycnedFlushVanishesOnReplay() throws IOException {
        final String syncId = randomUnicodeOfCodepointLengthBetween(10, 20);
        ParsedDocument doc = testParsedDocument("1", "test", null, -1, -1, testDocumentWithTextField(), B_1, null);
        engine.index(indexForDoc(doc));
        final Engine.CommitId commitID = engine.flush();
        assertEquals("should succeed to flush commit with right id and no pending doc", engine.syncFlush(syncId, commitID),
                Engine.SyncedFlushResult.SUCCESS);
        assertEquals(store.readLastCommittedSegmentsInfo().getUserData().get(Engine.SYNC_COMMIT_ID), syncId);
        assertEquals(engine.getLastCommittedSegmentInfos().getUserData().get(Engine.SYNC_COMMIT_ID), syncId);
        doc = testParsedDocument("2", "test", null, -1, -1, testDocumentWithTextField(), new BytesArray("{}"), null);
        engine.index(indexForDoc(doc));
        EngineConfig config = engine.config();
        engine.close();
        engine = new InternalEngine(copy(config, EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG));
        engine.recoverFromTranslog();
        assertNull("Sync ID must be gone since we have a document to replay", engine.getLastCommittedSegmentInfos().getUserData().get(Engine.SYNC_COMMIT_ID));
    }

    public void testVersioningNewCreate() throws IOException {
        ParsedDocument doc = testParsedDocument("1", "test", null, -1, -1, testDocument(), B_1, null);
        Engine.Index create = new Engine.Index(newUid(doc), doc, Versions.MATCH_DELETED);
        Engine.IndexResult indexResult = engine.index(create);
        assertThat(indexResult.getVersion(), equalTo(1L));

        create = new Engine.Index(newUid(doc), doc, indexResult.getVersion(), create.versionType().versionTypeForReplicationAndRecovery(), REPLICA, 0, -1, false);
        Engine.IndexResult replicaResult = replicaEngine.index(create);
        assertThat(replicaResult.getVersion(), equalTo(1L));
    }

    public void testVersioningNewIndex() throws IOException {
        ParsedDocument doc = testParsedDocument("1", "test", null, -1, -1, testDocument(), B_1, null);
        Engine.Index index = indexForDoc(doc);
        Engine.IndexResult indexResult = engine.index(index);
        assertThat(indexResult.getVersion(), equalTo(1L));

        index = new Engine.Index(newUid(doc), doc, indexResult.getVersion(), index.versionType().versionTypeForReplicationAndRecovery(), REPLICA, 0, -1, false);
        Engine.IndexResult replicaResult = replicaEngine.index(index);
        assertThat(replicaResult.getVersion(), equalTo(1L));
    }

    public void testForceMerge() throws IOException {
        try (Store store = createStore();
            Engine engine = new InternalEngine(config(defaultSettings, store, createTempDir(),
                     new LogByteSizeMergePolicy(), IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, null))) { // use log MP here we test some behavior in ESMP
            int numDocs = randomIntBetween(10, 100);
            for (int i = 0; i < numDocs; i++) {
                ParsedDocument doc = testParsedDocument(Integer.toString(i), "test", null, -1, -1, testDocument(), B_1, null);
                Engine.Index index = indexForDoc(doc);
                engine.index(index);
                engine.refresh("test");
            }
            try (Engine.Searcher test = engine.acquireSearcher("test")) {
                assertEquals(numDocs, test.reader().numDocs());
            }
            engine.forceMerge(true, 1, false, false, false);
            assertEquals(engine.segments(true).size(), 1);

            ParsedDocument doc = testParsedDocument(Integer.toString(0), "test", null, -1, -1, testDocument(), B_1, null);
            Engine.Index index = indexForDoc(doc);
            engine.delete(new Engine.Delete(index.type(), index.id(), index.uid()));
            engine.forceMerge(true, 10, true, false, false); //expunge deletes

            assertEquals(engine.segments(true).size(), 1);
            try (Engine.Searcher test = engine.acquireSearcher("test")) {
                assertEquals(numDocs - 1, test.reader().numDocs());
                assertEquals(engine.config().getMergePolicy().toString(), numDocs - 1, test.reader().maxDoc());
            }

            doc = testParsedDocument(Integer.toString(1), "test", null, -1, -1, testDocument(), B_1, null);
            index = indexForDoc(doc);
            engine.delete(new Engine.Delete(index.type(), index.id(), index.uid()));
            engine.forceMerge(true, 10, false, false, false); //expunge deletes

            assertEquals(engine.segments(true).size(), 1);
            try (Engine.Searcher test = engine.acquireSearcher("test")) {
                assertEquals(numDocs - 2, test.reader().numDocs());
                assertEquals(numDocs - 1, test.reader().maxDoc());
            }
        }
    }

    public void testForceMergeAndClose() throws IOException, InterruptedException {
        int numIters = randomIntBetween(2, 10);
        for (int j = 0; j < numIters; j++) {
            try (Store store = createStore()) {
                final InternalEngine engine = createEngine(store, createTempDir());
                final CountDownLatch startGun = new CountDownLatch(1);
                final CountDownLatch indexed = new CountDownLatch(1);

                Thread thread = new Thread() {
                    @Override
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
                                    ParsedDocument doc = testParsedDocument(Integer.toString(i), "test", null, -1, -1, testDocument(), B_1, null);
                                    Engine.Index index = indexForDoc(doc);
                                    engine.index(index);
                                }
                                engine.refresh("test");
                                indexed.countDown();
                                try {
                                    engine.forceMerge(randomBoolean(), 1, false, randomBoolean(), randomBoolean());
                                } catch (IOException e) {
                                    return;
                                }
                            }
                        } catch (AlreadyClosedException ex) {
                            // fine
                        } catch (IOException e) {
                            throw new AssertionError(e);
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
                IOUtils.close(engine);
                thread.join();
            }
        }

    }

    public void testVersioningCreateExistsException() throws IOException {
        ParsedDocument doc = testParsedDocument("1", "test", null, System.currentTimeMillis(), -1, testDocument(), B_1, null);
        Engine.Index create = new Engine.Index(newUid(doc), doc, Versions.MATCH_DELETED, VersionType.INTERNAL, PRIMARY, 0, -1, false);
        Engine.IndexResult indexResult = engine.index(create);
        assertThat(indexResult.getVersion(), equalTo(1L));

        create = new Engine.Index(newUid(doc), doc, Versions.MATCH_DELETED, VersionType.INTERNAL, PRIMARY, 0, -1, false);
        indexResult = engine.index(create);
        assertTrue(indexResult.hasFailure());
        assertThat(indexResult.getFailure(), instanceOf(VersionConflictEngineException.class));

    }

    protected List<Engine.Operation> generateSingleDocHistory(boolean forReplica, VersionType versionType, int minOpCount, int maxOpCount) {
        final int numOfOps = randomIntBetween(minOpCount, maxOpCount);
        final List<Engine.Operation> ops = new ArrayList<>();
        final Term id = newUid(Uid.createUid("test", "1"));
        final String valuePrefix = forReplica ? "r_" : "p_";
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
                case FORCE:
                    version = randomNonNegativeLong();
                    break;
                default:
                    throw new UnsupportedOperationException("unknown version type: " + versionType);
            }
            if (randomBoolean()) {
                op = new Engine.Index(id, testParsedDocument("1", "test", null, System.currentTimeMillis(), -1L,
                    testDocumentWithTextField(valuePrefix + i), B_1, null),
                    version,
                    forReplica ? versionType.versionTypeForReplicationAndRecovery() : versionType,
                    forReplica ? REPLICA : PRIMARY,
                    System.currentTimeMillis(), -1, false
                );
            } else {
                op = new Engine.Delete("test", "1", id,
                    version,
                    forReplica ? versionType.versionTypeForReplicationAndRecovery() : versionType,
                    forReplica ? REPLICA : PRIMARY,
                    System.currentTimeMillis());
            }
            ops.add(op);
        }
        return ops;
    }

    public void testOutOfOrderDocsOnReplica() throws IOException {
        final List<Engine.Operation> ops = generateSingleDocHistory(true,
            randomFrom(VersionType.INTERNAL, VersionType.EXTERNAL), 2, 20);
        assertOpsOnReplica(ops, replicaEngine, true);
    }

    public void testNonStandardVersioningOnReplica() throws IOException {
        final List<Engine.Operation> ops = generateSingleDocHistory(true, randomFrom(VersionType.EXTERNAL_GTE, VersionType.FORCE), 2, 20);
        assertOpsOnReplica(ops, replicaEngine, false);
    }


    public void testOutOfOrderDocsOnReplicaOldPrimary() throws IOException {
        IndexSettings oldSettings = IndexSettingsModule.newIndexSettings("testOld", Settings.builder()
            .put(IndexSettings.INDEX_GC_DELETES_SETTING.getKey(), "1h") // make sure this doesn't kick in on us
            .put(EngineConfig.INDEX_CODEC_SETTING.getKey(), codecName)
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_5_4_0)
            .put(IndexSettings.MAX_REFRESH_LISTENERS_PER_SHARD.getKey(),
                between(10, 10 * IndexSettings.MAX_REFRESH_LISTENERS_PER_SHARD.get(Settings.EMPTY)))
            .build());

        try (Store oldReplicaStore = createStore();
             InternalEngine replicaEngine =
                 createEngine(oldSettings, oldReplicaStore, createTempDir("translog-old-replica"), newMergePolicy())) {
            final List<Engine.Operation> ops = generateSingleDocHistory(true, randomFrom(VersionType.INTERNAL, VersionType.EXTERNAL), 2, 20);
            assertOpsOnReplica(ops, replicaEngine, true);
        }
    }

    private void assertOpsOnReplica(List<Engine.Operation> ops, InternalEngine replicaEngine, boolean shuffleOps) throws IOException {
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
            shuffle(ops, random());
        }
        boolean firstOp = true;
        for (Engine.Operation op : ops) {
            logger.info("performing [{}], v [{}]", op.operationType().name().charAt(0), op.version());
            if (op instanceof Engine.Index) {
                Engine.IndexResult result = replicaEngine.index((Engine.Index) op);
                // replicas don't really care to about creation status of documents
                // this allows to ignore the case where a document was found in the live version maps in
                // a delete state and return false for the created flag in favor of code simplicity
                // as deleted or not. This check is just signal regression so a decision can be made if it's
                // intentional
                assertThat(result.isCreated(), equalTo(firstOp));
                assertThat(result.getVersion(), equalTo(op.version()));
                assertThat(result.hasFailure(), equalTo(false));

            } else {
                Engine.DeleteResult result = replicaEngine.delete((Engine.Delete) op);
                // Replicas don't really care to about found status of documents
                // this allows to ignore the case where a document was found in the live version maps in
                // a delete state and return true for the found flag in favor of code simplicity
                // his check is just signal regression so a decision can be made if it's
                // intentional
                assertThat(result.isFound(), equalTo(firstOp == false));
                assertThat(result.getVersion(), equalTo(op.version()));
                assertThat(result.hasFailure(), equalTo(false));
            }
            if (randomBoolean()) {
                engine.refresh("test");
            } if (randomBoolean()) {
                engine.flush();
            }
            firstOp = false;
        }

        assertVisibleCount(replicaEngine, lastFieldValue == null ? 0 : 1);
        if (lastFieldValue != null) {
            try (Searcher searcher = replicaEngine.acquireSearcher("test")) {
                final TotalHitCountCollector collector = new TotalHitCountCollector();
                searcher.searcher().search(new TermQuery(new Term("value", lastFieldValue)), collector);
                assertThat(collector.getTotalHits(), equalTo(1));
            }
        }
    }

    public void testConcurrentOutOfDocsOnReplica() throws IOException, InterruptedException {
        final List<Engine.Operation> ops = generateSingleDocHistory(true, randomFrom(VersionType.INTERNAL, VersionType.EXTERNAL), 100, 300);
        final Engine.Operation lastOp = ops.get(ops.size() - 1);
        final String lastFieldValue;
        if (lastOp instanceof Engine.Index) {
            Engine.Index index = (Engine.Index) lastOp;
            lastFieldValue = index.docs().get(0).get("value");
        } else {
            // delete
            lastFieldValue = null;
        }
        shuffle(ops, random());
        concurrentlyApplyOps(ops, engine);

        assertVisibleCount(engine, lastFieldValue == null ? 0 : 1);
        if (lastFieldValue != null) {
            try (Searcher searcher = engine.acquireSearcher("test")) {
                final TotalHitCountCollector collector = new TotalHitCountCollector();
                searcher.searcher().search(new TermQuery(new Term("value", lastFieldValue)), collector);
                assertThat(collector.getTotalHits(), equalTo(1));
            }
        }
    }

    private void concurrentlyApplyOps(List<Engine.Operation> ops, InternalEngine engine) throws InterruptedException {
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
                        final Engine.Operation op = ops.get(docOffset);
                        if (op instanceof Engine.Index) {
                            engine.index((Engine.Index)op);
                        } else {
                            engine.delete((Engine.Delete)op);
                        }
                        if ((docOffset + 1) % 4 == 0) {
                            engine.refresh("test");
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

    public void testInternalVersioningOnPrimary() throws IOException {
        final List<Engine.Operation> ops = generateSingleDocHistory(false, VersionType.INTERNAL, 2, 20);
        assertOpsOnPrimary(ops, Versions.NOT_FOUND, true, engine);
    }

    private int assertOpsOnPrimary(List<Engine.Operation> ops, long currentOpVersion, boolean docDeleted, InternalEngine engine)
        throws IOException {
        String lastFieldValue = null;
        int opsPerformed = 0;
        long lastOpVersion = currentOpVersion;
        BiFunction<Long, Engine.Index, Engine.Index> indexWithVersion = (version, index) -> new Engine.Index(index.uid(), index.parsedDoc(),
            version, index.versionType(), index.origin(), index.startTime(), index.getAutoGeneratedIdTimestamp(), index.isRetry());
        BiFunction<Long, Engine.Delete, Engine.Delete> delWithVersion = (version, delete) -> new Engine.Delete(delete.type(), delete.id(),
            delete.uid(), version, delete.versionType(), delete.origin(), delete.startTime());
        for (Engine.Operation op : ops) {
            final boolean versionConflict = rarely();
            final boolean versionedOp = versionConflict || randomBoolean();
            final long conflictingVersion = docDeleted || randomBoolean() ?
                lastOpVersion + (randomBoolean() ? 1 : -1) :
                Versions.MATCH_DELETED;
            final long correctVersion = docDeleted && randomBoolean() ? Versions.MATCH_DELETED : lastOpVersion;
            logger.info("performing [{}]{}{}",
                op.operationType().name().charAt(0),
                versionConflict ? " (conflict " + conflictingVersion +")" : "",
                versionedOp ? " (versioned " + correctVersion + ")" : "");
            if (op instanceof Engine.Index) {
                final Engine.Index index = (Engine.Index) op;
                if (versionConflict) {
                    // generate a conflict
                    Engine.IndexResult result = engine.index(indexWithVersion.apply(conflictingVersion, index));
                    assertThat(result.isCreated(), equalTo(false));
                    assertThat(result.getVersion(), equalTo(lastOpVersion));
                    assertThat(result.hasFailure(), equalTo(true));
                    assertThat(result.getFailure(), instanceOf(VersionConflictEngineException.class));
                } else {
                    Engine.IndexResult result = engine.index(versionedOp ? indexWithVersion.apply(correctVersion, index) : index);
                    assertThat(result.isCreated(), equalTo(docDeleted));
                    assertThat(result.getVersion(), equalTo(Math.max(lastOpVersion + 1, 1)));
                    assertThat(result.hasFailure(), equalTo(false));
                    assertThat(result.getFailure(), nullValue());
                    lastFieldValue = index.docs().get(0).get("value");
                    docDeleted = false;
                    lastOpVersion = result.getVersion();
                    opsPerformed++;
                }
            } else {
                final Engine.Delete delete = (Engine.Delete) op;
                if (versionConflict) {
                    // generate a conflict
                    Engine.DeleteResult result = engine.delete(delWithVersion.apply(conflictingVersion, delete));
                    assertThat(result.isFound(), equalTo(docDeleted == false));
                    assertThat(result.getVersion(), equalTo(lastOpVersion));
                    assertThat(result.hasFailure(), equalTo(true));
                    assertThat(result.getFailure(), instanceOf(VersionConflictEngineException.class));
                } else {
                    Engine.DeleteResult result = engine.delete(versionedOp ? delWithVersion.apply(correctVersion, delete) : delete);
                    assertThat(result.isFound(), equalTo(docDeleted == false));
                    assertThat(result.getVersion(), equalTo(Math.max(lastOpVersion + 1, 1)));
                    assertThat(result.hasFailure(), equalTo(false));
                    assertThat(result.getFailure(), nullValue());
                    docDeleted = true;
                    lastOpVersion = result.getVersion();
                    opsPerformed++;
                }
            }
            if (randomBoolean()) {
                // refresh and take the chance to check everything is ok so far
                assertVisibleCount(engine, docDeleted ? 0 : 1);
                // even if doc is not not deleted, lastFieldValue can still be null if this is the
                // first op and it failed.
                if (docDeleted == false && lastFieldValue != null) {
                    try (Searcher searcher = engine.acquireSearcher("test")) {
                        final TotalHitCountCollector collector = new TotalHitCountCollector();
                        searcher.searcher().search(new TermQuery(new Term("value", lastFieldValue)), collector);
                        assertThat(collector.getTotalHits(), equalTo(1));
                    }
                }
            }
            if (randomBoolean()) {
                engine.flush();
            }

            if (rarely()) {
                // simulate GC deletes
                engine.refresh("gc_simulation");
                engine.clearDeletedTombstones();
                if (docDeleted) {
                    lastOpVersion = Versions.NOT_FOUND;
                }
            }
        }

        assertVisibleCount(engine, docDeleted ? 0 : 1);
        if (docDeleted == false) {
            try (Searcher searcher = engine.acquireSearcher("test")) {
                final TotalHitCountCollector collector = new TotalHitCountCollector();
                searcher.searcher().search(new TermQuery(new Term("value", lastFieldValue)), collector);
                assertThat(collector.getTotalHits(), equalTo(1));
            }
        }
        return opsPerformed;
    }

    public void testNonInternalVersioningOnPrimary() throws IOException {
        final Set<VersionType> nonInternalVersioning = new HashSet<>(Arrays.asList(VersionType.values()));
        nonInternalVersioning.remove(VersionType.INTERNAL);
        final VersionType versionType = randomFrom(nonInternalVersioning);
        final List<Engine.Operation> ops = generateSingleDocHistory(false, versionType, 2, 20);
        final Engine.Operation lastOp = ops.get(ops.size() - 1);
        final String lastFieldValue;
        if (lastOp instanceof Engine.Index) {
            Engine.Index index = (Engine.Index) lastOp;
            lastFieldValue = index.docs().get(0).get("value");
        } else {
            // delete
            lastFieldValue = null;
        }
        // other version types don't support out of order processing.
        if (versionType == VersionType.EXTERNAL) {
            shuffle(ops, random());
        }
        long highestOpVersion = Versions.NOT_FOUND;
        long seqNo = -1;
        boolean docDeleted = true;
        for (Engine.Operation op : ops) {
            logger.info("performing [{}], v [{}]", op.operationType().name().charAt(0), op.version());
            if (op instanceof Engine.Index) {
                final Engine.Index index = (Engine.Index) op;
                Engine.IndexResult result = engine.index(index);
                if (op.versionType().isVersionConflictForWrites(highestOpVersion, op.version(), docDeleted) == false) {
                    seqNo++;
                    assertThat(result.isCreated(), equalTo(docDeleted));
                    assertThat(result.getVersion(), equalTo(op.version()));
                    assertThat(result.hasFailure(), equalTo(false));
                    assertThat(result.getFailure(), nullValue());
                    docDeleted = false;
                    highestOpVersion = op.version();
                } else {
                    assertThat(result.isCreated(), equalTo(false));
                    assertThat(result.getVersion(), equalTo(highestOpVersion));
                    assertThat(result.hasFailure(), equalTo(true));
                    assertThat(result.getFailure(), instanceOf(VersionConflictEngineException.class));
                }
            } else {
                final Engine.Delete delete = (Engine.Delete) op;
                Engine.DeleteResult result = engine.delete(delete);
                if (op.versionType().isVersionConflictForWrites(highestOpVersion, op.version(), docDeleted) == false) {
                    seqNo++;
                    assertThat(result.isFound(), equalTo(docDeleted == false));
                    assertThat(result.getVersion(), equalTo(op.version()));
                    assertThat(result.hasFailure(), equalTo(false));
                    assertThat(result.getFailure(), nullValue());
                    docDeleted = true;
                    highestOpVersion = op.version();
                } else {
                    assertThat(result.isFound(), equalTo(docDeleted == false));
                    assertThat(result.getVersion(), equalTo(highestOpVersion));
                    assertThat(result.hasFailure(), equalTo(true));
                    assertThat(result.getFailure(), instanceOf(VersionConflictEngineException.class));
                }
            }
            if (randomBoolean()) {
                engine.refresh("test");
            }
            if (randomBoolean()) {
                engine.flush();
            }
        }

        assertVisibleCount(engine, docDeleted ? 0 : 1);
        if (docDeleted == false) {
            logger.info("searching for [{}]", lastFieldValue);
            try (Searcher searcher = engine.acquireSearcher("test")) {
                final TotalHitCountCollector collector = new TotalHitCountCollector();
                searcher.searcher().search(new TermQuery(new Term("value", lastFieldValue)), collector);
                assertThat(collector.getTotalHits(), equalTo(1));
            }
        }
    }

    public void testVersioningPromotedReplica() throws IOException {
        final List<Engine.Operation> replicaOps = generateSingleDocHistory(true, VersionType.INTERNAL, 2, 20);
        List<Engine.Operation> primaryOps = generateSingleDocHistory(false, VersionType.INTERNAL, 2, 20);
        Engine.Operation lastReplicaOp = replicaOps.get(replicaOps.size() - 1);
        final boolean deletedOnReplica = lastReplicaOp instanceof Engine.Delete;
        final long finalReplicaVersion = lastReplicaOp.version();
        assertOpsOnReplica(replicaOps, replicaEngine, true);
        assertOpsOnPrimary(primaryOps, finalReplicaVersion, deletedOnReplica, replicaEngine);
    }

    public void testConcurrentExternalVersioningOnPrimary() throws IOException, InterruptedException {
        final List<Engine.Operation> ops = generateSingleDocHistory(false, VersionType.EXTERNAL, 100, 300);
        final Engine.Operation lastOp = ops.get(ops.size() - 1);
        final String lastFieldValue;
        if (lastOp instanceof Engine.Index) {
            Engine.Index index = (Engine.Index) lastOp;
            lastFieldValue = index.docs().get(0).get("value");
        } else {
            // delete
            lastFieldValue = null;
        }
        shuffle(ops, random());
        concurrentlyApplyOps(ops, engine);

        assertVisibleCount(engine, lastFieldValue == null ? 0 : 1);
        if (lastFieldValue != null) {
            try (Searcher searcher = engine.acquireSearcher("test")) {
                final TotalHitCountCollector collector = new TotalHitCountCollector();
                searcher.searcher().search(new TermQuery(new Term("value", lastFieldValue)), collector);
                assertThat(collector.getTotalHits(), equalTo(1));
            }
        }
    }

    public void testConcurrentGetAndSetOnPrimary() throws IOException, InterruptedException {
        Thread[] thread = new Thread[randomIntBetween(3, 5)];
        CountDownLatch startGun = new CountDownLatch(thread.length);
        final int opsPerThread = randomIntBetween(10, 20);
        class OpAndVersion {
            final long version;
            final String removed;
            final String added;

            OpAndVersion(long version, String removed, String added) {
                this.version = version;
                this.removed = removed;
                this.added = added;
            }
        }
        final AtomicInteger idGenerator = new AtomicInteger();
        final Queue<OpAndVersion> history = ConcurrentCollections.newQueue();
        ParsedDocument doc = testParsedDocument("1", "test", null, System.currentTimeMillis(), -1L, testDocument(), bytesArray(""), null);
        final Term uidTerm = newUid(doc);
        engine.index(indexForDoc(doc));
        final Function<String, Searcher> searcherFactory = engine::acquireSearcher;
        for (int i = 0; i < thread.length; i++) {
            thread[i] = new Thread(() -> {
                startGun.countDown();
                try {
                    startGun.await();
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
                for (int op = 0; op < opsPerThread; op++) {
                    try (Engine.GetResult get = engine.get(new Engine.Get(true, doc.type(), doc.id(), uidTerm), searcherFactory)) {
                        FieldsVisitor visitor = new FieldsVisitor(true);
                        get.docIdAndVersion().context.reader().document(get.docIdAndVersion().docId, visitor);
                        List<String> values = new ArrayList<>(Strings.commaDelimitedListToSet(visitor.source().utf8ToString()));
                        String removed = op % 3 == 0 && values.size() > 0 ? values.remove(0) : null;
                        String added = "v_" + idGenerator.incrementAndGet();
                        values.add(added);
                        Engine.Index index = new Engine.Index(uidTerm,
                            testParsedDocument("1", "test", null, System.currentTimeMillis(), -1L, testDocument(),
                                bytesArray(Strings.collectionToCommaDelimitedString(values)), null),
                            get.version(), VersionType.INTERNAL,
                            PRIMARY, System.currentTimeMillis(), -1, false);
                        Engine.IndexResult indexResult = engine.index(index);
                        if (indexResult.hasFailure() == false) {
                            history.add(new OpAndVersion(indexResult.getVersion(), removed, added));
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
        List<OpAndVersion> sortedHistory = new ArrayList<>(history);
        sortedHistory.sort(Comparator.comparing(o -> o.version));
        Set<String> currentValues = new HashSet<>();
        for (int i = 0; i < sortedHistory.size(); i++) {
            OpAndVersion op = sortedHistory.get(i);
            if (i > 0) {
                assertThat("duplicate version", op.version, not(equalTo(sortedHistory.get(i - 1).version)));
            }
            boolean exists = op.removed == null ? true : currentValues.remove(op.removed);
            assertTrue(op.removed + " should exist", exists);
            exists = currentValues.add(op.added);
            assertTrue(op.added + " should not exist", exists);
        }

        try (Engine.GetResult get = engine.get(new Engine.Get(true, doc.type(), doc.id(), uidTerm), searcherFactory)) {
            FieldsVisitor visitor = new FieldsVisitor(true);
            get.docIdAndVersion().context.reader().document(get.docIdAndVersion().docId, visitor);
            List<String> values = Arrays.asList(Strings.commaDelimitedListToStringArray(visitor.source().utf8ToString()));
            assertThat(currentValues, equalTo(new HashSet<>(values)));
        }
    }

    public void testBasicCreatedFlag() throws IOException {
        ParsedDocument doc = testParsedDocument("1", "test", null, -1, -1, testDocument(), B_1, null);
        Engine.Index index = indexForDoc(doc);
        Engine.IndexResult indexResult = engine.index(index);
        assertTrue(indexResult.isCreated());

        index = indexForDoc(doc);
        indexResult = engine.index(index);
        assertFalse(indexResult.isCreated());

        engine.delete(new Engine.Delete("doc", "1", newUid(doc)));

        index = indexForDoc(doc);
        indexResult = engine.index(index);
        assertTrue(indexResult.isCreated());
    }

    public void testCreatedFlagAfterFlush() throws IOException {
        ParsedDocument doc = testParsedDocument("1", "doc", null, -1, -1, testDocument(), B_1, null);
        Engine.Index index = indexForDoc(doc);
        Engine.IndexResult indexResult = engine.index(index);
        assertTrue(indexResult.isCreated());

        engine.delete(new Engine.Delete("doc", "1", newUid(doc)));

        engine.flush();

        index = indexForDoc(doc);
        indexResult = engine.index(index);
        assertTrue(indexResult.isCreated());
    }

    private static class MockAppender extends AbstractAppender {
        public boolean sawIndexWriterMessage;

        public boolean sawIndexWriterIFDMessage;

        MockAppender(final String name) throws IllegalAccessException {
            super(name, RegexFilter.createFilter(".*(\n.*)*", new String[0], false, null, null), null);
        }

        @Override
        public void append(LogEvent event) {
            final String formattedMessage = event.getMessage().getFormattedMessage();
            if (event.getLevel() == Level.TRACE && event.getMarker().getName().contains("[index][1] ")) {
                if (event.getLoggerName().endsWith(".IW") &&
                    formattedMessage.contains("IW: apply all deletes during flush")) {
                    sawIndexWriterMessage = true;
                }
                if (event.getLoggerName().endsWith(".IFD")) {
                    sawIndexWriterIFDMessage = true;
                }
            }
        }
    }

    // #5891: make sure IndexWriter's infoStream output is
    // sent to lucene.iw with log level TRACE:

    public void testIndexWriterInfoStream() throws IllegalAccessException, IOException {
        assumeFalse("who tests the tester?", VERBOSE);
        MockAppender mockAppender = new MockAppender("testIndexWriterInfoStream");
        mockAppender.start();

        Logger rootLogger = LogManager.getRootLogger();
        Level savedLevel = rootLogger.getLevel();
        Loggers.addAppender(rootLogger, mockAppender);
        Loggers.setLevel(rootLogger, Level.DEBUG);
        rootLogger = LogManager.getRootLogger();

        try {
            // First, with DEBUG, which should NOT log IndexWriter output:
            ParsedDocument doc = testParsedDocument("1", "test", null, -1, -1, testDocumentWithTextField(), B_1, null);
            engine.index(indexForDoc(doc));
            engine.flush();
            assertFalse(mockAppender.sawIndexWriterMessage);

            // Again, with TRACE, which should log IndexWriter output:
            Loggers.setLevel(rootLogger, Level.TRACE);
            engine.index(indexForDoc(doc));
            engine.flush();
            assertTrue(mockAppender.sawIndexWriterMessage);

        } finally {
            Loggers.removeAppender(rootLogger, mockAppender);
            mockAppender.stop();
            Loggers.setLevel(rootLogger, savedLevel);
        }
    }

    // #8603: make sure we can separately log IFD's messages
    public void testIndexWriterIFDInfoStream() throws IllegalAccessException, IOException {
        assumeFalse("who tests the tester?", VERBOSE);
        MockAppender mockAppender = new MockAppender("testIndexWriterIFDInfoStream");
        mockAppender.start();

        final Logger iwIFDLogger = Loggers.getLogger("org.elasticsearch.index.engine.Engine.IFD");

        Loggers.addAppender(iwIFDLogger, mockAppender);
        Loggers.setLevel(iwIFDLogger, Level.DEBUG);

        try {
            // First, with DEBUG, which should NOT log IndexWriter output:
            ParsedDocument doc = testParsedDocument("1", "test", null, -1, -1, testDocumentWithTextField(), B_1, null);
            engine.index(indexForDoc(doc));
            engine.flush();
            assertFalse(mockAppender.sawIndexWriterMessage);
            assertFalse(mockAppender.sawIndexWriterIFDMessage);

            // Again, with TRACE, which should only log IndexWriter IFD output:
            Loggers.setLevel(iwIFDLogger, Level.TRACE);
            engine.index(indexForDoc(doc));
            engine.flush();
            assertFalse(mockAppender.sawIndexWriterMessage);
            assertTrue(mockAppender.sawIndexWriterIFDMessage);

        } finally {
            Loggers.removeAppender(iwIFDLogger, mockAppender);
            mockAppender.stop();
            Loggers.setLevel(iwIFDLogger, (Level) null);
        }
    }

    public void testEnableGcDeletes() throws Exception {
        try (Store store = createStore();
            Engine engine = new InternalEngine(config(defaultSettings, store, createTempDir(), newMergePolicy(), IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, null))) {
            engine.config().setEnableGcDeletes(false);

            final Function<String, Searcher> searcherFactory = engine::acquireSearcher;

            // Add document
            Document document = testDocument();
            document.add(new TextField("value", "test1", Field.Store.YES));

            ParsedDocument doc = testParsedDocument("1", "test", null, -1, -1, document, B_2, null);
            engine.index(new Engine.Index(newUid(doc), doc, 1, VersionType.EXTERNAL, Engine.Operation.Origin.PRIMARY, System.nanoTime(), -1, false));

            // Delete document we just added:
            engine.delete(new Engine.Delete("test", "1", newUid(doc), 10, VersionType.EXTERNAL, Engine.Operation.Origin.PRIMARY, System.nanoTime()));

            // Get should not find the document
            Engine.GetResult getResult = engine.get(new Engine.Get(true, doc.type(), doc.id(), newUid(doc)), searcherFactory);
            assertThat(getResult.exists(), equalTo(false));

            // Give the gc pruning logic a chance to kick in
            Thread.sleep(1000);

            if (randomBoolean()) {
                engine.refresh("test");
            }

            // Delete non-existent document
            ParsedDocument doc2 = testParsedDocument("2", "test", null, -1, -1, document, B_2, null);
            engine.delete(new Engine.Delete("test", "2", newUid(doc2), 10, VersionType.EXTERNAL, Engine.Operation.Origin.PRIMARY, System.nanoTime()));

            // Get should not find the document (we never indexed uid=2):
            getResult = engine.get(new Engine.Get(true, doc.type(), doc.id(), newUid(doc2)), searcherFactory);
            assertThat(getResult.exists(), equalTo(false));

            // Try to index uid=1 with a too-old version, should fail:
            Engine.IndexResult indexResult = engine.index(new Engine.Index(newUid(doc), doc, 2, VersionType.EXTERNAL, Engine.Operation.Origin.PRIMARY, System.nanoTime(), -1, false));
            assertTrue(indexResult.hasFailure());
            assertThat(indexResult.getFailure(), instanceOf(VersionConflictEngineException.class));

            // Get should still not find the document
            getResult = engine.get(new Engine.Get(true, doc.type(), doc.id(), newUid(doc)), searcherFactory);
            assertThat(getResult.exists(), equalTo(false));

            // Try to index uid=2 with a too-old version, should fail:
            indexResult = engine.index(new Engine.Index(newUid(doc2), doc2, 2, VersionType.EXTERNAL, Engine.Operation.Origin.PRIMARY, System.nanoTime(), -1, false));
            assertTrue(indexResult.hasFailure());
            assertThat(indexResult.getFailure(), instanceOf(VersionConflictEngineException.class));

            // Get should not find the document
            getResult = engine.get(new Engine.Get(true, doc.type(), doc.id(), newUid(doc)), searcherFactory);
            assertThat(getResult.exists(), equalTo(false));
        }
    }

    public void testExtractShardId() {
        try (Engine.Searcher test = this.engine.acquireSearcher("test")) {
            ShardId shardId = ShardUtils.extractShardId(test.getDirectoryReader());
            assertNotNull(shardId);
            assertEquals(shardId, engine.config().getShardId());
        }
    }

    /**
     * Random test that throws random exception and ensures all references are
     * counted down / released and resources are closed.
     */
    public void testFailStart() throws IOException {
        // this test fails if any reader, searcher or directory is not closed - MDW FTW
        final int iters = scaledRandomIntBetween(10, 100);
        for (int i = 0; i < iters; i++) {
            MockDirectoryWrapper wrapper = newMockDirectory();
            wrapper.setFailOnOpenInput(randomBoolean());
            wrapper.setAllowRandomFileNotFoundException(randomBoolean());
            wrapper.setRandomIOExceptionRate(randomDouble());
            wrapper.setRandomIOExceptionRateOnOpen(randomDouble());
            final Path translogPath = createTempDir("testFailStart");
            try (Store store = createStore(wrapper)) {
                int refCount = store.refCount();
                assertTrue("refCount: " + store.refCount(), store.refCount() > 0);
                InternalEngine holder;
                try {
                    holder = createEngine(store, translogPath);
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
                        holder = createEngine(store, translogPath);
                        assertEquals(store.refCount(), refCount + 1);
                    } catch (EngineCreationFailureException ex) {
                        // all is fine
                        assertEquals(store.refCount(), refCount);
                        break;
                    }
                }
                holder.close();
                assertEquals(store.refCount(), refCount);
            }
        }
    }

    public void testSettings() {
        CodecService codecService = new CodecService(null, logger);
        LiveIndexWriterConfig currentIndexWriterConfig = engine.getCurrentIndexWriterConfig();

        assertEquals(engine.config().getCodec().getName(), codecService.codec(codecName).getName());
        assertEquals(currentIndexWriterConfig.getCodec().getName(), codecService.codec(codecName).getName());
    }

    public void testMissingTranslog() throws IOException {
        // test that we can force start the engine , even if the translog is missing.
        engine.close();
        // fake a new translog, causing the engine to point to a missing one.
        Translog translog = createTranslog();
        long id = translog.currentFileGeneration();
        translog.close();
        IOUtils.rm(translog.location().resolve(Translog.getFilename(id)));
        try {
            engine = createEngine(store, primaryTranslogDir);
            fail("engine shouldn't start without a valid translog id");
        } catch (EngineCreationFailureException ex) {
            // expected
        }
        // now it should be OK.
        EngineConfig config = copy(config(defaultSettings, store, primaryTranslogDir, newMergePolicy(), IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, null), EngineConfig.OpenMode.OPEN_INDEX_CREATE_TRANSLOG);
        engine = new InternalEngine(config);
    }

    public void testTranslogReplayWithFailure() throws IOException {
        final int numDocs = randomIntBetween(1, 10);
        for (int i = 0; i < numDocs; i++) {
            ParsedDocument doc = testParsedDocument(Integer.toString(i), "test", null, -1, -1, testDocument(), SOURCE, null);
            Engine.Index firstIndexRequest = new Engine.Index(newUid(doc), doc, Versions.MATCH_DELETED, VersionType.INTERNAL, PRIMARY, System.nanoTime(), -1, false);
            Engine.IndexResult indexResult = engine.index(firstIndexRequest);
            assertThat(indexResult.getVersion(), equalTo(1L));
        }
        assertVisibleCount(engine, numDocs);
        engine.close();
        final MockDirectoryWrapper directory = DirectoryUtils.getLeaf(store.directory(), MockDirectoryWrapper.class);
        if (directory != null) {
            // since we rollback the IW we are writing the same segment files again after starting IW but MDW prevents
            // this so we have to disable the check explicitly
            boolean started = false;
            final int numIters = randomIntBetween(10, 20);
            for (int i = 0; i < numIters; i++) {
                directory.setRandomIOExceptionRateOnOpen(randomDouble());
                directory.setRandomIOExceptionRate(randomDouble());
                directory.setFailOnOpenInput(randomBoolean());
                directory.setAllowRandomFileNotFoundException(randomBoolean());
                try {
                    engine = createEngine(store, primaryTranslogDir);
                    started = true;
                    break;
                } catch (EngineException | IOException e) {
                }
            }

            directory.setRandomIOExceptionRateOnOpen(0.0);
            directory.setRandomIOExceptionRate(0.0);
            directory.setFailOnOpenInput(false);
            directory.setAllowRandomFileNotFoundException(false);
            if (started == false) {
                engine = createEngine(store, primaryTranslogDir);
            }
        } else {
            // no mock directory, no fun.
            engine = createEngine(store, primaryTranslogDir);
        }
        assertVisibleCount(engine, numDocs, false);
    }

    private static void assertVisibleCount(InternalEngine engine, int numDocs) throws IOException {
        assertVisibleCount(engine, numDocs, true);
    }

    private static void assertVisibleCount(InternalEngine engine, int numDocs, boolean refresh) throws IOException {
        if (refresh) {
            engine.refresh("test");
        }
        try (Searcher searcher = engine.acquireSearcher("test")) {
            final TotalHitCountCollector collector = new TotalHitCountCollector();
            searcher.searcher().search(new MatchAllDocsQuery(), collector);
            assertThat(collector.getTotalHits(), equalTo(numDocs));
        }
    }

    public void testSkipTranslogReplay() throws IOException {
        final int numDocs = randomIntBetween(1, 10);
        for (int i = 0; i < numDocs; i++) {
            ParsedDocument doc = testParsedDocument(Integer.toString(i), "test", null, -1, -1, testDocument(), SOURCE, null);
            Engine.Index firstIndexRequest = new Engine.Index(newUid(doc), doc, Versions.MATCH_DELETED, VersionType.INTERNAL, PRIMARY, System.nanoTime(), -1, false);
            Engine.IndexResult firstIndexResult = engine.index(firstIndexRequest);
            assertThat(firstIndexResult.getVersion(), equalTo(1L));
        }
        assertVisibleCount(engine, numDocs);
        engine.close();
        engine = new InternalEngine(engine.config());

        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), randomIntBetween(numDocs, numDocs + 10));
            assertThat(topDocs.totalHits, equalTo(0));
        }

    }

    private Mapping dynamicUpdate() {
        BuilderContext context = new BuilderContext(
            Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build(), new ContentPath());
        final RootObjectMapper root = new RootObjectMapper.Builder("some_type").build(context);
        return new Mapping(Version.CURRENT, root, new MetadataFieldMapper[0], emptyMap());
    }

    public void testUpgradeOldIndex() throws IOException {
        List<Path> indexes = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(getBwcIndicesPath(), "index-*.zip")) {
            for (Path path : stream) {
                indexes.add(path);
            }
        }
        Collections.shuffle(indexes, random());
        for (Path indexFile : indexes.subList(0, scaledRandomIntBetween(1, indexes.size() / 2))) {
            final String indexName = indexFile.getFileName().toString().replace(".zip", "").toLowerCase(Locale.ROOT);
            Path unzipDir = createTempDir();
            Path unzipDataDir = unzipDir.resolve("data");
            // decompress the index
            try (InputStream stream = Files.newInputStream(indexFile)) {
                TestUtil.unzip(stream, unzipDir);
            }
            // check it is unique
            assertTrue(Files.exists(unzipDataDir));
            Path[] list = filterExtraFSFiles(FileSystemUtils.files(unzipDataDir));

            if (list.length != 1) {
                throw new IllegalStateException("Backwards index must contain exactly one cluster but was " + list.length
                    + " " + Arrays.toString(list));
            }

            // the bwc scripts packs the indices under this path
            Path src = OldIndexUtils.getIndexDir(logger, indexName, indexFile.toString(), list[0]);
            Path translog = src.resolve("0").resolve("translog");
            assertTrue("[" + indexFile + "] missing translog dir: " + translog.toString(), Files.exists(translog));
            Path[] tlogFiles = filterExtraFSFiles(FileSystemUtils.files(translog));
            assertEquals(Arrays.toString(tlogFiles), tlogFiles.length, 2); // ckp & tlog
            Path tlogFile = tlogFiles[0].getFileName().toString().endsWith("tlog") ? tlogFiles[0] : tlogFiles[1];
            final long size = Files.size(tlogFile);
            logger.debug("upgrading index {} file: {} size: {}", indexName, tlogFiles[0].getFileName(), size);
            Directory directory = newFSDirectory(src.resolve("0").resolve("index"));
            Store store = createStore(directory);
            final int iters = randomIntBetween(0, 2);
            int numDocs = -1;
            for (int i = 0; i < iters; i++) { // make sure we can restart on an upgraded index
                try (InternalEngine engine = createEngine(store, translog)) {
                    try (Searcher searcher = engine.acquireSearcher("test")) {
                        if (i > 0) {
                            assertEquals(numDocs, searcher.reader().numDocs());
                        }
                        TopDocs search = searcher.searcher().search(new MatchAllDocsQuery(), 1);
                        numDocs = searcher.reader().numDocs();
                        assertTrue(search.totalHits > 1);
                    }
                    CommitStats commitStats = engine.commitStats();
                    Map<String, String> userData = commitStats.getUserData();
                    assertTrue("userdata dosn't contain uuid", userData.containsKey(Translog.TRANSLOG_UUID_KEY));
                    assertTrue("userdata doesn't contain generation key", userData.containsKey(Translog.TRANSLOG_GENERATION_KEY));
                    assertFalse("userdata contains legacy marker", userData.containsKey("translog_id"));
                }
            }

            try (InternalEngine engine = createEngine(store, translog)) {
                if (numDocs == -1) {
                    try (Searcher searcher = engine.acquireSearcher("test")) {
                        numDocs = searcher.reader().numDocs();
                    }
                }
                final int numExtraDocs = randomIntBetween(1, 10);
                for (int i = 0; i < numExtraDocs; i++) {
                    ParsedDocument doc = testParsedDocument("extra" + Integer.toString(i), "test", null, -1, -1, testDocument(), new BytesArray("{}"), null);
                    Engine.Index firstIndexRequest = new Engine.Index(newUid(doc), doc, Versions.MATCH_DELETED, VersionType.INTERNAL, PRIMARY, System.nanoTime(), -1, false);
                    Engine.IndexResult firstIndexResult = engine.index(firstIndexRequest);
                    assertThat(firstIndexResult.getVersion(), equalTo(1L));
                }
                engine.refresh("test");
                try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
                    TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), randomIntBetween(numDocs, numDocs + numExtraDocs));
                    assertThat(topDocs.totalHits, equalTo(numDocs + numExtraDocs));
                }
            }
            IOUtils.close(store, directory);
        }
    }

    private Path[] filterExtraFSFiles(Path[] files) {
        List<Path> paths = new ArrayList<>();
        for (Path p : files) {
            if (p.getFileName().toString().startsWith("extra")) {
                continue;
            }
            paths.add(p);
        }
        return paths.toArray(new Path[0]);
    }

    public void testTranslogReplay() throws IOException {
        final int numDocs = randomIntBetween(1, 10);
        for (int i = 0; i < numDocs; i++) {
            ParsedDocument doc = testParsedDocument(Integer.toString(i), "test", null, -1, -1, testDocument(), SOURCE, null);
            Engine.Index firstIndexRequest = new Engine.Index(newUid(doc), doc, Versions.MATCH_DELETED, VersionType.INTERNAL, PRIMARY, System.nanoTime(), -1, false);
            Engine.IndexResult firstIndexResult = engine.index(firstIndexRequest);
            assertThat(firstIndexResult.getVersion(), equalTo(1L));
        }
        assertVisibleCount(engine, numDocs);

        TranslogHandler parser = (TranslogHandler) engine.config().getTranslogRecoveryPerformer();
        parser.mappingUpdate = dynamicUpdate();

        engine.close();
        engine = new InternalEngine(copy(engine.config(), EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG)); // we need to reuse the engine config unless the parser.mappingModified won't work
        engine.recoverFromTranslog();

        assertVisibleCount(engine, numDocs, false);
        parser = (TranslogHandler) engine.config().getTranslogRecoveryPerformer();
        assertEquals(numDocs, parser.recoveredOps.get());
        if (parser.mappingUpdate != null) {
            assertEquals(1, parser.getRecoveredTypes().size());
            assertTrue(parser.getRecoveredTypes().containsKey("test"));
        } else {
            assertEquals(0, parser.getRecoveredTypes().size());
        }

        engine.close();
        engine = createEngine(store, primaryTranslogDir);
        assertVisibleCount(engine, numDocs, false);
        parser = (TranslogHandler) engine.config().getTranslogRecoveryPerformer();
        assertEquals(0, parser.recoveredOps.get());

        final boolean flush = randomBoolean();
        int randomId = randomIntBetween(numDocs + 1, numDocs + 10);
        ParsedDocument doc = testParsedDocument(Integer.toString(randomId), "test", null, -1, -1, testDocument(), new BytesArray("{}"), null);
        Engine.Index firstIndexRequest = new Engine.Index(newUid(doc), doc, 1, VersionType.EXTERNAL, PRIMARY, System.nanoTime(), -1, false);
        Engine.IndexResult firstIndexResult = engine.index(firstIndexRequest);
        assertThat(firstIndexResult.getVersion(), equalTo(1L));
        if (flush) {
            engine.flush();
        }

        doc = testParsedDocument(Integer.toString(randomId), "test", null, -1, -1, testDocument(), new BytesArray("{}"), null);
        Engine.Index idxRequest = new Engine.Index(newUid(doc), doc, 2, VersionType.EXTERNAL, PRIMARY, System.nanoTime(), -1, false);
        Engine.IndexResult indexResult = engine.index(idxRequest);
        engine.refresh("test");
        assertThat(indexResult.getVersion(), equalTo(2L));
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), numDocs + 1);
            assertThat(topDocs.totalHits, equalTo(numDocs + 1));
        }

        engine.close();
        engine = createEngine(store, primaryTranslogDir);
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), numDocs + 1);
            assertThat(topDocs.totalHits, equalTo(numDocs + 1));
        }
        parser = (TranslogHandler) engine.config().getTranslogRecoveryPerformer();
        assertEquals(flush ? 1 : 2, parser.recoveredOps.get());
        engine.delete(new Engine.Delete("test", Integer.toString(randomId), newUid(doc)));
        if (randomBoolean()) {
            engine.refresh("test");
        } else {
            engine.close();
            engine = createEngine(store, primaryTranslogDir);
        }
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), numDocs);
            assertThat(topDocs.totalHits, equalTo(numDocs));
        }
    }

    public void testRecoverFromForeignTranslog() throws IOException {
        final int numDocs = randomIntBetween(1, 10);
        for (int i = 0; i < numDocs; i++) {
            ParsedDocument doc = testParsedDocument(Integer.toString(i), "test", null, -1, -1, testDocument(), new BytesArray("{}"), null);
            Engine.Index firstIndexRequest = new Engine.Index(newUid(doc), doc, Versions.MATCH_DELETED, VersionType.INTERNAL, PRIMARY, System.nanoTime(), -1, false);
            Engine.IndexResult indexResult = engine.index(firstIndexRequest);
            assertThat(indexResult.getVersion(), equalTo(1L));
        }
        assertVisibleCount(engine, numDocs);
        Translog.TranslogGeneration generation = engine.getTranslog().getGeneration();
        engine.close();

        Translog translog = new Translog(new TranslogConfig(shardId, createTempDir(), INDEX_SETTINGS, BigArrays.NON_RECYCLING_INSTANCE)
            , null);
        translog.add(new Translog.Index("test", "SomeBogusId", "{}".getBytes(Charset.forName("UTF-8"))));
        assertEquals(generation.translogFileGeneration, translog.currentFileGeneration());
        translog.close();

        EngineConfig config = engine.config();
        /* create a TranslogConfig that has been created with a different UUID */
        TranslogConfig translogConfig = new TranslogConfig(shardId, translog.location(), config.getIndexSettings(), BigArrays.NON_RECYCLING_INSTANCE);

        EngineConfig brokenConfig = new EngineConfig(EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG, shardId, threadPool,
                config.getIndexSettings(), null, store, createSnapshotDeletionPolicy(), newMergePolicy(), config.getAnalyzer(),
                config.getSimilarity(), new CodecService(null, logger), config.getEventListener(), config.getTranslogRecoveryPerformer(),
                IndexSearcher.getDefaultQueryCache(), IndexSearcher.getDefaultQueryCachingPolicy(), translogConfig,
                TimeValue.timeValueMinutes(5), config.getRefreshListeners(), IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP);

        try {
            InternalEngine internalEngine = new InternalEngine(brokenConfig);
            fail("translog belongs to a different engine");
        } catch (EngineCreationFailureException ex) {
        }

        engine = createEngine(store, primaryTranslogDir); // and recover again!
        assertVisibleCount(engine, numDocs, false);
    }

    public void testShardNotAvailableExceptionWhenEngineClosedConcurrently() throws IOException, InterruptedException {
        AtomicReference<Exception> exception = new AtomicReference<>();
        String operation = randomFrom("optimize", "refresh", "flush");
        Thread mergeThread = new Thread() {
            @Override
            public void run() {
                boolean stop = false;
                logger.info("try with {}", operation);
                while (stop == false) {
                    try {
                        switch (operation) {
                            case "optimize": {
                                engine.forceMerge(true, 1, false, false, false);
                                break;
                            }
                            case "refresh": {
                                engine.refresh("test refresh");
                                break;
                            }
                            case "flush": {
                                engine.flush(true, false);
                                break;
                            }
                        }
                    } catch (Exception e) {
                        exception.set(e);
                        stop = true;
                    }
                }
            }
        };
        mergeThread.start();
        engine.close();
        mergeThread.join();
        logger.info("exception caught: ", exception.get());
        assertTrue("expected an Exception that signals shard is not available", TransportActions.isShardNotAvailableException(exception.get()));
    }

    /**
     * Tests that when the the close method returns the engine is actually guaranteed to have cleaned up and that resources are closed
     */
    public void testConcurrentEngineClosed() throws BrokenBarrierException, InterruptedException {
        Thread[] closingThreads = new Thread[3];
        CyclicBarrier barrier = new CyclicBarrier(1 + closingThreads.length + 1);
        Thread failEngine = new Thread(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }

            @Override
            protected void doRun() throws Exception {
                barrier.await();
                engine.failEngine("test", new RuntimeException("test"));
            }
        });
        failEngine.start();
        for (int i = 0;i < closingThreads.length ; i++) {
            boolean flushAndClose = randomBoolean();
            closingThreads[i] = new Thread(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    throw new AssertionError(e);
                }

                @Override
                protected void doRun() throws Exception {
                    barrier.await();
                    if (flushAndClose) {
                        engine.flushAndClose();
                    } else {
                        engine.close();
                    }
                    // try to acquire the writer lock - i.e., everything is closed, we need to synchronize
                    // to avoid races between closing threads
                    synchronized (closingThreads) {
                        try (Lock ignored = store.directory().obtainLock(IndexWriter.WRITE_LOCK_NAME)) {
                            // all good.
                        }
                    }
                }
            });
            closingThreads[i].setName("closingThread_" + i);
            closingThreads[i].start();
        }
        barrier.await();
        failEngine.join();
        for (Thread t : closingThreads) {
            t.join();
        }
    }

    public void testCurrentTranslogIDisCommitted() throws IOException {
        try (Store store = createStore()) {
            EngineConfig config = config(defaultSettings, store, createTempDir(), newMergePolicy(), IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, null);

            // create
            {
                ParsedDocument doc = testParsedDocument(Integer.toString(0), "test", null, -1, -1, testDocument(), new BytesArray("{}"), null);
                Engine.Index firstIndexRequest = new Engine.Index(newUid(doc), doc, Versions.MATCH_DELETED, VersionType.INTERNAL, PRIMARY, System.nanoTime(), -1, false);

                try (InternalEngine engine = new InternalEngine(copy(config, EngineConfig.OpenMode.CREATE_INDEX_AND_TRANSLOG))){
                    assertFalse(engine.isRecovering());
                    engine.index(firstIndexRequest);

                    expectThrows(IllegalStateException.class, () -> engine.recoverFromTranslog());
                    Map<String, String> userData = engine.getLastCommittedSegmentInfos().getUserData();
                    assertEquals("1", userData.get(Translog.TRANSLOG_GENERATION_KEY));
                    assertEquals(engine.getTranslog().getTranslogUUID(), userData.get(Translog.TRANSLOG_UUID_KEY));
                }
            }
            // open and recover tlog
            {
                for (int i = 0; i < 2; i++) {
                    try (InternalEngine engine = new InternalEngine(copy(config, EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG))) {
                        assertTrue(engine.isRecovering());
                        Map<String, String> userData = engine.getLastCommittedSegmentInfos().getUserData();
                        if (i == 0) {
                            assertEquals("1", userData.get(Translog.TRANSLOG_GENERATION_KEY));
                        } else {
                            assertEquals("3", userData.get(Translog.TRANSLOG_GENERATION_KEY));
                        }
                        assertEquals(engine.getTranslog().getTranslogUUID(), userData.get(Translog.TRANSLOG_UUID_KEY));
                        engine.recoverFromTranslog();
                        userData = engine.getLastCommittedSegmentInfos().getUserData();
                        assertEquals("3", userData.get(Translog.TRANSLOG_GENERATION_KEY));
                        assertEquals(engine.getTranslog().getTranslogUUID(), userData.get(Translog.TRANSLOG_UUID_KEY));
                    }
                }
            }
            // open index with new tlog
            {
                try (InternalEngine engine = new InternalEngine(copy(config, EngineConfig.OpenMode.OPEN_INDEX_CREATE_TRANSLOG))) {
                    Map<String, String> userData = engine.getLastCommittedSegmentInfos().getUserData();
                    assertEquals("1", userData.get(Translog.TRANSLOG_GENERATION_KEY));
                    assertEquals(engine.getTranslog().getTranslogUUID(), userData.get(Translog.TRANSLOG_UUID_KEY));
                    expectThrows(IllegalStateException.class, () -> engine.recoverFromTranslog());
                }
            }

            // open and recover tlog with empty tlog
            {
                for (int i = 0; i < 2; i++) {
                    try (InternalEngine engine = new InternalEngine(copy(config, EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG))) {
                        Map<String, String> userData = engine.getLastCommittedSegmentInfos().getUserData();
                        assertEquals("1", userData.get(Translog.TRANSLOG_GENERATION_KEY));
                        assertEquals(engine.getTranslog().getTranslogUUID(), userData.get(Translog.TRANSLOG_UUID_KEY));
                        engine.recoverFromTranslog();
                        userData = engine.getLastCommittedSegmentInfos().getUserData();
                        assertEquals("no changes - nothing to commit", "1", userData.get(Translog.TRANSLOG_GENERATION_KEY));
                        assertEquals(engine.getTranslog().getTranslogUUID(), userData.get(Translog.TRANSLOG_UUID_KEY));
                    }
                }
            }
        }
    }

    private static class ThrowingIndexWriter extends IndexWriter {
        private AtomicReference<Supplier<Exception>> failureToThrow = new AtomicReference<>();

        ThrowingIndexWriter(Directory d, IndexWriterConfig conf) throws IOException {
            super(d, conf);
        }

        @Override
        public long addDocument(Iterable<? extends IndexableField> doc) throws IOException {
            maybeThrowFailure();
            return super.addDocument(doc);
        }

        private void maybeThrowFailure() throws IOException {
            if (failureToThrow.get() != null) {
                Exception failure = failureToThrow.get().get();
                if (failure instanceof RuntimeException) {
                    throw (RuntimeException) failure;
                } else if (failure instanceof IOException) {
                    throw (IOException) failure;
                } else {
                    assert false: "unsupported failure class: " + failure.getClass().getCanonicalName();
                }
            }
        }

        @Override
        public long deleteDocuments(Term... terms) throws IOException {
            maybeThrowFailure();
            return super.deleteDocuments(terms);
        }

        public void setThrowFailure(Supplier<Exception> failureSupplier) {
            failureToThrow.set(failureSupplier);
        }

        public void clearFailure() {
            failureToThrow.set(null);
        }
    }

    public void testHandleDocumentFailure() throws Exception {
        try (Store store = createStore()) {
            final ParsedDocument doc1 = testParsedDocument("1", "test", null, -1, -1, testDocumentWithTextField(), B_1, null);
            final ParsedDocument doc2 = testParsedDocument("2", "test", null, -1, -1, testDocumentWithTextField(), B_1, null);
            final ParsedDocument doc3 = testParsedDocument("3", "test", null, -1, -1, testDocumentWithTextField(), B_1, null);

            ThrowingIndexWriter throwingIndexWriter = new ThrowingIndexWriter(store.directory(), new IndexWriterConfig());
            final IndexWriterFactory indexWriterFactory = (directory, iwc) -> throwingIndexWriter;
            try (Engine engine = createEngine(defaultSettings, store, createTempDir(), NoMergePolicy.INSTANCE, indexWriterFactory)) {
                // test document failure while indexing
                if (randomBoolean()) {
                    throwingIndexWriter.setThrowFailure(() -> new IOException("simulated"));
                } else {
                    throwingIndexWriter.setThrowFailure(() -> new IllegalArgumentException("simulated max token length"));
                }
                Engine.IndexResult indexResult = engine.index(indexForDoc(doc1));
                assertNotNull(indexResult.getFailure());

                throwingIndexWriter.clearFailure();
                indexResult = engine.index(indexForDoc(doc1));
                assertNull(indexResult.getFailure());
                engine.index(indexForDoc(doc2));

                // test failure while deleting
                // all these simulated exceptions are not fatal to the IW so we treat them as document failures
                if (randomBoolean()) {
                    throwingIndexWriter.setThrowFailure(() -> new IOException("simulated"));
                    assertThat(engine.delete(new Engine.Delete("test", "1", newUid(doc1))).getFailure(), instanceOf(IOException.class));
                } else {
                    throwingIndexWriter.setThrowFailure(() -> new IllegalArgumentException("simulated max token length"));
                    assertThat(engine.delete(new Engine.Delete("test", "1", newUid(doc1))).getFailure(),
                        instanceOf(IllegalArgumentException.class));
                }

                // test non document level failure is thrown
                if (randomBoolean()) {
                    // simulate close by corruption
                    throwingIndexWriter.setThrowFailure(null);
                    UncheckedIOException uncheckedIOException = expectThrows(UncheckedIOException.class, () -> {
                        Engine.Index index = indexForDoc(doc3);
                        index.parsedDoc().rootDoc().add(new StoredField("foo", "bar") {
                            // this is a hack to add a failure during store document which triggers a tragic event
                            // and in turn fails the engine
                            @Override
                            public BytesRef binaryValue() {
                                throw new UncheckedIOException(new MockDirectoryWrapper.FakeIOException());
                            }
                        });
                        engine.index(index);
                    });
                    assertTrue(uncheckedIOException.getCause() instanceof MockDirectoryWrapper.FakeIOException);
                } else {
                    // normal close
                    engine.close();
                }
                // now the engine is closed check we respond correctly
                try {
                    if (randomBoolean()) {
                        engine.index(indexForDoc(doc1));
                    } else {
                        engine.delete(new Engine.Delete("test", "", newUid(doc1)));
                    }
                    fail("engine should be closed");
                } catch (Exception e) {
                    assertThat(e, instanceOf(AlreadyClosedException.class));
                }
            }
        }
    }

    public void testDoubleDeliveryPrimary() throws IOException {
        final ParsedDocument doc = testParsedDocument("1", "test", null, System.currentTimeMillis(), -1L,
            testDocumentWithTextField(), new BytesArray("{}".getBytes(Charset.defaultCharset())), null);
        final Engine.Index operation = appendOnlyPrimary(doc, false, 1);
        final Engine.Index retry = appendOnlyPrimary(doc, true, 1);
        if (randomBoolean()) {
            Engine.IndexResult indexResult = engine.index(operation);
            assertFalse(engine.indexWriterHasDeletions());
            assertEquals(0, engine.getNumVersionLookups());
            assertNotNull(indexResult.getTranslogLocation());
            Engine.IndexResult retryResult = engine.index(retry);
            assertTrue(engine.indexWriterHasDeletions());
            assertEquals(0, engine.getNumVersionLookups());
            assertNotNull(retryResult.getTranslogLocation());
            assertTrue(retryResult.getTranslogLocation().compareTo(indexResult.getTranslogLocation()) > 0);
        } else {
            Engine.IndexResult retryResult = engine.index(retry);
            assertTrue(engine.indexWriterHasDeletions());
            assertEquals(0, engine.getNumVersionLookups());
            assertNotNull(retryResult.getTranslogLocation());
            Engine.IndexResult indexResult = engine.index(operation);
            assertTrue(engine.indexWriterHasDeletions());
            assertEquals(0, engine.getNumVersionLookups());
            assertNotNull(retryResult.getTranslogLocation());
            assertTrue(retryResult.getTranslogLocation().compareTo(indexResult.getTranslogLocation()) < 0);
        }

        engine.refresh("test");
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), 10);
            assertEquals(1, topDocs.totalHits);
        }
        if (randomBoolean()) {
            Engine.IndexResult indexResult = engine.index(operation);
            assertNotNull(indexResult.getTranslogLocation());
            Engine.IndexResult retryResult = engine.index(retry);
            assertNotNull(retryResult.getTranslogLocation());
            assertTrue(retryResult.getTranslogLocation().compareTo(indexResult.getTranslogLocation()) > 0);
        } else {
            Engine.IndexResult retryResult = engine.index(retry);
            assertNotNull(retryResult.getTranslogLocation());
            Engine.IndexResult indexResult = engine.index(operation);
            assertNotNull(indexResult.getTranslogLocation());
            assertTrue(retryResult.getTranslogLocation().compareTo(indexResult.getTranslogLocation()) < 0);
        }

        engine.refresh("test");
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), 10);
            assertEquals(1, topDocs.totalHits);
        }
    }

    public void testDoubleDeliveryReplicaAppendingOnly() throws IOException {
        final ParsedDocument doc = testParsedDocument("1", "test", null, System.currentTimeMillis(), -1,
            testDocumentWithTextField(), new BytesArray("{}".getBytes(Charset.defaultCharset())), null);
        final Engine.Index operation = appendOnlyReplica(doc, false, 1);
        final Engine.Index retry = appendOnlyReplica(doc, true, 1);
        if (randomBoolean()) {
            Engine.IndexResult indexResult = engine.index(operation);
            assertFalse(engine.indexWriterHasDeletions());
            assertEquals(0, engine.getNumVersionLookups());
            assertNotNull(indexResult.getTranslogLocation());
            Engine.IndexResult retryResult = engine.index(retry);
            assertFalse(engine.indexWriterHasDeletions());
            assertEquals(1, engine.getNumVersionLookups());
            assertNull(retryResult.getTranslogLocation()); // we didn't index it nor put it in the translog
        } else {
            Engine.IndexResult retryResult = engine.index(retry);
            assertFalse(engine.indexWriterHasDeletions());
            assertEquals(1, engine.getNumVersionLookups());
            assertNotNull(retryResult.getTranslogLocation());
            Engine.IndexResult indexResult = engine.index(operation);
            assertFalse(engine.indexWriterHasDeletions());
            assertEquals(2, engine.getNumVersionLookups());
            assertNull(indexResult.getTranslogLocation()); // we didn't index it nor put it in the translog
        }

        engine.refresh("test");
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), 10);
            assertEquals(1, topDocs.totalHits);
        }
        if (randomBoolean()) {
            Engine.IndexResult indexResult = engine.index(operation);
            assertNull(indexResult.getTranslogLocation()); // we don't index because a retry has already been processed.
            Engine.IndexResult retryResult = engine.index(retry);
            assertNull(retryResult.getTranslogLocation());
        } else {
            Engine.IndexResult retryResult = engine.index(retry);
            assertNull(retryResult.getTranslogLocation());
            Engine.IndexResult indexResult = engine.index(operation);
            assertNull(indexResult.getTranslogLocation());
        }

        engine.refresh("test");
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), 10);
            assertEquals(1, topDocs.totalHits);
        }
    }

    public void testDoubleDeliveryReplica() throws IOException {
        final ParsedDocument doc = testParsedDocument("1", "test", null, System.currentTimeMillis(), -1L,
            testDocumentWithTextField(), new BytesArray("{}".getBytes(Charset.defaultCharset())), null);
        Engine.Index operation = replicaIndexForDoc(doc, 20, false);
        Engine.Index duplicate = replicaIndexForDoc(doc, 20, true);
        if (randomBoolean()) {
            Engine.IndexResult indexResult = engine.index(operation);
            assertFalse(engine.indexWriterHasDeletions());
            assertEquals(1, engine.getNumVersionLookups());
            assertNotNull(indexResult.getTranslogLocation());
            if (randomBoolean()) {
                engine.refresh("test");
            }
            Engine.IndexResult retryResult = engine.index(duplicate);
            assertFalse(engine.indexWriterHasDeletions());
            assertEquals(2, engine.getNumVersionLookups());
            assertNull(retryResult.getTranslogLocation());
        } else {
            Engine.IndexResult retryResult = engine.index(duplicate);
            assertFalse(engine.indexWriterHasDeletions());
            assertEquals(1, engine.getNumVersionLookups());
            assertNotNull(retryResult.getTranslogLocation());
            if (randomBoolean()) {
                engine.refresh("test");
            }
            Engine.IndexResult indexResult = engine.index(operation);
            assertFalse(engine.indexWriterHasDeletions());
            assertEquals(2, engine.getNumVersionLookups());
            assertNull(indexResult.getTranslogLocation()); // we didn't index, no need to put in translog
        }

        engine.refresh("test");
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), 10);
            assertEquals(1, topDocs.totalHits);
        }
        engine.refresh("test");
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), 10);
            assertEquals(1, topDocs.totalHits);
        }
    }

    public void testRetryWithAutogeneratedIdWorksAndNoDuplicateDocs() throws IOException {

        final ParsedDocument doc = testParsedDocument("1", "test", null, 100, -1, testDocumentWithTextField(), new BytesArray("{}".getBytes(Charset.defaultCharset())), null);
        boolean isRetry = false;
        long autoGeneratedIdTimestamp = 0;

        Engine.Index index = new Engine.Index(newUid(doc), doc, Versions.MATCH_ANY, VersionType.INTERNAL, PRIMARY, System.nanoTime(), autoGeneratedIdTimestamp, isRetry);
        Engine.IndexResult indexResult = engine.index(index);
        assertThat(indexResult.getVersion(), equalTo(1L));

        index = new Engine.Index(newUid(doc), doc, indexResult.getVersion(), index.versionType().versionTypeForReplicationAndRecovery(), REPLICA, System.nanoTime(), autoGeneratedIdTimestamp, isRetry);
        Engine.IndexResult replicaResult = replicaEngine.index(index);
        assertThat(replicaResult.getVersion(), equalTo(1L));

        isRetry = true;
        index = new Engine.Index(newUid(doc), doc, Versions.MATCH_ANY, VersionType.INTERNAL, PRIMARY, System.nanoTime(), autoGeneratedIdTimestamp, isRetry);
        Engine.IndexResult indexResult1 = engine.index(index);
        assertThat(indexResult1.getVersion(), equalTo(1L));
        engine.refresh("test");
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), 10);
            assertEquals(1, topDocs.totalHits);
        }

        index = new Engine.Index(newUid(doc), doc, indexResult.getVersion(), index.versionType().versionTypeForReplicationAndRecovery(), REPLICA, System.nanoTime(), autoGeneratedIdTimestamp, isRetry);
        replicaEngine.index(index);
        replicaEngine.refresh("test");
        try (Engine.Searcher searcher = replicaEngine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), 10);
            assertEquals(1, topDocs.totalHits);
        }
    }

    public void testRetryWithAutogeneratedIdsAndWrongOrderWorksAndNoDuplicateDocs() throws IOException {

        final ParsedDocument doc = testParsedDocument("1", "test", null, 100, -1, testDocumentWithTextField(), new BytesArray("{}".getBytes(Charset.defaultCharset())), null);
        boolean isRetry = true;
        long autoGeneratedIdTimestamp = 0;


        Engine.Index firstIndexRequest = new Engine.Index(newUid(doc), doc, Versions.MATCH_ANY, VersionType.INTERNAL, PRIMARY, System.nanoTime(), autoGeneratedIdTimestamp, isRetry);
        Engine.IndexResult firstIndexResult = engine.index(firstIndexRequest);
        assertThat(firstIndexResult.getVersion(), equalTo(1L));

        Engine.Index firstIndexRequestReplica = new Engine.Index(newUid(doc), doc, firstIndexResult.getVersion(), firstIndexRequest.versionType().versionTypeForReplicationAndRecovery(), REPLICA, System.nanoTime(), autoGeneratedIdTimestamp, isRetry);
        Engine.IndexResult firstIndexReplicaResult = replicaEngine.index(firstIndexRequestReplica);
        assertThat(firstIndexReplicaResult.getVersion(), equalTo(1L));

        isRetry = false;
        Engine.Index secondIndexRequest = new Engine.Index(newUid(doc), doc, Versions.MATCH_ANY, VersionType.INTERNAL, PRIMARY, System.nanoTime(), autoGeneratedIdTimestamp, isRetry);
        Engine.IndexResult indexResult = engine.index(secondIndexRequest);
        assertTrue(indexResult.isCreated());
        engine.refresh("test");
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), 10);
            assertEquals(1, topDocs.totalHits);
        }

        Engine.Index secondIndexRequestReplica = new Engine.Index(newUid(doc), doc, firstIndexResult.getVersion(), firstIndexRequest.versionType().versionTypeForReplicationAndRecovery(), REPLICA, System.nanoTime(), autoGeneratedIdTimestamp, isRetry);
        replicaEngine.index(secondIndexRequestReplica);
        replicaEngine.refresh("test");
        try (Engine.Searcher searcher = replicaEngine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), 10);
            assertEquals(1, topDocs.totalHits);
        }
    }

    public Engine.Index randomAppendOnly(ParsedDocument doc, boolean retry, final long autoGeneratedIdTimestamp) {
        if (randomBoolean()) {
            return appendOnlyPrimary(doc, retry, autoGeneratedIdTimestamp);
        } else {
            return appendOnlyReplica(doc, retry, autoGeneratedIdTimestamp);
        }
    }

    public Engine.Index appendOnlyPrimary(ParsedDocument doc, boolean retry, final long autoGeneratedIdTimestamp) {
        return new Engine.Index(newUid(doc), doc, Versions.MATCH_ANY,
            VersionType.INTERNAL, Engine.Operation.Origin.PRIMARY, System.nanoTime(), autoGeneratedIdTimestamp, retry);
    }

    public Engine.Index appendOnlyReplica(ParsedDocument doc, boolean retry, final long autoGeneratedIdTimestamp) {
        return new Engine.Index(newUid(doc), doc, 1, VersionType.EXTERNAL,
            Engine.Operation.Origin.REPLICA, System.nanoTime(), autoGeneratedIdTimestamp, retry);
    }

    public void testRetryConcurrently() throws InterruptedException, IOException {
        Thread[] thread = new Thread[randomIntBetween(3, 5)];
        int numDocs = randomIntBetween(1000, 10000);
        List<Engine.Index> docs = new ArrayList<>();
        final boolean primary = randomBoolean();
        for (int i = 0; i < numDocs; i++) {
            final ParsedDocument doc = testParsedDocument(Integer.toString(i), "test", null,
                System.currentTimeMillis(), -1L, testDocumentWithTextField(),
                new BytesArray("{}".getBytes(Charset.defaultCharset())), null);
            final Engine.Index originalIndex;
            final Engine.Index retryIndex;
            if (primary) {
               originalIndex = appendOnlyPrimary(doc, false, i);
               retryIndex = appendOnlyPrimary(doc, true, i);
            } else {
                originalIndex = appendOnlyReplica(doc, false, i);
                retryIndex = appendOnlyReplica(doc, true, i);
            }
            docs.add(originalIndex);
            docs.add(retryIndex);
        }
        Collections.shuffle(docs, random());
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
                while ((docOffset = offset.incrementAndGet()) < docs.size()) {
                    try {
                        engine.index(docs.get(docOffset));
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
        if (primary) {
            assertEquals(0, engine.getNumVersionLookups());
            assertEquals(0, engine.getNumIndexVersionsLookups());
        } else {
            // we don't really know what order the operations will arrive and thus can't predict how many
            // version lookups will be needed
            assertThat(engine.getNumIndexVersionsLookups(), lessThanOrEqualTo(engine.getNumVersionLookups()));
        }
        engine.refresh("test");
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), 10);
            assertEquals(numDocs, topDocs.totalHits);
        }
        if (primary) {
            // primaries rely on lucene dedup and may index the same document twice
            assertTrue(engine.indexWriterHasDeletions());
        } else {
            // replicas rely on seq# based dedup and in this setup (same seq#) should never rely on lucene
            assertFalse(engine.indexWriterHasDeletions());
        }
    }

    public void testEngineMaxTimestampIsInitialized() throws IOException {
        try (Store store = createStore();
             Engine engine = new InternalEngine(config(defaultSettings, store, createTempDir(), NoMergePolicy.INSTANCE,
                 IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, null))) {
            assertEquals(IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, engine.segmentsStats(false).getMaxUnsafeAutoIdTimestamp());

        }

        final long timestamp1 = Math.abs(randomLong());
        final Path storeDir = createTempDir();
        final Path translogDir = createTempDir();
        try (Store store = createStore(newFSDirectory(storeDir));
             Engine engine = new InternalEngine(
                 config(defaultSettings, store, translogDir, NoMergePolicy.INSTANCE, timestamp1, null) )) {
            assertEquals(timestamp1, engine.segmentsStats(false).getMaxUnsafeAutoIdTimestamp());
        }
        final long timestamp2 = randomNonNegativeLong();
        final long timestamp3 = randomNonNegativeLong();
        final long maxTimestamp12 = Math.max(timestamp1, timestamp2);
        final long maxTimestamp123 = Math.max(maxTimestamp12, timestamp3);
        try (Store store = createStore(newFSDirectory(storeDir));
             Engine engine = new InternalEngine(
                 copy(config(defaultSettings, store, translogDir, NoMergePolicy.INSTANCE, timestamp2, null),
                     randomFrom(EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG, EngineConfig.OpenMode.OPEN_INDEX_CREATE_TRANSLOG)))) {
            assertEquals(maxTimestamp12, engine.segmentsStats(false).getMaxUnsafeAutoIdTimestamp());
            if (engine.config().getOpenMode() == EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG) {
                // recover from translog and commit maxTimestamp12
                engine.recoverFromTranslog();
                // force flush as the were no ops performed
                engine.flush(true, false);
            }
            final ParsedDocument doc = testParsedDocument("1", "test", null, 1, -1, testDocumentWithTextField(),
                new BytesArray("{}".getBytes(Charset.defaultCharset())), null);
            engine.index(appendOnlyPrimary(doc, true, timestamp3));
            assertEquals(maxTimestamp123, engine.segmentsStats(false).getMaxUnsafeAutoIdTimestamp());
        }
        try (Store store = createStore(newFSDirectory(storeDir));
             Engine engine = new InternalEngine(
                 config(defaultSettings, store, translogDir, NoMergePolicy.INSTANCE, IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, null))) {
            assertEquals(maxTimestamp12, engine.segmentsStats(false).getMaxUnsafeAutoIdTimestamp());
            engine.recoverFromTranslog();
            assertEquals(maxTimestamp123, engine.segmentsStats(false).getMaxUnsafeAutoIdTimestamp());
        }
    }

    public void testAppendConcurrently() throws InterruptedException, IOException {
        Thread[] thread = new Thread[randomIntBetween(3, 5)];
        int numDocs = randomIntBetween(1000, 10000);
        assertEquals(0, engine.getNumVersionLookups());
        assertEquals(0, engine.getNumIndexVersionsLookups());
        List<Engine.Index> docs = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            final ParsedDocument doc = testParsedDocument(Integer.toString(i), "test", null, i, -1, testDocumentWithTextField(), new BytesArray("{}".getBytes(Charset.defaultCharset())), null);
            Engine.Index index = randomAppendOnly(doc, false, i);
            docs.add(index);
        }
        Collections.shuffle(docs, random());
        CountDownLatch startGun = new CountDownLatch(thread.length);
        AtomicInteger offset = new AtomicInteger(-1);
        for (int i = 0; i < thread.length; i++) {
            thread[i] = new Thread() {
                @Override
                public void run() {
                    startGun.countDown();
                    try {
                        startGun.await();
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }
                    int docOffset;
                    while ((docOffset = offset.incrementAndGet()) < docs.size()) {
                        try {
                            engine.index(docs.get(docOffset));
                        } catch (IOException e) {
                            throw new AssertionError(e);
                        }
                    }
                }
            };
            thread[i].start();
        }
        for (int i = 0; i < thread.length; i++) {
            thread[i].join();
        }

        engine.refresh("test");
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), 10);
            assertEquals(docs.size(), topDocs.totalHits);
        }
        assertEquals(0, engine.getNumVersionLookups());
        assertEquals(0, engine.getNumIndexVersionsLookups());
        assertFalse(engine.indexWriterHasDeletions());

    }

    public static long getNumVersionLookups(InternalEngine engine) { // for other tests to access this
        return engine.getNumVersionLookups();
    }

    public static long getNumIndexVersionsLookups(InternalEngine engine) { // for other tests to access this
        return engine.getNumIndexVersionsLookups();
    }

    public void testFailEngineOnRandomIO() throws IOException, InterruptedException {
        MockDirectoryWrapper wrapper = newMockDirectory();
        final Path translogPath = createTempDir("testFailEngineOnRandomIO");
        try (Store store = createStore(wrapper)) {
            CyclicBarrier join = new CyclicBarrier(2);
            CountDownLatch start = new CountDownLatch(1);
            AtomicInteger controller = new AtomicInteger(0);
            EngineConfig config = config(defaultSettings, store, translogPath, newMergePolicy(),
                IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, new ReferenceManager.RefreshListener() {
                    @Override
                    public void beforeRefresh() throws IOException {
                    }

                    @Override
                    public void afterRefresh(boolean didRefresh) throws IOException {
                        int i = controller.incrementAndGet();
                        if (i == 1) {
                            throw new MockDirectoryWrapper.FakeIOException();
                        } else if (i == 2) {
                            try {
                                start.await();
                            } catch (InterruptedException e) {
                                throw new AssertionError(e);
                            }
                            throw new ElasticsearchException("something completely different");
                        }
                    }
                });
            InternalEngine internalEngine = new InternalEngine(config);
            int docId = 0;
            final ParsedDocument doc = testParsedDocument(Integer.toString(docId), "test", null, docId, -1,
                testDocumentWithTextField(), new BytesArray("{}".getBytes(Charset.defaultCharset())), null);

            Engine.Index index = randomBoolean() ? indexForDoc(doc) : randomAppendOnly(doc, false, docId);
            internalEngine.index(index);
            Runnable r = () ->  {
                try {
                    join.await();
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
                try {
                    internalEngine.refresh("test");
                    fail();
                } catch (AlreadyClosedException ex) {
                    if (ex.getCause() != null) {
                        assertTrue(ex.toString(), ex.getCause() instanceof MockDirectoryWrapper.FakeIOException);
                    }
                } catch (RefreshFailedEngineException ex) {
                    // fine
                } finally {
                    start.countDown();
                }

            };
            Thread t = new Thread(r);
            Thread t1 = new Thread(r);
            t.start();
            t1.start();
            t.join();
            t1.join();
            assertTrue(internalEngine.isClosed.get());
            assertTrue(internalEngine.failedEngine.get() instanceof MockDirectoryWrapper.FakeIOException);
        }
    }

    public void testTragicEventErrorBubblesUp() throws IOException {
        engine.close();
        final AtomicBoolean failWithFatalError = new AtomicBoolean(true);
        final VirtualMachineError error = randomFrom(
            new InternalError(),
            new OutOfMemoryError(),
            new StackOverflowError(),
            new UnknownError());
        engine = new InternalEngine(copy(engine.config(), EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG, new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                return new TokenStreamComponents(new Tokenizer() {
                    @Override
                    public boolean incrementToken() throws IOException {
                        if (failWithFatalError.get()) {
                            throw error;
                        } else {
                            throw new AssertionError("should not get to this point");
                        }
                    }
                });
            }
        }));
        final Document document = testDocument();
        document.add(new TextField("value", "test", Field.Store.YES));
        final ParsedDocument firstDoc = testParsedDocument("1", "test", null, -1, -1, document, B_1, null);
        final Engine.Index first = indexForDoc(firstDoc);
        expectThrows(error.getClass(), () -> engine.index(first));
        failWithFatalError.set(false);
        final ParsedDocument secondDoc = testParsedDocument("2", "test", null, -1, -1, document, B_1, null);
        final Engine.Index second = indexForDoc(secondDoc);
        expectThrows(error.getClass(), () -> engine.index(second));
        assertNull(engine.failedEngine.get());
    }

}
