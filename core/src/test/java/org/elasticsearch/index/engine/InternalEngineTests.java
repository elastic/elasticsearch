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
import org.apache.lucene.codecs.lucene50.Lucene50StoredFieldsFormat;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.LiveIndexWriterConfig;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.lucene.uid.VersionsAndSeqNoResolver;
import org.elasticsearch.common.lucene.uid.VersionsAndSeqNoResolver.DocIdAndSeqNo;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.engine.Engine.Searcher;
import org.elasticsearch.index.fieldvisitor.FieldsVisitor;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.Mapper.BuilderContext;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.RootObjectMapper;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.seqno.SequenceNumbersService;
import org.elasticsearch.index.shard.IndexSearcherWrapper;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardUtils;
import org.elasticsearch.index.store.DirectoryUtils;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogConfig;
import org.elasticsearch.test.IndexSettingsModule;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.function.ToLongBiFunction;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static java.util.Collections.emptyMap;
import static java.util.Collections.shuffle;
import static org.elasticsearch.index.engine.Engine.Operation.Origin.LOCAL_TRANSLOG_RECOVERY;
import static org.elasticsearch.index.engine.Engine.Operation.Origin.PEER_RECOVERY;
import static org.elasticsearch.index.engine.Engine.Operation.Origin.PRIMARY;
import static org.elasticsearch.index.engine.Engine.Operation.Origin.REPLICA;
import static org.elasticsearch.index.translog.TranslogDeletionPolicies.createTranslogDeletionPolicy;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class InternalEngineTests extends EngineTestCase {

    public void testSegments() throws Exception {
        try (Store store = createStore();
             InternalEngine engine = createEngine(defaultSettings, store, createTempDir(), NoMergePolicy.INSTANCE)) {
            List<Segment> segments = engine.segments(false);
            assertThat(segments.isEmpty(), equalTo(true));
            assertThat(engine.segmentsStats(false).getCount(), equalTo(0L));
            assertThat(engine.segmentsStats(false).getMemoryInBytes(), equalTo(0L));

            // create two docs and refresh
            ParsedDocument doc = testParsedDocument("1", null, testDocumentWithTextField(), B_1, null);
            Engine.Index first = indexForDoc(doc);
            Engine.IndexResult firstResult = engine.index(first);
            ParsedDocument doc2 = testParsedDocument("2", null, testDocumentWithTextField(), B_2, null);
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
            assertThat(segments.get(0).getAttributes().keySet(), Matchers.contains(Lucene50StoredFieldsFormat.MODE_KEY));

            engine.flush();

            segments = engine.segments(false);
            assertThat(segments.size(), equalTo(1));
            assertThat(engine.segmentsStats(false).getCount(), equalTo(1L));
            assertThat(segments.get(0).isCommitted(), equalTo(true));
            assertThat(segments.get(0).isSearch(), equalTo(true));
            assertThat(segments.get(0).getNumDocs(), equalTo(2));
            assertThat(segments.get(0).getDeletedDocs(), equalTo(0));
            assertThat(segments.get(0).isCompound(), equalTo(true));

            ParsedDocument doc3 = testParsedDocument("3", null, testDocumentWithTextField(), B_3, null);
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
            ParsedDocument doc4 = testParsedDocument("4", null, testDocumentWithTextField(), B_3, null);
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

            // internal refresh - lets make sure we see those segments in the stats
            ParsedDocument doc5 = testParsedDocument("5", null, testDocumentWithTextField(), B_3, null);
            engine.index(indexForDoc(doc5));
            engine.refresh("test", Engine.SearcherScope.INTERNAL);

            segments = engine.segments(false);
            assertThat(segments.size(), equalTo(4));
            assertThat(engine.segmentsStats(false).getCount(), equalTo(4L));
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

            assertThat(segments.get(3).isCommitted(), equalTo(false));
            assertThat(segments.get(3).isSearch(), equalTo(false));
            assertThat(segments.get(3).getNumDocs(), equalTo(1));
            assertThat(segments.get(3).getDeletedDocs(), equalTo(0));
            assertThat(segments.get(3).isCompound(), equalTo(true));

            // now refresh the external searcher and make sure it has the new segment
            engine.refresh("test");
            segments = engine.segments(false);
            assertThat(segments.size(), equalTo(4));
            assertThat(engine.segmentsStats(false).getCount(), equalTo(4L));
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

            assertThat(segments.get(3).isCommitted(), equalTo(false));
            assertThat(segments.get(3).isSearch(), equalTo(true));
            assertThat(segments.get(3).getNumDocs(), equalTo(1));
            assertThat(segments.get(3).getDeletedDocs(), equalTo(0));
            assertThat(segments.get(3).isCompound(), equalTo(true));
        }
    }

    public void testVerboseSegments() throws Exception {
        try (Store store = createStore();
             Engine engine = createEngine(defaultSettings, store, createTempDir(), NoMergePolicy.INSTANCE)) {
            List<Segment> segments = engine.segments(true);
            assertThat(segments.isEmpty(), equalTo(true));

            ParsedDocument doc = testParsedDocument("1", null, testDocumentWithTextField(), B_1, null);
            engine.index(indexForDoc(doc));
            engine.refresh("test");

            segments = engine.segments(true);
            assertThat(segments.size(), equalTo(1));
            assertThat(segments.get(0).ramTree, notNullValue());

            ParsedDocument doc2 = testParsedDocument("2", null, testDocumentWithTextField(), B_2, null);
            engine.index(indexForDoc(doc2));
            engine.refresh("test");
            ParsedDocument doc3 = testParsedDocument("3", null, testDocumentWithTextField(), B_3, null);
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
            ParsedDocument doc = testParsedDocument("1", null, testDocument(), B_1, null);
            Engine.Index index = indexForDoc(doc);
            engine.index(index);
            engine.flush();
            assertThat(engine.segments(false).size(), equalTo(1));
            index = indexForDoc(testParsedDocument("2", null, testDocument(), B_1, null));
            engine.index(index);
            engine.flush();
            List<Segment> segments = engine.segments(false);
            assertThat(segments.size(), equalTo(2));
            for (Segment segment : segments) {
                assertThat(segment.getMergeId(), nullValue());
            }
            index = indexForDoc(testParsedDocument("3", null, testDocument(), B_1, null));
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

    public void testSegmentsWithIndexSort() throws Exception {
        Sort indexSort = new Sort(new SortedSetSortField("_type", false));
        try (Store store = createStore();
             Engine engine =
                     createEngine(defaultSettings, store, createTempDir(), NoMergePolicy.INSTANCE, null, null, null, indexSort)) {
            List<Segment> segments = engine.segments(true);
            assertThat(segments.isEmpty(), equalTo(true));

            ParsedDocument doc = testParsedDocument("1", null, testDocumentWithTextField(), B_1, null);
            engine.index(indexForDoc(doc));
            engine.refresh("test");

            segments = engine.segments(false);
            assertThat(segments.size(), equalTo(1));
            assertThat(segments.get(0).getSegmentSort(), equalTo(indexSort));

            ParsedDocument doc2 = testParsedDocument("2", null, testDocumentWithTextField(), B_2, null);
            engine.index(indexForDoc(doc2));
            engine.refresh("test");
            ParsedDocument doc3 = testParsedDocument("3", null, testDocumentWithTextField(), B_3, null);
            engine.index(indexForDoc(doc3));
            engine.refresh("test");

            segments = engine.segments(true);
            assertThat(segments.size(), equalTo(3));
            assertThat(segments.get(0).getSegmentSort(), equalTo(indexSort));
            assertThat(segments.get(1).getSegmentSort(), equalTo(indexSort));
            assertThat(segments.get(2).getSegmentSort(), equalTo(indexSort));
        }
    }

    public void testSegmentsStatsIncludingFileSizes() throws Exception {
        try (Store store = createStore();
             Engine engine = createEngine(defaultSettings, store, createTempDir(), NoMergePolicy.INSTANCE)) {
            assertThat(engine.segmentsStats(true).getFileSizes().size(), equalTo(0));

            ParsedDocument doc = testParsedDocument("1", null, testDocumentWithTextField(), B_1, null);
            engine.index(indexForDoc(doc));
            engine.refresh("test");

            SegmentsStats stats = engine.segmentsStats(true);
            assertThat(stats.getFileSizes().size(), greaterThan(0));
            assertThat(() -> stats.getFileSizes().valuesIt(), everyItem(greaterThan(0L)));

            ObjectObjectCursor<String, Long> firstEntry = stats.getFileSizes().iterator().next();

            ParsedDocument doc2 = testParsedDocument("2", null, testDocumentWithTextField(), B_2, null);
            engine.index(indexForDoc(doc2));
            engine.refresh("test");

            assertThat(engine.segmentsStats(true).getFileSizes().get(firstEntry.key), greaterThan(firstEntry.value));
        }
    }

    public void testCommitStats() throws IOException {
        final AtomicLong maxSeqNo = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        final AtomicLong localCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.UNASSIGNED_SEQ_NO);
        try (
            Store store = createStore();
            InternalEngine engine = createEngine(store, createTempDir(), (config, seqNoStats) -> new SequenceNumbersService(
                            config.getShardId(),
                            config.getAllocationId(),
                            config.getIndexSettings(),
                            seqNoStats.getMaxSeqNo(),
                            seqNoStats.getLocalCheckpoint(),
                            seqNoStats.getGlobalCheckpoint()) {
                        @Override
                        public long getMaxSeqNo() {
                            return maxSeqNo.get();
                        }

                        @Override
                        public long getLocalCheckpoint() {
                            return localCheckpoint.get();
                        }

                        @Override
                        public long getGlobalCheckpoint() {
                            return globalCheckpoint.get();
                        }
                    }
            )) {
            CommitStats stats1 = engine.commitStats();
            assertThat(stats1.getGeneration(), greaterThan(0L));
            assertThat(stats1.getId(), notNullValue());
            assertThat(stats1.getUserData(), hasKey(Translog.TRANSLOG_GENERATION_KEY));
            assertThat(stats1.getUserData(), hasKey(SequenceNumbers.LOCAL_CHECKPOINT_KEY));
            assertThat(
                Long.parseLong(stats1.getUserData().get(SequenceNumbers.LOCAL_CHECKPOINT_KEY)),
                equalTo(SequenceNumbers.NO_OPS_PERFORMED));

            assertThat(stats1.getUserData(), hasKey(SequenceNumbers.MAX_SEQ_NO));
            assertThat(
                Long.parseLong(stats1.getUserData().get(SequenceNumbers.MAX_SEQ_NO)),
                equalTo(SequenceNumbers.NO_OPS_PERFORMED));

            maxSeqNo.set(rarely() ? SequenceNumbers.NO_OPS_PERFORMED : randomIntBetween(0, 1024));
            localCheckpoint.set(
                rarely() || maxSeqNo.get() == SequenceNumbers.NO_OPS_PERFORMED ?
                    SequenceNumbers.NO_OPS_PERFORMED : randomIntBetween(0, 1024));
            globalCheckpoint.set(rarely() || localCheckpoint.get() == SequenceNumbers.NO_OPS_PERFORMED ?
                SequenceNumbers.UNASSIGNED_SEQ_NO : randomIntBetween(0, (int) localCheckpoint.get()));

            engine.flush(true, true);

            CommitStats stats2 = engine.commitStats();
            assertThat(stats2.getGeneration(), greaterThan(stats1.getGeneration()));
            assertThat(stats2.getId(), notNullValue());
            assertThat(stats2.getId(), not(equalTo(stats1.getId())));
            assertThat(stats2.getUserData(), hasKey(Translog.TRANSLOG_GENERATION_KEY));
            assertThat(stats2.getUserData(), hasKey(Translog.TRANSLOG_UUID_KEY));
            assertThat(
                stats2.getUserData().get(Translog.TRANSLOG_GENERATION_KEY),
                not(equalTo(stats1.getUserData().get(Translog.TRANSLOG_GENERATION_KEY))));
            assertThat(stats2.getUserData().get(Translog.TRANSLOG_UUID_KEY), equalTo(stats1.getUserData().get(Translog.TRANSLOG_UUID_KEY)));
            assertThat(Long.parseLong(stats2.getUserData().get(SequenceNumbers.LOCAL_CHECKPOINT_KEY)), equalTo(localCheckpoint.get()));
            assertThat(stats2.getUserData(), hasKey(SequenceNumbers.MAX_SEQ_NO));
            assertThat(Long.parseLong(stats2.getUserData().get(SequenceNumbers.MAX_SEQ_NO)), equalTo(maxSeqNo.get()));
        }
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
        ParsedDocument doc = testParsedDocument("1", null, testDocumentWithTextField(), SOURCE, null);
        engine.index(indexForDoc(doc));
        engine.close();

        engine = new InternalEngine(copy(engine.config(), EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG));
        expectThrows(IllegalStateException.class, () -> engine.flush(true, true));
        assertTrue(engine.isRecovering());
        engine.recoverFromTranslog();
        assertFalse(engine.isRecovering());
        doc = testParsedDocument("2", null, testDocumentWithTextField(), SOURCE, null);
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
                final ParsedDocument doc = testParsedDocument("1", null, testDocumentWithTextField(), SOURCE, null);
                if (randomBoolean()) {
                    final Engine.Index operation = new Engine.Index(newUid(doc), doc, SequenceNumbers.UNASSIGNED_SEQ_NO, 0, i, VersionType.EXTERNAL, Engine.Operation.Origin.PRIMARY, System.nanoTime(), -1, false);
                    operations.add(operation);
                    initialEngine.index(operation);
                } else {
                    final Engine.Delete operation = new Engine.Delete("test", "1", newUid(doc), SequenceNumbers.UNASSIGNED_SEQ_NO, 0, i, VersionType.EXTERNAL, Engine.Operation.Origin.PRIMARY, System.nanoTime());
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
                final ParsedDocument doc = testParsedDocument(id, null, testDocumentWithTextField(), SOURCE, null);
                initialEngine.index(indexForDoc(doc));
            }
        } finally {
            IOUtils.close(initialEngine);
        }

        Engine recoveringEngine = null;
        try {
            final AtomicBoolean flushed = new AtomicBoolean();
            recoveringEngine = new InternalEngine(copy(initialEngine.config(), EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG)) {
                @Override
                public CommitId flush(boolean force, boolean waitIfOngoing) throws EngineException {
                    assertThat(getTranslog().uncommittedOperations(), equalTo(docs));
                    final CommitId commitId = super.flush(force, waitIfOngoing);
                    flushed.set(true);
                    return commitId;
                }
            };

            assertThat(recoveringEngine.getTranslog().uncommittedOperations(), equalTo(docs));
            recoveringEngine.recoverFromTranslog();
            assertTrue(flushed.get());
        } finally {
            IOUtils.close(recoveringEngine);
        }
    }

    public void testTranslogRecoveryWithMultipleGenerations() throws IOException {
        final int docs = randomIntBetween(1, 4096);
        final List<Long> seqNos = LongStream.range(0, docs).boxed().collect(Collectors.toList());
        Randomness.shuffle(seqNos);
        Engine initialEngine = null;
        Engine recoveringEngine = null;
        Store store = createStore();
        final AtomicInteger counter = new AtomicInteger();
        try {
            initialEngine = createEngine(
                    store,
                    createTempDir(),
                    InternalEngine::sequenceNumberService,
                    (engine, operation) -> seqNos.get(counter.getAndIncrement()));
            for (int i = 0; i < docs; i++) {
                final String id = Integer.toString(i);
                final ParsedDocument doc = testParsedDocument(id, null, testDocumentWithTextField(), SOURCE, null);
                initialEngine.index(indexForDoc(doc));
                if (rarely()) {
                    initialEngine.getTranslog().rollGeneration();
                } else if (rarely()) {
                    initialEngine.flush();
                }
            }
            initialEngine.close();
            recoveringEngine = new InternalEngine(copy(initialEngine.config(), EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG));
            recoveringEngine.recoverFromTranslog();
            try (Engine.Searcher searcher = recoveringEngine.acquireSearcher("test")) {
                TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), docs);
                assertEquals(docs, topDocs.totalHits);
            }
        } finally {
            IOUtils.close(initialEngine, recoveringEngine, store);
        }
    }

    public void testConcurrentGetAndFlush() throws Exception {
        ParsedDocument doc = testParsedDocument("1", null, testDocumentWithTextField(), B_1, null);
        engine.index(indexForDoc(doc));

        final AtomicReference<Engine.GetResult> latestGetResult = new AtomicReference<>();
        final BiFunction<String, Engine.SearcherScope, Searcher> searcherFactory = engine::acquireSearcher;
        latestGetResult.set(engine.get(newGet(true, doc), searcherFactory));
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
                latestGetResult.set(engine.get(newGet(true, doc), searcherFactory));
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

        final BiFunction<String, Engine.SearcherScope, Searcher> searcherFactory = engine::acquireSearcher;

        // create a document
        Document document = testDocumentWithTextField();
        document.add(new Field(SourceFieldMapper.NAME, BytesReference.toBytes(B_1), SourceFieldMapper.Defaults.FIELD_TYPE));
        ParsedDocument doc = testParsedDocument("1", null, document, B_1, null);
        engine.index(indexForDoc(doc));

        // its not there...
        searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(0));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 0));
        searchResult.close();

        // but, not there non realtime
        Engine.GetResult getResult = engine.get(newGet(false, doc), searcherFactory);
        assertThat(getResult.exists(), equalTo(false));
        getResult.release();

        // but, we can still get it (in realtime)
        getResult = engine.get(newGet(true, doc), searcherFactory);
        assertThat(getResult.exists(), equalTo(true));
        assertThat(getResult.docIdAndVersion(), notNullValue());
        getResult.release();

        // but not real time is not yet visible
        getResult = engine.get(newGet(false, doc), searcherFactory);
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
        getResult = engine.get(newGet(false, doc), searcherFactory);
        assertThat(getResult.exists(), equalTo(true));
        assertThat(getResult.docIdAndVersion(), notNullValue());
        getResult.release();

        // now do an update
        document = testDocument();
        document.add(new TextField("value", "test1", Field.Store.YES));
        document.add(new Field(SourceFieldMapper.NAME, BytesReference.toBytes(B_2), SourceFieldMapper.Defaults.FIELD_TYPE));
        doc = testParsedDocument("1", null, document, B_2, null);
        engine.index(indexForDoc(doc));

        // its not updated yet...
        searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(1));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 1));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(new TermQuery(new Term("value", "test1")), 0));
        searchResult.close();

        // but, we can still get it (in realtime)
        getResult = engine.get(newGet(true, doc), searcherFactory);
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
        getResult = engine.get(newGet(true, doc), searcherFactory);
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
        doc = testParsedDocument("1", null, document, B_1, null);
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
        getResult = engine.get(newGet(true, doc), searcherFactory);
        assertThat(getResult.exists(), equalTo(true));
        assertThat(getResult.docIdAndVersion(), notNullValue());
        getResult.release();

        // make sure we can still work with the engine
        // now do an update
        document = testDocument();
        document.add(new TextField("value", "test1", Field.Store.YES));
        doc = testParsedDocument("1", null, document, B_1, null);
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
        ParsedDocument doc = testParsedDocument("1", null, testDocumentWithTextField(), B_1, null);
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

    public void testCommitAdvancesMinTranslogForRecovery() throws IOException {
        ParsedDocument doc = testParsedDocument("1", null, testDocumentWithTextField(), B_1, null);
        engine.index(indexForDoc(doc));
        engine.flush();
        assertThat(engine.getTranslog().currentFileGeneration(), equalTo(2L));
        assertThat(engine.getTranslog().getDeletionPolicy().getMinTranslogGenerationForRecovery(), equalTo(2L));
        engine.flush();
        assertThat(engine.getTranslog().currentFileGeneration(), equalTo(2L));
        assertThat(engine.getTranslog().getDeletionPolicy().getMinTranslogGenerationForRecovery(), equalTo(2L));
        engine.flush(true, true);
        assertThat(engine.getTranslog().currentFileGeneration(), equalTo(3L));
        assertThat(engine.getTranslog().getDeletionPolicy().getMinTranslogGenerationForRecovery(), equalTo(3L));
    }

    public void testSyncedFlush() throws IOException {
        try (Store store = createStore();
             Engine engine = new InternalEngine(config(defaultSettings, store, createTempDir(), new LogByteSizeMergePolicy(), null))) {
            final String syncId = randomUnicodeOfCodepointLengthBetween(10, 20);
            ParsedDocument doc = testParsedDocument("1", null, testDocumentWithTextField(), B_1, null);
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
                     new LogDocMergePolicy(), null))) {
                final String syncId = randomUnicodeOfCodepointLengthBetween(10, 20);
                Engine.Index doc1 = indexForDoc(testParsedDocument("1", null, testDocumentWithTextField(), B_1, null));
                engine.index(doc1);
                assertEquals(engine.getLastWriteNanos(), doc1.startTime());
                engine.flush();
                Engine.Index doc2 = indexForDoc(testParsedDocument("2", null, testDocumentWithTextField(), B_1, null));
                engine.index(doc2);
                assertEquals(engine.getLastWriteNanos(), doc2.startTime());
                engine.flush();
                final boolean forceMergeFlushes = randomBoolean();
                final ParsedDocument parsedDoc3 = testParsedDocument("3", null, testDocumentWithTextField(), B_1, null);
                if (forceMergeFlushes) {
                    engine.index(new Engine.Index(newUid(parsedDoc3), parsedDoc3, SequenceNumbers.UNASSIGNED_SEQ_NO, 0, Versions.MATCH_ANY, VersionType.INTERNAL, Engine.Operation.Origin.PRIMARY, System.nanoTime() - engine.engineConfig.getFlushMergesAfter().nanos(), -1, false));
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
                    engine.refresh("test");
                    assertBusy(() -> assertEquals(1, engine.segments(false).size()));
                }
                assertEquals(store.readLastCommittedSegmentsInfo().getUserData().get(Engine.SYNC_COMMIT_ID), syncId);
                assertEquals(engine.getLastCommittedSegmentInfos().getUserData().get(Engine.SYNC_COMMIT_ID), syncId);

                if (randomBoolean()) {
                    Engine.Index doc4 = indexForDoc(testParsedDocument("4", null, testDocumentWithTextField(), B_1, null));
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

    public void testSyncedFlushSurvivesEngineRestart() throws IOException {
        final String syncId = randomUnicodeOfCodepointLengthBetween(10, 20);
        ParsedDocument doc = testParsedDocument("1", null, testDocumentWithTextField(), B_1, null);
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

    public void testSyncedFlushVanishesOnReplay() throws IOException {
        final String syncId = randomUnicodeOfCodepointLengthBetween(10, 20);
        ParsedDocument doc = testParsedDocument("1", null, testDocumentWithTextField(), B_1, null);
        engine.index(indexForDoc(doc));
        final Engine.CommitId commitID = engine.flush();
        assertEquals("should succeed to flush commit with right id and no pending doc", engine.syncFlush(syncId, commitID),
            Engine.SyncedFlushResult.SUCCESS);
        assertEquals(store.readLastCommittedSegmentsInfo().getUserData().get(Engine.SYNC_COMMIT_ID), syncId);
        assertEquals(engine.getLastCommittedSegmentInfos().getUserData().get(Engine.SYNC_COMMIT_ID), syncId);
        doc = testParsedDocument("2", null, testDocumentWithTextField(), new BytesArray("{}"), null);
        engine.index(indexForDoc(doc));
        EngineConfig config = engine.config();
        engine.close();
        engine = new InternalEngine(copy(config, EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG));
        engine.recoverFromTranslog();
        assertNull("Sync ID must be gone since we have a document to replay", engine.getLastCommittedSegmentInfos().getUserData().get(Engine.SYNC_COMMIT_ID));
    }

    public void testVersioningNewCreate() throws IOException {
        ParsedDocument doc = testParsedDocument("1", null, testDocument(), B_1, null);
        Engine.Index create = new Engine.Index(newUid(doc), doc, Versions.MATCH_DELETED);
        Engine.IndexResult indexResult = engine.index(create);
        assertThat(indexResult.getVersion(), equalTo(1L));

        create = new Engine.Index(newUid(doc), doc, indexResult.getSeqNo(), create.primaryTerm(), indexResult.getVersion(),
            create.versionType().versionTypeForReplicationAndRecovery(), REPLICA, 0, -1, false);
        indexResult = replicaEngine.index(create);
        assertThat(indexResult.getVersion(), equalTo(1L));
    }

    public void testReplicatedVersioningWithFlush() throws IOException {
        ParsedDocument doc = testParsedDocument("1", null, testDocument(), B_1, null);
        Engine.Index create = new Engine.Index(newUid(doc), doc, Versions.MATCH_DELETED);
        Engine.IndexResult indexResult = engine.index(create);
        assertThat(indexResult.getVersion(), equalTo(1L));
        assertTrue(indexResult.isCreated());


        create = new Engine.Index(newUid(doc), doc, indexResult.getSeqNo(), create.primaryTerm(), indexResult.getVersion(),
            create.versionType().versionTypeForReplicationAndRecovery(), REPLICA, 0, -1, false);
        indexResult = replicaEngine.index(create);
        assertThat(indexResult.getVersion(), equalTo(1L));
        assertTrue(indexResult.isCreated());

        if (randomBoolean()) {
            engine.flush();
        }
        if (randomBoolean()) {
            replicaEngine.flush();
        }

        Engine.Index update = new Engine.Index(newUid(doc), doc, 1);
        Engine.IndexResult updateResult = engine.index(update);
        assertThat(updateResult.getVersion(), equalTo(2L));
        assertFalse(updateResult.isCreated());


        update = new Engine.Index(newUid(doc), doc, updateResult.getSeqNo(), update.primaryTerm(), updateResult.getVersion(),
            update.versionType().versionTypeForReplicationAndRecovery(), REPLICA, 0, -1, false);
        updateResult = replicaEngine.index(update);
        assertThat(updateResult.getVersion(), equalTo(2L));
        assertFalse(updateResult.isCreated());
        replicaEngine.refresh("test");
        try (Searcher searcher = replicaEngine.acquireSearcher("test")) {
            assertEquals(1, searcher.getDirectoryReader().numDocs());
        }

        engine.refresh("test");
        try (Searcher searcher = engine.acquireSearcher("test")) {
            assertEquals(1, searcher.getDirectoryReader().numDocs());
        }
    }

    /**
     * simulates what an upsert / update API does
     */
    public void testVersionedUpdate() throws IOException {
        final BiFunction<String, Engine.SearcherScope, Searcher> searcherFactory = engine::acquireSearcher;

        ParsedDocument doc = testParsedDocument("1", null, testDocument(), B_1, null);
        Engine.Index create = new Engine.Index(newUid(doc), doc, Versions.MATCH_DELETED);
        Engine.IndexResult indexResult = engine.index(create);
        assertThat(indexResult.getVersion(), equalTo(1L));
        try (Engine.GetResult get = engine.get(new Engine.Get(true, doc.type(), doc.id(), create.uid()), searcherFactory)) {
            assertEquals(1, get.version());
        }

        Engine.Index update_1 = new Engine.Index(newUid(doc), doc, 1);
        Engine.IndexResult update_1_result = engine.index(update_1);
        assertThat(update_1_result.getVersion(), equalTo(2L));

        try (Engine.GetResult get = engine.get(new Engine.Get(true, doc.type(), doc.id(), create.uid()), searcherFactory)) {
            assertEquals(2, get.version());
        }

        Engine.Index update_2 = new Engine.Index(newUid(doc), doc, 2);
        Engine.IndexResult update_2_result = engine.index(update_2);
        assertThat(update_2_result.getVersion(), equalTo(3L));

        try (Engine.GetResult get = engine.get(new Engine.Get(true, doc.type(), doc.id(), create.uid()), searcherFactory)) {
            assertEquals(3, get.version());
        }

    }

    public void testVersioningNewIndex() throws IOException {
        ParsedDocument doc = testParsedDocument("1", null, testDocument(), B_1, null);
        Engine.Index index = indexForDoc(doc);
        Engine.IndexResult indexResult = engine.index(index);
        assertThat(indexResult.getVersion(), equalTo(1L));

        index = new Engine.Index(newUid(doc), doc, indexResult.getSeqNo(), index.primaryTerm(), indexResult.getVersion(), index.versionType().versionTypeForReplicationAndRecovery(), REPLICA, 0, -1, false);
        indexResult = replicaEngine.index(index);
        assertThat(indexResult.getVersion(), equalTo(1L));
    }

    public void testForceMerge() throws IOException {
        try (Store store = createStore();
             Engine engine = new InternalEngine(config(defaultSettings, store, createTempDir(),
                 new LogByteSizeMergePolicy(), null))) { // use log MP here we test some behavior in ESMP
            int numDocs = randomIntBetween(10, 100);
            for (int i = 0; i < numDocs; i++) {
                ParsedDocument doc = testParsedDocument(Integer.toString(i), null, testDocument(), B_1, null);
                Engine.Index index = indexForDoc(doc);
                engine.index(index);
                engine.refresh("test");
            }
            try (Engine.Searcher test = engine.acquireSearcher("test")) {
                assertEquals(numDocs, test.reader().numDocs());
            }
            engine.forceMerge(true, 1, false, false, false);
            engine.refresh("test");
            assertEquals(engine.segments(true).size(), 1);

            ParsedDocument doc = testParsedDocument(Integer.toString(0), null, testDocument(), B_1, null);
            Engine.Index index = indexForDoc(doc);
            engine.delete(new Engine.Delete(index.type(), index.id(), index.uid()));
            engine.forceMerge(true, 10, true, false, false); //expunge deletes
            engine.refresh("test");

            assertEquals(engine.segments(true).size(), 1);
            try (Engine.Searcher test = engine.acquireSearcher("test")) {
                assertEquals(numDocs - 1, test.reader().numDocs());
                assertEquals(engine.config().getMergePolicy().toString(), numDocs - 1, test.reader().maxDoc());
            }

            doc = testParsedDocument(Integer.toString(1), null, testDocument(), B_1, null);
            index = indexForDoc(doc);
            engine.delete(new Engine.Delete(index.type(), index.id(), index.uid()));
            engine.forceMerge(true, 10, false, false, false); //expunge deletes
            engine.refresh("test");
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
                                    ParsedDocument doc = testParsedDocument(Integer.toString(i), null, testDocument(), B_1, null);
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
        ParsedDocument doc = testParsedDocument("1", null, testDocument(), B_1, null);
        Engine.Index create = new Engine.Index(newUid(doc), doc, SequenceNumbers.UNASSIGNED_SEQ_NO, 0, Versions.MATCH_DELETED, VersionType.INTERNAL, PRIMARY, 0, -1, false);
        Engine.IndexResult indexResult = engine.index(create);
        assertThat(indexResult.getVersion(), equalTo(1L));

        create = new Engine.Index(newUid(doc), doc, SequenceNumbers.UNASSIGNED_SEQ_NO, 0, Versions.MATCH_DELETED, VersionType.INTERNAL, PRIMARY, 0, -1, false);
        indexResult = engine.index(create);
        assertTrue(indexResult.hasFailure());
        assertThat(indexResult.getFailure(), instanceOf(VersionConflictEngineException.class));
    }

    protected List<Engine.Operation> generateSingleDocHistory(boolean forReplica, VersionType versionType,
                                                              boolean partialOldPrimary, long primaryTerm,
                                                              int minOpCount, int maxOpCount) {
        final int numOfOps = randomIntBetween(minOpCount, maxOpCount);
        final List<Engine.Operation> ops = new ArrayList<>();
        final Term id = newUid("1");
        final int startWithSeqNo;
        if (partialOldPrimary) {
            startWithSeqNo = randomBoolean() ? numOfOps - 1 : randomIntBetween(0, numOfOps - 1);
        } else {
            startWithSeqNo = 0;
        }
        final String valuePrefix = forReplica ? "r_" : "p_";
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
                case FORCE:
                    version = randomNonNegativeLong();
                    break;
                default:
                    throw new UnsupportedOperationException("unknown version type: " + versionType);
            }
            if (randomBoolean()) {
                op = new Engine.Index(id, testParsedDocument("1", null, testDocumentWithTextField(valuePrefix + i), B_1, null),
                    forReplica && i >= startWithSeqNo ? i * 2 : SequenceNumbers.UNASSIGNED_SEQ_NO,
                    forReplica && i >= startWithSeqNo && incrementTermWhenIntroducingSeqNo ? primaryTerm + 1 : primaryTerm,
                    version,
                    forReplica ? versionType.versionTypeForReplicationAndRecovery() : versionType,
                    forReplica ? REPLICA : PRIMARY,
                    System.currentTimeMillis(), -1, false
                );
            } else {
                op = new Engine.Delete("test", "1", id,
                    forReplica && i >= startWithSeqNo ? i * 2 : SequenceNumbers.UNASSIGNED_SEQ_NO,
                    forReplica && i >= startWithSeqNo && incrementTermWhenIntroducingSeqNo ? primaryTerm + 1 : primaryTerm,
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
            randomFrom(VersionType.INTERNAL, VersionType.EXTERNAL, VersionType.EXTERNAL_GTE, VersionType.FORCE), false, 2, 2, 20);
        assertOpsOnReplica(ops, replicaEngine, true);
    }

    public void testOutOfOrderDocsOnReplicaOldPrimary() throws IOException {
        IndexSettings oldSettings = IndexSettingsModule.newIndexSettings("testOld", Settings.builder()
            .put(IndexSettings.INDEX_GC_DELETES_SETTING.getKey(), "1h") // make sure this doesn't kick in on us
            .put(EngineConfig.INDEX_CODEC_SETTING.getKey(), codecName)
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_5_4_0)
            .put(IndexSettings.INDEX_MAPPING_SINGLE_TYPE_SETTING_KEY, true)
            .put(IndexSettings.MAX_REFRESH_LISTENERS_PER_SHARD.getKey(),
                between(10, 10 * IndexSettings.MAX_REFRESH_LISTENERS_PER_SHARD.get(Settings.EMPTY)))
            .build());

        try (Store oldReplicaStore = createStore();
             InternalEngine replicaEngine =
                 createEngine(oldSettings, oldReplicaStore, createTempDir("translog-old-replica"), newMergePolicy())) {
            final List<Engine.Operation> ops = generateSingleDocHistory(true, randomFrom(VersionType.INTERNAL, VersionType.EXTERNAL), true, 2, 2, 20);
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
            }
            if (randomBoolean()) {
                engine.flush();
                engine.refresh("test");
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
        final List<Engine.Operation> ops = generateSingleDocHistory(true, randomFrom(VersionType.INTERNAL, VersionType.EXTERNAL), false, 2, 100, 300);
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
                            engine.index((Engine.Index) op);
                        } else {
                            engine.delete((Engine.Delete) op);
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
        final List<Engine.Operation> ops = generateSingleDocHistory(false, VersionType.INTERNAL, false, 2, 2, 20);
        assertOpsOnPrimary(ops, Versions.NOT_FOUND, true, engine);
    }

    private int assertOpsOnPrimary(List<Engine.Operation> ops, long currentOpVersion, boolean docDeleted, InternalEngine engine)
        throws IOException {
        String lastFieldValue = null;
        int opsPerformed = 0;
        long lastOpVersion = currentOpVersion;
        BiFunction<Long, Engine.Index, Engine.Index> indexWithVersion = (version, index) -> new Engine.Index(index.uid(), index.parsedDoc(),
            index.seqNo(), index.primaryTerm(), version, index.versionType(), index.origin(), index.startTime(),
            index.getAutoGeneratedIdTimestamp(), index.isRetry());
        BiFunction<Long, Engine.Delete, Engine.Delete> delWithVersion = (version, delete) -> new Engine.Delete(delete.type(), delete.id(),
            delete.uid(), delete.seqNo(), delete.primaryTerm(), version, delete.versionType(), delete.origin(), delete.startTime());
        for (Engine.Operation op : ops) {
            final boolean versionConflict = rarely();
            final boolean versionedOp = versionConflict || randomBoolean();
            final long conflictingVersion = docDeleted || randomBoolean() ?
                lastOpVersion + (randomBoolean() ? 1 : -1) :
                Versions.MATCH_DELETED;
            final long correctVersion = docDeleted && randomBoolean() ? Versions.MATCH_DELETED : lastOpVersion;
            logger.info("performing [{}]{}{}",
                op.operationType().name().charAt(0),
                versionConflict ? " (conflict " + conflictingVersion + ")" : "",
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
                engine.refresh("test");
            }

            if (rarely()) {
                // simulate GC deletes
                engine.refresh("gc_simulation", Engine.SearcherScope.INTERNAL);
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
        final List<Engine.Operation> ops = generateSingleDocHistory(false, versionType, false, 2, 2, 20);
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
            logger.info("performing [{}], v [{}], seq# [{}], term [{}]",
                op.operationType().name().charAt(0), op.version(), op.seqNo(), op.primaryTerm());
            if (op instanceof Engine.Index) {
                final Engine.Index index = (Engine.Index) op;
                Engine.IndexResult result = engine.index(index);
                if (op.versionType().isVersionConflictForWrites(highestOpVersion, op.version(), docDeleted) == false) {
                    seqNo++;
                    assertThat(result.getSeqNo(), equalTo(seqNo));
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
                    assertThat(result.getSeqNo(), equalTo(seqNo));
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
                engine.refresh("test");
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
        final List<Engine.Operation> replicaOps = generateSingleDocHistory(true, VersionType.INTERNAL, false, 1, 2, 20);
        List<Engine.Operation> primaryOps = generateSingleDocHistory(false, VersionType.INTERNAL, false, 2, 2, 20);
        Engine.Operation lastReplicaOp = replicaOps.get(replicaOps.size() - 1);
        final boolean deletedOnReplica = lastReplicaOp instanceof Engine.Delete;
        final long finalReplicaVersion = lastReplicaOp.version();
        final long finalReplicaSeqNo = lastReplicaOp.seqNo();
        assertOpsOnReplica(replicaOps, replicaEngine, true);
        final int opsOnPrimary = assertOpsOnPrimary(primaryOps, finalReplicaVersion, deletedOnReplica, replicaEngine);
        final long currentSeqNo = getSequenceID(replicaEngine,
            new Engine.Get(false, "type", lastReplicaOp.uid().text(), lastReplicaOp.uid())).v1();
        try (Searcher searcher = engine.acquireSearcher("test")) {
            final TotalHitCountCollector collector = new TotalHitCountCollector();
            searcher.searcher().search(new MatchAllDocsQuery(), collector);
            if (collector.getTotalHits() > 0) {
                // last op wasn't delete
                assertThat(currentSeqNo, equalTo(finalReplicaSeqNo + opsOnPrimary));
            }
        }
    }

    public void testConcurrentExternalVersioningOnPrimary() throws IOException, InterruptedException {
        final List<Engine.Operation> ops = generateSingleDocHistory(false, VersionType.EXTERNAL, false, 2, 100, 300);
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
        ParsedDocument doc = testParsedDocument("1", null, testDocument(), bytesArray(""), null);
        final Term uidTerm = newUid(doc);
        engine.index(indexForDoc(doc));
        final BiFunction<String, Engine.SearcherScope, Searcher> searcherFactory = engine::acquireSearcher;
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
                            testParsedDocument("1", null, testDocument(),
                                bytesArray(Strings.collectionToCommaDelimitedString(values)), null),
                            SequenceNumbers.UNASSIGNED_SEQ_NO, 2,
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
        ParsedDocument doc = testParsedDocument("1", null, testDocument(), B_1, null);
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

    private static class MockAppender extends AbstractAppender {
        public boolean sawIndexWriterMessage;

        public boolean sawIndexWriterIFDMessage;

        MockAppender(final String name) throws IllegalAccessException {
            super(name, RegexFilter.createFilter(".*(\n.*)*", new String[0], false, null, null), null);
        }

        @Override
        public void append(LogEvent event) {
            final String formattedMessage = event.getMessage().getFormattedMessage();
            if (event.getLevel() == Level.TRACE && event.getMarker().getName().contains("[index][0] ")) {
                if (event.getLoggerName().endsWith(".IW") &&
                    formattedMessage.contains("IW: now apply all deletes")) {
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
            ParsedDocument doc = testParsedDocument("1", null, testDocumentWithTextField(), B_1, null);
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

    public void testSeqNoAndCheckpoints() throws IOException {
        final int opCount = randomIntBetween(1, 256);
        long primarySeqNo = SequenceNumbers.NO_OPS_PERFORMED;
        final String[] ids = new String[]{"1", "2", "3"};
        final Set<String> indexedIds = new HashSet<>();
        long localCheckpoint = SequenceNumbers.NO_OPS_PERFORMED;
        long replicaLocalCheckpoint = SequenceNumbers.NO_OPS_PERFORMED;
        final long globalCheckpoint;
        long maxSeqNo = SequenceNumbers.NO_OPS_PERFORMED;
        InternalEngine initialEngine = null;

        try {
            initialEngine = engine;
            final ShardRouting primary = TestShardRouting.newShardRouting("test", shardId.id(), "node1", null, true, ShardRoutingState.STARTED, allocationId);
            final ShardRouting replica = TestShardRouting.newShardRouting(shardId, "node2", false, ShardRoutingState.STARTED);
            initialEngine.seqNoService().updateAllocationIdsFromMaster(1L, new HashSet<>(Arrays.asList(primary.allocationId().getId(),
                replica.allocationId().getId())),
                new IndexShardRoutingTable.Builder(shardId).addShard(primary).addShard(replica).build(), Collections.emptySet());
            initialEngine.seqNoService().activatePrimaryMode(primarySeqNo);
            for (int op = 0; op < opCount; op++) {
                final String id;
                // mostly index, sometimes delete
                if (rarely() && indexedIds.isEmpty() == false) {
                    // we have some docs indexed, so delete one of them
                    id = randomFrom(indexedIds);
                    final Engine.Delete delete = new Engine.Delete(
                        "test", id, newUid(id), SequenceNumbers.UNASSIGNED_SEQ_NO, 0,
                        rarely() ? 100 : Versions.MATCH_ANY, VersionType.INTERNAL, PRIMARY, 0);
                    final Engine.DeleteResult result = initialEngine.delete(delete);
                    if (!result.hasFailure()) {
                        assertThat(result.getSeqNo(), equalTo(primarySeqNo + 1));
                        assertThat(initialEngine.seqNoService().getMaxSeqNo(), equalTo(primarySeqNo + 1));
                        indexedIds.remove(id);
                        primarySeqNo++;
                    } else {
                        assertThat(result.getSeqNo(), equalTo(SequenceNumbers.UNASSIGNED_SEQ_NO));
                        assertThat(initialEngine.seqNoService().getMaxSeqNo(), equalTo(primarySeqNo));
                    }
                } else {
                    // index a document
                    id = randomFrom(ids);
                    ParsedDocument doc = testParsedDocument(id, null, testDocumentWithTextField(), SOURCE, null);
                    final Engine.Index index = new Engine.Index(newUid(doc), doc,
                        SequenceNumbers.UNASSIGNED_SEQ_NO, 0,
                        rarely() ? 100 : Versions.MATCH_ANY, VersionType.INTERNAL,
                        PRIMARY, 0, -1, false);
                    final Engine.IndexResult result = initialEngine.index(index);
                    if (!result.hasFailure()) {
                        assertThat(result.getSeqNo(), equalTo(primarySeqNo + 1));
                        assertThat(initialEngine.seqNoService().getMaxSeqNo(), equalTo(primarySeqNo + 1));
                        indexedIds.add(id);
                        primarySeqNo++;
                    } else {
                        assertThat(result.getSeqNo(), equalTo(SequenceNumbers.UNASSIGNED_SEQ_NO));
                        assertThat(initialEngine.seqNoService().getMaxSeqNo(), equalTo(primarySeqNo));
                    }
                }

                if (randomInt(10) < 3) {
                    // only update rarely as we do it every doc
                    replicaLocalCheckpoint = randomIntBetween(Math.toIntExact(replicaLocalCheckpoint), Math.toIntExact(primarySeqNo));
                }
                initialEngine.seqNoService().updateLocalCheckpointForShard(primary.allocationId().getId(),
                    initialEngine.seqNoService().getLocalCheckpoint());
                initialEngine.seqNoService().updateLocalCheckpointForShard(replica.allocationId().getId(), replicaLocalCheckpoint);

                if (rarely()) {
                    localCheckpoint = primarySeqNo;
                    maxSeqNo = primarySeqNo;
                    initialEngine.flush(true, true);
                }
            }

            logger.info("localcheckpoint {}, global {}", replicaLocalCheckpoint, primarySeqNo);
            globalCheckpoint = initialEngine.seqNoService().getGlobalCheckpoint();

            assertEquals(primarySeqNo, initialEngine.seqNoService().getMaxSeqNo());
            assertThat(initialEngine.seqNoService().stats().getMaxSeqNo(), equalTo(primarySeqNo));
            assertThat(initialEngine.seqNoService().stats().getLocalCheckpoint(), equalTo(primarySeqNo));
            assertThat(initialEngine.seqNoService().stats().getGlobalCheckpoint(), equalTo(replicaLocalCheckpoint));

            assertThat(
                Long.parseLong(initialEngine.commitStats().getUserData().get(SequenceNumbers.LOCAL_CHECKPOINT_KEY)),
                equalTo(localCheckpoint));
            initialEngine.getTranslog().sync(); // to guarantee the global checkpoint is written to the translog checkpoint
            assertThat(
                initialEngine.getTranslog().getLastSyncedGlobalCheckpoint(),
                equalTo(globalCheckpoint));
            assertThat(
                Long.parseLong(initialEngine.commitStats().getUserData().get(SequenceNumbers.MAX_SEQ_NO)),
                equalTo(maxSeqNo));

        } finally {
            IOUtils.close(initialEngine);
        }

        InternalEngine recoveringEngine = null;
        try {
            recoveringEngine = new InternalEngine(copy(initialEngine.config(), EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG));
            recoveringEngine.recoverFromTranslog();

            assertEquals(primarySeqNo, recoveringEngine.seqNoService().getMaxSeqNo());
            assertThat(
                Long.parseLong(recoveringEngine.commitStats().getUserData().get(SequenceNumbers.LOCAL_CHECKPOINT_KEY)),
                equalTo(primarySeqNo));
            assertThat(
                recoveringEngine.getTranslog().getLastSyncedGlobalCheckpoint(),
                equalTo(globalCheckpoint));
            assertThat(
                Long.parseLong(recoveringEngine.commitStats().getUserData().get(SequenceNumbers.MAX_SEQ_NO)),
                // after recovering from translog, all docs have been flushed to Lucene segments, so here we will assert
                // that the committed max seq no is equivalent to what the current primary seq no is, as all data
                // we have assigned sequence numbers to should be in the commit
                equalTo(primarySeqNo));
            assertThat(recoveringEngine.seqNoService().stats().getLocalCheckpoint(), equalTo(primarySeqNo));
            assertThat(recoveringEngine.seqNoService().stats().getMaxSeqNo(), equalTo(primarySeqNo));
            assertThat(recoveringEngine.seqNoService().generateSeqNo(), equalTo(primarySeqNo + 1));
        } finally {
            IOUtils.close(recoveringEngine);
        }
    }

    // this test writes documents to the engine while concurrently flushing/commit
    // and ensuring that the commit points contain the correct sequence number data
    public void testConcurrentWritesAndCommits() throws Exception {
        List<Engine.IndexCommitRef> commits = new ArrayList<>();
        try (Store store = createStore();
             InternalEngine engine = new InternalEngine(config(defaultSettings, store, createTempDir(), newMergePolicy(), null))) {

            final int numIndexingThreads = scaledRandomIntBetween(2, 4);
            final int numDocsPerThread = randomIntBetween(500, 1000);
            final CyclicBarrier barrier = new CyclicBarrier(numIndexingThreads + 1);
            final List<Thread> indexingThreads = new ArrayList<>();
            final CountDownLatch doneLatch = new CountDownLatch(numIndexingThreads);
            // create N indexing threads to index documents simultaneously
            for (int threadNum = 0; threadNum < numIndexingThreads; threadNum++) {
                final int threadIdx = threadNum;
                Thread indexingThread = new Thread(() -> {
                    try {
                        barrier.await(); // wait for all threads to start at the same time
                        // index random number of docs
                        for (int i = 0; i < numDocsPerThread; i++) {
                            final String id = "thread" + threadIdx + "#" + i;
                            ParsedDocument doc = testParsedDocument(id, null, testDocument(), B_1, null);
                            engine.index(indexForDoc(doc));
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        doneLatch.countDown();
                    }

                });
                indexingThreads.add(indexingThread);
            }

            // start the indexing threads
            for (Thread thread : indexingThreads) {
                thread.start();
            }
            barrier.await(); // wait for indexing threads to all be ready to start
            int commitLimit = randomIntBetween(10, 20);
            long sleepTime = 1;
            // create random commit points
            boolean doneIndexing;
            do {
                doneIndexing = doneLatch.await(sleepTime, TimeUnit.MILLISECONDS);
                commits.add(engine.acquireIndexCommit(true));
                if (commits.size() > commitLimit) { // don't keep on piling up too many commits
                    IOUtils.close(commits.remove(randomIntBetween(0, commits.size()-1)));
                    // we increase the wait time to make sure we eventually if things are slow wait for threads to finish.
                    // this will reduce pressure on disks and will allow threads to make progress without piling up too many commits
                    sleepTime = sleepTime * 2;
                }
            } while (doneIndexing == false);

            // now, verify all the commits have the correct docs according to the user commit data
            long prevLocalCheckpoint = SequenceNumbers.NO_OPS_PERFORMED;
            long prevMaxSeqNo = SequenceNumbers.NO_OPS_PERFORMED;
            for (Engine.IndexCommitRef commitRef : commits) {
                final IndexCommit commit = commitRef.getIndexCommit();
                Map<String, String> userData = commit.getUserData();
                long localCheckpoint = userData.containsKey(SequenceNumbers.LOCAL_CHECKPOINT_KEY) ?
                    Long.parseLong(userData.get(SequenceNumbers.LOCAL_CHECKPOINT_KEY)) :
                    SequenceNumbers.NO_OPS_PERFORMED;
                long maxSeqNo = userData.containsKey(SequenceNumbers.MAX_SEQ_NO) ?
                    Long.parseLong(userData.get(SequenceNumbers.MAX_SEQ_NO)) :
                    SequenceNumbers.UNASSIGNED_SEQ_NO;
                // local checkpoint and max seq no shouldn't go backwards
                assertThat(localCheckpoint, greaterThanOrEqualTo(prevLocalCheckpoint));
                assertThat(maxSeqNo, greaterThanOrEqualTo(prevMaxSeqNo));
                try (IndexReader reader = DirectoryReader.open(commit)) {
                    Long highest = getHighestSeqNo(reader);
                    final long highestSeqNo;
                    if (highest != null) {
                        highestSeqNo = highest.longValue();
                    } else {
                        highestSeqNo = SequenceNumbers.NO_OPS_PERFORMED;
                    }
                    // make sure localCheckpoint <= highest seq no found <= maxSeqNo
                    assertThat(highestSeqNo, greaterThanOrEqualTo(localCheckpoint));
                    assertThat(highestSeqNo, lessThanOrEqualTo(maxSeqNo));
                    // make sure all sequence numbers up to and including the local checkpoint are in the index
                    FixedBitSet seqNosBitSet = getSeqNosSet(reader, highestSeqNo);
                    for (int i = 0; i <= localCheckpoint; i++) {
                        assertTrue("local checkpoint [" + localCheckpoint + "], _seq_no [" + i + "] should be indexed",
                            seqNosBitSet.get(i));
                    }
                }
                prevLocalCheckpoint = localCheckpoint;
                prevMaxSeqNo = maxSeqNo;
            }
        } finally {
            IOUtils.close(commits);
        }
    }

    private static Long getHighestSeqNo(final IndexReader reader) throws IOException {
        final String fieldName = SeqNoFieldMapper.NAME;
        long size = PointValues.size(reader, fieldName);
        if (size == 0) {
            return null;
        }
        byte[] max = PointValues.getMaxPackedValue(reader, fieldName);
        return LongPoint.decodeDimension(max, 0);
    }

    private static FixedBitSet getSeqNosSet(final IndexReader reader, final long highestSeqNo) throws IOException {
        // _seq_no are stored as doc values for the time being, so this is how we get them
        // (as opposed to using an IndexSearcher or IndexReader)
        final FixedBitSet bitSet = new FixedBitSet((int) highestSeqNo + 1);
        final List<LeafReaderContext> leaves = reader.leaves();
        if (leaves.isEmpty()) {
            return bitSet;
        }

        for (int i = 0; i < leaves.size(); i++) {
            final LeafReader leaf = leaves.get(i).reader();
            final NumericDocValues values = leaf.getNumericDocValues(SeqNoFieldMapper.NAME);
            if (values == null) {
                continue;
            }
            final Bits bits = leaf.getLiveDocs();
            for (int docID = 0; docID < leaf.maxDoc(); docID++) {
                if (bits == null || bits.get(docID)) {
                    if (values.advanceExact(docID) == false) {
                        throw new AssertionError("Document does not have a seq number: " + docID);
                    }
                    final long seqNo = values.longValue();
                    assertFalse("should not have more than one document with the same seq_no[" + seqNo + "]", bitSet.get((int) seqNo));
                    bitSet.set((int) seqNo);
                }
            }
        }
        return bitSet;
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
            ParsedDocument doc = testParsedDocument("1", null, testDocumentWithTextField(), B_1, null);
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
             Engine engine = new InternalEngine(config(defaultSettings, store, createTempDir(), newMergePolicy(), null))) {
            engine.config().setEnableGcDeletes(false);

            final BiFunction<String, Engine.SearcherScope, Searcher> searcherFactory = engine::acquireSearcher;

            // Add document
            Document document = testDocument();
            document.add(new TextField("value", "test1", Field.Store.YES));

            ParsedDocument doc = testParsedDocument("1", null, document, B_2, null);
            engine.index(new Engine.Index(newUid(doc), doc, SequenceNumbers.UNASSIGNED_SEQ_NO, 0, 1, VersionType.EXTERNAL, Engine.Operation.Origin.PRIMARY, System.nanoTime(), -1, false));

            // Delete document we just added:
            engine.delete(new Engine.Delete("test", "1", newUid(doc), SequenceNumbers.UNASSIGNED_SEQ_NO, 0, 10, VersionType.EXTERNAL, Engine.Operation.Origin.PRIMARY, System.nanoTime()));

            // Get should not find the document
            Engine.GetResult getResult = engine.get(newGet(true, doc), searcherFactory);
            assertThat(getResult.exists(), equalTo(false));

            // Give the gc pruning logic a chance to kick in
            Thread.sleep(1000);

            if (randomBoolean()) {
                engine.refresh("test");
            }

            // Delete non-existent document
            engine.delete(new Engine.Delete("test", "2", newUid("2"), SequenceNumbers.UNASSIGNED_SEQ_NO, 0, 10, VersionType.EXTERNAL, Engine.Operation.Origin.PRIMARY, System.nanoTime()));

            // Get should not find the document (we never indexed uid=2):
            getResult = engine.get(new Engine.Get(true, "type", "2", newUid("2")), searcherFactory);
            assertThat(getResult.exists(), equalTo(false));

            // Try to index uid=1 with a too-old version, should fail:
            Engine.Index index = new Engine.Index(newUid(doc), doc, SequenceNumbers.UNASSIGNED_SEQ_NO, 0, 2, VersionType.EXTERNAL, Engine.Operation.Origin.PRIMARY, System.nanoTime(), -1, false);
            Engine.IndexResult indexResult = engine.index(index);
            assertTrue(indexResult.hasFailure());
            assertThat(indexResult.getFailure(), instanceOf(VersionConflictEngineException.class));

            // Get should still not find the document
            getResult = engine.get(newGet(true, doc), searcherFactory);
            assertThat(getResult.exists(), equalTo(false));

            // Try to index uid=2 with a too-old version, should fail:
            Engine.Index index1 = new Engine.Index(newUid(doc), doc, SequenceNumbers.UNASSIGNED_SEQ_NO, 0, 2, VersionType.EXTERNAL, Engine.Operation.Origin.PRIMARY, System.nanoTime(), -1, false);
            indexResult = engine.index(index1);
            assertTrue(indexResult.hasFailure());
            assertThat(indexResult.getFailure(), instanceOf(VersionConflictEngineException.class));

            // Get should not find the document
            getResult = engine.get(newGet(true, doc), searcherFactory);
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
        EngineConfig config = copy(config(defaultSettings, store, primaryTranslogDir, newMergePolicy(), null),
            EngineConfig.OpenMode.OPEN_INDEX_CREATE_TRANSLOG);
        engine = new InternalEngine(config);
    }

    public void testTranslogReplayWithFailure() throws IOException {
        final int numDocs = randomIntBetween(1, 10);
        for (int i = 0; i < numDocs; i++) {
            ParsedDocument doc = testParsedDocument(Integer.toString(i), null, testDocument(), new BytesArray("{}"), null);
            Engine.Index firstIndexRequest = new Engine.Index(newUid(doc), doc, SequenceNumbers.UNASSIGNED_SEQ_NO, 0, Versions.MATCH_DELETED, VersionType.INTERNAL, PRIMARY, System.nanoTime(), -1, false);
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

    public void testTranslogCleanUpPostCommitCrash() throws Exception {
        IndexSettings indexSettings = new IndexSettings(defaultSettings.getIndexMetaData(), defaultSettings.getNodeSettings(),
            defaultSettings.getScopedSettings());
        IndexMetaData.Builder builder = IndexMetaData.builder(indexSettings.getIndexMetaData());
        builder.settings(Settings.builder().put(indexSettings.getSettings())
            .put(IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.getKey(), "-1")
            .put(IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.getKey(), "-1")
        );
        indexSettings.updateIndexMetaData(builder.build());

        try (Store store = createStore()) {
            AtomicBoolean throwErrorOnCommit = new AtomicBoolean();
            final Path translogPath = createTempDir();
            try (InternalEngine engine = new InternalEngine(config(indexSettings, store, translogPath, newMergePolicy(), null, null)) {
                @Override
                protected void commitIndexWriter(IndexWriter writer, Translog translog, String syncId) throws IOException {
                    super.commitIndexWriter(writer, translog, syncId);
                    if (throwErrorOnCommit.get()) {
                        throw new RuntimeException("power's out");
                    }
                }
            }) {
                final ParsedDocument doc1 = testParsedDocument("1", null, testDocumentWithTextField(), SOURCE, null);
                engine.index(indexForDoc(doc1));
                throwErrorOnCommit.set(true);
                FlushFailedEngineException e = expectThrows(FlushFailedEngineException.class, engine::flush);
                assertThat(e.getCause().getMessage(), equalTo("power's out"));
            }
            try (InternalEngine engine = new InternalEngine(config(indexSettings, store, translogPath, newMergePolicy(), null, null))) {
                engine.recoverFromTranslog();
                assertVisibleCount(engine, 1);
                final long committedGen = Long.valueOf(
                    engine.getLastCommittedSegmentInfos().getUserData().get(Translog.TRANSLOG_GENERATION_KEY));
                for (int gen = 1; gen < committedGen; gen++) {
                    final Path genFile = translogPath.resolve(Translog.getFilename(gen));
                    assertFalse(genFile + " wasn't cleaned up", Files.exists(genFile));
                }
            }
        }
    }

    public void testSkipTranslogReplay() throws IOException {
        final int numDocs = randomIntBetween(1, 10);
        for (int i = 0; i < numDocs; i++) {
            ParsedDocument doc = testParsedDocument(Integer.toString(i), null, testDocument(), new BytesArray("{}"), null);
            Engine.Index firstIndexRequest = new Engine.Index(newUid(doc), doc, SequenceNumbers.UNASSIGNED_SEQ_NO, 0, Versions.MATCH_DELETED, VersionType.INTERNAL, PRIMARY, System.nanoTime(), -1, false);
            Engine.IndexResult indexResult = engine.index(firstIndexRequest);
            assertThat(indexResult.getVersion(), equalTo(1L));
        }
        assertVisibleCount(engine, numDocs);
        engine.close();
        engine = new InternalEngine(copy(engine.config(), EngineConfig.OpenMode.OPEN_INDEX_CREATE_TRANSLOG));
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), randomIntBetween(numDocs, numDocs + 10));
            assertThat(topDocs.totalHits, equalTo(0L));
        }
    }

    private Mapping dynamicUpdate() {
        BuilderContext context = new BuilderContext(
            Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build(), new ContentPath());
        final RootObjectMapper root = new RootObjectMapper.Builder("some_type").build(context);
        return new Mapping(Version.CURRENT, root, new MetadataFieldMapper[0], emptyMap());
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
            ParsedDocument doc = testParsedDocument(Integer.toString(i), null, testDocument(), new BytesArray("{}"), null);
            Engine.Index firstIndexRequest = new Engine.Index(newUid(doc), doc, SequenceNumbers.UNASSIGNED_SEQ_NO, 0, Versions.MATCH_DELETED, VersionType.INTERNAL, PRIMARY, System.nanoTime(), -1, false);
            Engine.IndexResult indexResult = engine.index(firstIndexRequest);
            assertThat(indexResult.getVersion(), equalTo(1L));
        }
        assertVisibleCount(engine, numDocs);

        TranslogHandler parser = (TranslogHandler) engine.config().getTranslogRecoveryRunner();
        parser.mappingUpdate = dynamicUpdate();

        engine.close();
        engine = new InternalEngine(copy(engine.config(), EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG)); // we need to reuse the engine config unless the parser.mappingModified won't work
        engine.recoverFromTranslog();

        assertVisibleCount(engine, numDocs, false);
        parser = (TranslogHandler) engine.config().getTranslogRecoveryRunner();
        assertEquals(numDocs, parser.appliedOperations());
        if (parser.mappingUpdate != null) {
            assertEquals(1, parser.getRecoveredTypes().size());
            assertTrue(parser.getRecoveredTypes().containsKey("test"));
        } else {
            assertEquals(0, parser.getRecoveredTypes().size());
        }

        engine.close();
        engine = createEngine(store, primaryTranslogDir);
        assertVisibleCount(engine, numDocs, false);
        parser = (TranslogHandler) engine.config().getTranslogRecoveryRunner();
        assertEquals(0, parser.appliedOperations());

        final boolean flush = randomBoolean();
        int randomId = randomIntBetween(numDocs + 1, numDocs + 10);
        ParsedDocument doc = testParsedDocument(Integer.toString(randomId), null, testDocument(), new BytesArray("{}"), null);
        Engine.Index firstIndexRequest = new Engine.Index(newUid(doc), doc, SequenceNumbers.UNASSIGNED_SEQ_NO, 0, 1, VersionType.EXTERNAL, PRIMARY, System.nanoTime(), -1, false);
        Engine.IndexResult indexResult = engine.index(firstIndexRequest);
        assertThat(indexResult.getVersion(), equalTo(1L));
        if (flush) {
            engine.flush();
            engine.refresh("test");
        }

        doc = testParsedDocument(Integer.toString(randomId), null, testDocument(), new BytesArray("{}"), null);
        Engine.Index idxRequest = new Engine.Index(newUid(doc), doc, SequenceNumbers.UNASSIGNED_SEQ_NO, 0, 2, VersionType.EXTERNAL, PRIMARY, System.nanoTime(), -1, false);
        Engine.IndexResult result = engine.index(idxRequest);
        engine.refresh("test");
        assertThat(result.getVersion(), equalTo(2L));
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), numDocs + 1);
            assertThat(topDocs.totalHits, equalTo(numDocs + 1L));
        }

        engine.close();
        engine = createEngine(store, primaryTranslogDir);
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), numDocs + 1);
            assertThat(topDocs.totalHits, equalTo(numDocs + 1L));
        }
        parser = (TranslogHandler) engine.config().getTranslogRecoveryRunner();
        assertEquals(flush ? 1 : 2, parser.appliedOperations());
        engine.delete(new Engine.Delete("test", Integer.toString(randomId), newUid(doc)));
        if (randomBoolean()) {
            engine.refresh("test");
        } else {
            engine.close();
            engine = createEngine(store, primaryTranslogDir);
        }
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), numDocs);
            assertThat(topDocs.totalHits, equalTo((long) numDocs));
        }
    }

    public void testRecoverFromForeignTranslog() throws IOException {
        final int numDocs = randomIntBetween(1, 10);
        for (int i = 0; i < numDocs; i++) {
            ParsedDocument doc = testParsedDocument(Integer.toString(i), null, testDocument(), new BytesArray("{}"), null);
            Engine.Index firstIndexRequest = new Engine.Index(newUid(doc), doc, SequenceNumbers.UNASSIGNED_SEQ_NO, 0, Versions.MATCH_DELETED, VersionType.INTERNAL, PRIMARY, System.nanoTime(), -1, false);
            Engine.IndexResult index = engine.index(firstIndexRequest);
            assertThat(index.getVersion(), equalTo(1L));
        }
        assertVisibleCount(engine, numDocs);
        Translog.TranslogGeneration generation = engine.getTranslog().getGeneration();
        engine.close();

        Translog translog = new Translog(
            new TranslogConfig(shardId, createTempDir(), INDEX_SETTINGS, BigArrays.NON_RECYCLING_INSTANCE),
            null, createTranslogDeletionPolicy(INDEX_SETTINGS), () -> SequenceNumbers.UNASSIGNED_SEQ_NO);
        translog.add(new Translog.Index("test", "SomeBogusId", 0, "{}".getBytes(Charset.forName("UTF-8"))));
        assertEquals(generation.translogFileGeneration, translog.currentFileGeneration());
        translog.close();

        EngineConfig config = engine.config();
        /* create a TranslogConfig that has been created with a different UUID */
        TranslogConfig translogConfig = new TranslogConfig(shardId, translog.location(), config.getIndexSettings(),
            BigArrays.NON_RECYCLING_INSTANCE);

        EngineConfig brokenConfig = new EngineConfig(EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG, shardId, allocationId.getId(),
                threadPool, config.getIndexSettings(), null, store, newMergePolicy(), config.getAnalyzer(), config.getSimilarity(),
                new CodecService(null, logger), config.getEventListener(), IndexSearcher.getDefaultQueryCache(),
                IndexSearcher.getDefaultQueryCachingPolicy(), false, translogConfig, TimeValue.timeValueMinutes(5),
                config.getExternalRefreshListener(), config.getInternalRefreshListener(), null, config.getTranslogRecoveryRunner());
        try {
            InternalEngine internalEngine = new InternalEngine(brokenConfig);
            fail("translog belongs to a different engine");
        } catch (EngineCreationFailureException ex) {
        }

        engine = createEngine(store, primaryTranslogDir); // and recover again!
        assertVisibleCount(engine, numDocs, false);
    }

    public void testHistoryUUIDIsSetIfMissing() throws IOException {
        final int numDocs = randomIntBetween(0, 3);
        for (int i = 0; i < numDocs; i++) {
            ParsedDocument doc = testParsedDocument(Integer.toString(i), null, testDocument(), new BytesArray("{}"), null);
            Engine.Index firstIndexRequest = new Engine.Index(newUid(doc), doc, SequenceNumbers.UNASSIGNED_SEQ_NO, 0, Versions.MATCH_DELETED, VersionType.INTERNAL, PRIMARY, System.nanoTime(), -1, false);
            Engine.IndexResult index = engine.index(firstIndexRequest);
            assertThat(index.getVersion(), equalTo(1L));
        }
        assertVisibleCount(engine, numDocs);
        engine.close();

        IndexWriterConfig iwc = new IndexWriterConfig(null)
            .setCommitOnClose(false)
            // we don't want merges to happen here - we call maybe merge on the engine
            // later once we stared it up otherwise we would need to wait for it here
            // we also don't specify a codec here and merges should use the engines for this index
            .setMergePolicy(NoMergePolicy.INSTANCE)
            .setOpenMode(IndexWriterConfig.OpenMode.APPEND);
        try (IndexWriter writer = new IndexWriter(store.directory(), iwc)) {
            Map<String, String> newCommitData = new HashMap<>();
            for (Map.Entry<String, String> entry: writer.getLiveCommitData()) {
                if (entry.getKey().equals(Engine.HISTORY_UUID_KEY) == false)  {
                    newCommitData.put(entry.getKey(), entry.getValue());
                }
            }
            writer.setLiveCommitData(newCommitData.entrySet());
            writer.commit();
        }

        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", Settings.builder()
            .put(defaultSettings.getSettings())
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_6_0_0_beta1)
            .build());

        EngineConfig config = engine.config();

        EngineConfig newConfig = new EngineConfig(
            randomBoolean() ? EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG : EngineConfig.OpenMode.OPEN_INDEX_CREATE_TRANSLOG,
            shardId, allocationId.getId(),
            threadPool, indexSettings, null, store, newMergePolicy(), config.getAnalyzer(), config.getSimilarity(),
            new CodecService(null, logger), config.getEventListener(), IndexSearcher.getDefaultQueryCache(),
            IndexSearcher.getDefaultQueryCachingPolicy(), false, config.getTranslogConfig(), TimeValue.timeValueMinutes(5),
            config.getExternalRefreshListener(), config.getInternalRefreshListener(), null, config.getTranslogRecoveryRunner());
        engine = new InternalEngine(newConfig);
        if (newConfig.getOpenMode() == EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG) {
            engine.recoverFromTranslog();
            assertVisibleCount(engine, numDocs, false);
        } else {
            assertVisibleCount(engine, 0, false);
        }
        assertThat(engine.getHistoryUUID(), notNullValue());
    }

    public void testHistoryUUIDCanBeForced() throws IOException {
        final int numDocs = randomIntBetween(0, 3);
        for (int i = 0; i < numDocs; i++) {
            ParsedDocument doc = testParsedDocument(Integer.toString(i), null, testDocument(), new BytesArray("{}"), null);
            Engine.Index firstIndexRequest = new Engine.Index(newUid(doc), doc, SequenceNumbers.UNASSIGNED_SEQ_NO, 0, Versions.MATCH_DELETED, VersionType.INTERNAL, PRIMARY, System.nanoTime(), -1, false);
            Engine.IndexResult index = engine.index(firstIndexRequest);
            assertThat(index.getVersion(), equalTo(1L));
        }
        assertVisibleCount(engine, numDocs);
        final String oldHistoryUUID = engine.getHistoryUUID();
        engine.close();
        EngineConfig config = engine.config();

        EngineConfig newConfig = new EngineConfig(
            randomBoolean() ? EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG : EngineConfig.OpenMode.OPEN_INDEX_CREATE_TRANSLOG,
            shardId, allocationId.getId(),
            threadPool, config.getIndexSettings(), null, store, newMergePolicy(), config.getAnalyzer(), config.getSimilarity(),
            new CodecService(null, logger), config.getEventListener(), IndexSearcher.getDefaultQueryCache(),
            IndexSearcher.getDefaultQueryCachingPolicy(), true, config.getTranslogConfig(), TimeValue.timeValueMinutes(5),
            config.getExternalRefreshListener(), config.getInternalRefreshListener(), null, config.getTranslogRecoveryRunner());
        engine = new InternalEngine(newConfig);
        if (newConfig.getOpenMode() == EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG) {
            engine.recoverFromTranslog();
            assertVisibleCount(engine, numDocs, false);
        } else {
            assertVisibleCount(engine, 0, false);
        }
        assertThat(engine.getHistoryUUID(), not(equalTo(oldHistoryUUID)));
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
     * Tests that when the close method returns the engine is actually guaranteed to have cleaned up and that resources are closed
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
            EngineConfig config = config(defaultSettings, store, createTempDir(), newMergePolicy(), null);

            // create
            {
                ParsedDocument doc = testParsedDocument(Integer.toString(0), null, testDocument(), new BytesArray("{}"), null);
                Engine.Index firstIndexRequest = new Engine.Index(newUid(doc), doc, SequenceNumbers.UNASSIGNED_SEQ_NO, 0, Versions.MATCH_DELETED, VersionType.INTERNAL, PRIMARY, System.nanoTime(), -1, false);

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
                    assertEquals(1, engine.getTranslog().currentFileGeneration());
                    assertEquals(0L, engine.getTranslog().uncommittedOperations());
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
            final ParsedDocument doc1 = testParsedDocument("1", null, testDocumentWithTextField(), B_1, null);
            final ParsedDocument doc2 = testParsedDocument("2", null, testDocumentWithTextField(), B_1, null);
            final ParsedDocument doc3 = testParsedDocument("3", null, testDocumentWithTextField(), B_1, null);

            AtomicReference<ThrowingIndexWriter> throwingIndexWriter = new AtomicReference<>();
            try (Engine engine = createEngine(defaultSettings, store, createTempDir(), NoMergePolicy.INSTANCE,
                (directory, iwc) -> {
                  throwingIndexWriter.set(new ThrowingIndexWriter(directory, iwc));
                  return throwingIndexWriter.get();
                })
            ) {
                // test document failure while indexing
                if (randomBoolean()) {
                    throwingIndexWriter.get().setThrowFailure(() -> new IOException("simulated"));
                } else {
                    throwingIndexWriter.get().setThrowFailure(() -> new IllegalArgumentException("simulated max token length"));
                }
                // test index with document failure
                Engine.IndexResult indexResult = engine.index(indexForDoc(doc1));
                assertNotNull(indexResult.getFailure());
                assertThat(indexResult.getSeqNo(), equalTo(0L));
                assertThat(indexResult.getVersion(), equalTo(Versions.MATCH_ANY));
                assertNotNull(indexResult.getTranslogLocation());

                throwingIndexWriter.get().clearFailure();
                indexResult = engine.index(indexForDoc(doc1));
                assertThat(indexResult.getSeqNo(), equalTo(1L));
                assertThat(indexResult.getVersion(), equalTo(1L));
                assertNull(indexResult.getFailure());
                assertNotNull(indexResult.getTranslogLocation());
                engine.index(indexForDoc(doc2));

                // test failure while deleting
                // all these simulated exceptions are not fatal to the IW so we treat them as document failures
                final Engine.DeleteResult deleteResult;
                if (randomBoolean()) {
                    throwingIndexWriter.get().setThrowFailure(() -> new IOException("simulated"));
                    deleteResult = engine.delete(new Engine.Delete("test", "1", newUid(doc1)));
                    assertThat(deleteResult.getFailure(), instanceOf(IOException.class));
                } else {
                    throwingIndexWriter.get().setThrowFailure(() -> new IllegalArgumentException("simulated max token length"));
                    deleteResult = engine.delete(new Engine.Delete("test", "1", newUid(doc1)));
                    assertThat(deleteResult.getFailure(),
                        instanceOf(IllegalArgumentException.class));
                }
                assertThat(deleteResult.getVersion(), equalTo(2L));
                assertThat(deleteResult.getSeqNo(), equalTo(3L));

                // test non document level failure is thrown
                if (randomBoolean()) {
                    // simulate close by corruption
                    throwingIndexWriter.get().setThrowFailure(null);
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
        final ParsedDocument doc = testParsedDocument("1", null, testDocumentWithTextField(), new BytesArray("{}".getBytes(Charset.defaultCharset())), null);
        Engine.Index operation = appendOnlyPrimary(doc, false, 1);
        Engine.Index retry = appendOnlyPrimary(doc, true, 1);
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
        operation = appendOnlyPrimary(doc, false, 1);
        retry = appendOnlyPrimary(doc, true, 1);
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
            assertNotNull(retryResult.getTranslogLocation());
            assertTrue(retryResult.getTranslogLocation().compareTo(indexResult.getTranslogLocation()) < 0);
        }

        engine.refresh("test");
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), 10);
            assertEquals(1, topDocs.totalHits);
        }
    }

    public void testDoubleDeliveryReplicaAppendingOnly() throws IOException {
        final ParsedDocument doc = testParsedDocument("1", null, testDocumentWithTextField(),
            new BytesArray("{}".getBytes(Charset.defaultCharset())), null);
        Engine.Index operation = appendOnlyReplica(doc, false, 1, randomIntBetween(0, 5));
        Engine.Index retry = appendOnlyReplica(doc, true, 1, randomIntBetween(0, 5));
        // operations with a seq# equal or lower to the local checkpoint are not indexed to lucene
        // and the version lookup is skipped
        final boolean belowLckp = operation.seqNo() == 0 && retry.seqNo() == 0;
        if (randomBoolean()) {
            Engine.IndexResult indexResult = engine.index(operation);
            assertFalse(engine.indexWriterHasDeletions());
            assertEquals(0, engine.getNumVersionLookups());
            assertNotNull(indexResult.getTranslogLocation());
            Engine.IndexResult retryResult = engine.index(retry);
            assertEquals(retry.seqNo() > operation.seqNo(), engine.indexWriterHasDeletions());
            assertEquals(belowLckp ? 0 : 1, engine.getNumVersionLookups());
            assertNotNull(retryResult.getTranslogLocation());
            assertTrue(retryResult.getTranslogLocation().compareTo(indexResult.getTranslogLocation()) > 0);
        } else {
            Engine.IndexResult retryResult = engine.index(retry);
            assertFalse(engine.indexWriterHasDeletions());
            assertEquals(1, engine.getNumVersionLookups());
            assertNotNull(retryResult.getTranslogLocation());
            Engine.IndexResult indexResult = engine.index(operation);
            assertEquals(operation.seqNo() > retry.seqNo(), engine.indexWriterHasDeletions());
            assertEquals(belowLckp ? 1 : 2, engine.getNumVersionLookups());
            assertNotNull(retryResult.getTranslogLocation());
            assertTrue(retryResult.getTranslogLocation().compareTo(indexResult.getTranslogLocation()) < 0);
        }

        engine.refresh("test");
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), 10);
            assertEquals(1, topDocs.totalHits);
        }
        operation = randomAppendOnly(doc, false, 1);
        retry = randomAppendOnly(doc, true, 1);
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
            assertNotNull(retryResult.getTranslogLocation());
            assertTrue(retryResult.getTranslogLocation().compareTo(indexResult.getTranslogLocation()) < 0);
        }

        engine.refresh("test");
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), 10);
            assertEquals(1, topDocs.totalHits);
        }
    }

    public void testDoubleDeliveryReplica() throws IOException {
        final ParsedDocument doc = testParsedDocument("1", null, testDocumentWithTextField(),
            new BytesArray("{}".getBytes(Charset.defaultCharset())), null);
        Engine.Index operation = replicaIndexForDoc(doc, 1, 20, false);
        Engine.Index duplicate = replicaIndexForDoc(doc, 1, 20, true);
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
            assertNotNull(retryResult.getTranslogLocation());
            assertTrue(retryResult.getTranslogLocation().compareTo(indexResult.getTranslogLocation()) > 0);
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
            assertNotNull(retryResult.getTranslogLocation());
            assertTrue(retryResult.getTranslogLocation().compareTo(indexResult.getTranslogLocation()) < 0);
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

        final ParsedDocument doc = testParsedDocument("1", null, testDocumentWithTextField(), new BytesArray("{}".getBytes(Charset.defaultCharset())), null);
        boolean isRetry = false;
        long autoGeneratedIdTimestamp = 0;

        Engine.Index index = new Engine.Index(newUid(doc), doc, SequenceNumbers.UNASSIGNED_SEQ_NO, 0, Versions.MATCH_ANY, VersionType.INTERNAL, PRIMARY, System.nanoTime(), autoGeneratedIdTimestamp, isRetry);
        Engine.IndexResult indexResult = engine.index(index);
        assertThat(indexResult.getVersion(), equalTo(1L));

        index = new Engine.Index(newUid(doc), doc, indexResult.getSeqNo(), index.primaryTerm(), indexResult.getVersion(), index.versionType().versionTypeForReplicationAndRecovery(), REPLICA, System.nanoTime(), autoGeneratedIdTimestamp, isRetry);
        indexResult = replicaEngine.index(index);
        assertThat(indexResult.getVersion(), equalTo(1L));

        isRetry = true;
        index = new Engine.Index(newUid(doc), doc, SequenceNumbers.UNASSIGNED_SEQ_NO, 0, Versions.MATCH_ANY, VersionType.INTERNAL, PRIMARY, System.nanoTime(), autoGeneratedIdTimestamp, isRetry);
        indexResult = engine.index(index);
        assertThat(indexResult.getVersion(), equalTo(1L));
        engine.refresh("test");
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), 10);
            assertEquals(1, topDocs.totalHits);
        }

        index = new Engine.Index(newUid(doc), doc, indexResult.getSeqNo(), index.primaryTerm(), indexResult.getVersion(), index.versionType().versionTypeForReplicationAndRecovery(), REPLICA, System.nanoTime(), autoGeneratedIdTimestamp, isRetry);
        indexResult = replicaEngine.index(index);
        assertThat(indexResult.hasFailure(), equalTo(false));
        replicaEngine.refresh("test");
        try (Engine.Searcher searcher = replicaEngine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), 10);
            assertEquals(1, topDocs.totalHits);
        }
    }

    public void testRetryWithAutogeneratedIdsAndWrongOrderWorksAndNoDuplicateDocs() throws IOException {

        final ParsedDocument doc = testParsedDocument("1", null, testDocumentWithTextField(), new BytesArray("{}".getBytes(Charset.defaultCharset())), null);
        boolean isRetry = true;
        long autoGeneratedIdTimestamp = 0;

        Engine.Index firstIndexRequest = new Engine.Index(newUid(doc), doc, SequenceNumbers.UNASSIGNED_SEQ_NO, 0, Versions.MATCH_ANY, VersionType.INTERNAL, PRIMARY, System.nanoTime(), autoGeneratedIdTimestamp, isRetry);
        Engine.IndexResult result = engine.index(firstIndexRequest);
        assertThat(result.getVersion(), equalTo(1L));

        Engine.Index firstIndexRequestReplica = new Engine.Index(newUid(doc), doc, result.getSeqNo(), firstIndexRequest.primaryTerm(), result.getVersion(), firstIndexRequest.versionType().versionTypeForReplicationAndRecovery(), REPLICA, System.nanoTime(), autoGeneratedIdTimestamp, isRetry);
        Engine.IndexResult indexReplicaResult = replicaEngine.index(firstIndexRequestReplica);
        assertThat(indexReplicaResult.getVersion(), equalTo(1L));

        isRetry = false;
        Engine.Index secondIndexRequest = new Engine.Index(newUid(doc), doc, SequenceNumbers.UNASSIGNED_SEQ_NO, 0, Versions.MATCH_ANY, VersionType.INTERNAL, PRIMARY, System.nanoTime(), autoGeneratedIdTimestamp, isRetry);
        Engine.IndexResult indexResult = engine.index(secondIndexRequest);
        assertTrue(indexResult.isCreated());
        engine.refresh("test");
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), 10);
            assertEquals(1, topDocs.totalHits);
        }

        Engine.Index secondIndexRequestReplica = new Engine.Index(newUid(doc), doc, result.getSeqNo(), secondIndexRequest.primaryTerm(), result.getVersion(), firstIndexRequest.versionType().versionTypeForReplicationAndRecovery(), REPLICA, System.nanoTime(), autoGeneratedIdTimestamp, isRetry);
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
            return appendOnlyReplica(doc, retry, autoGeneratedIdTimestamp, 0);
        }
    }

    public Engine.Index appendOnlyPrimary(ParsedDocument doc, boolean retry, final long autoGeneratedIdTimestamp) {
        return new Engine.Index(newUid(doc), doc, SequenceNumbers.UNASSIGNED_SEQ_NO, 0, Versions.MATCH_ANY,
            VersionType.INTERNAL, Engine.Operation.Origin.PRIMARY, System.nanoTime(), autoGeneratedIdTimestamp, retry);
    }

    public Engine.Index appendOnlyReplica(ParsedDocument doc, boolean retry, final long autoGeneratedIdTimestamp, final long seqNo) {
        return new Engine.Index(newUid(doc), doc, seqNo, 2, 1, VersionType.EXTERNAL,
            Engine.Operation.Origin.REPLICA, System.nanoTime(), autoGeneratedIdTimestamp, retry);
    }

    public void testRetryConcurrently() throws InterruptedException, IOException {
        Thread[] thread = new Thread[randomIntBetween(3, 5)];
        int numDocs = randomIntBetween(1000, 10000);
        List<Engine.Index> docs = new ArrayList<>();
        final boolean primary = randomBoolean();
        for (int i = 0; i < numDocs; i++) {
            final ParsedDocument doc = testParsedDocument(Integer.toString(i), null, testDocumentWithTextField(), new BytesArray("{}".getBytes(Charset.defaultCharset())), null);
            final Engine.Index originalIndex;
            final Engine.Index retryIndex;
            if (primary) {
               originalIndex = appendOnlyPrimary(doc, false, i);
               retryIndex = appendOnlyPrimary(doc, true, i);
            } else {
                originalIndex = appendOnlyReplica(doc, false, i, i * 2);
                retryIndex = appendOnlyReplica(doc, true, i, i * 2);
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

        final long timestamp1 = Math.abs(randomNonNegativeLong());
        final Path storeDir = createTempDir();
        final Path translogDir = createTempDir();
        final long timestamp2 = randomNonNegativeLong();
        final long maxTimestamp12 = Math.max(timestamp1, timestamp2);
        try (Store store = createStore(newFSDirectory(storeDir));
             Engine engine = new InternalEngine(config(defaultSettings, store, translogDir, NoMergePolicy.INSTANCE, null))) {
            assertEquals(IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, engine.segmentsStats(false).getMaxUnsafeAutoIdTimestamp());
            final ParsedDocument doc = testParsedDocument("1", null, testDocumentWithTextField(),
                new BytesArray("{}".getBytes(Charset.defaultCharset())), null);
            engine.index(appendOnlyPrimary(doc, true, timestamp1));
            assertEquals(timestamp1, engine.segmentsStats(false).getMaxUnsafeAutoIdTimestamp());
        }
        try (Store store = createStore(newFSDirectory(storeDir));
             Engine engine = new InternalEngine(config(defaultSettings, store, translogDir, NoMergePolicy.INSTANCE, null))) {
            assertEquals(IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, engine.segmentsStats(false).getMaxUnsafeAutoIdTimestamp());
            engine.recoverFromTranslog();
            assertEquals(timestamp1, engine.segmentsStats(false).getMaxUnsafeAutoIdTimestamp());
            final ParsedDocument doc = testParsedDocument("1", null, testDocumentWithTextField(),
                new BytesArray("{}".getBytes(Charset.defaultCharset())), null);
            engine.index(appendOnlyPrimary(doc, true, timestamp2));
            assertEquals(maxTimestamp12, engine.segmentsStats(false).getMaxUnsafeAutoIdTimestamp());
            engine.flush();
        }
        try (Store store = createStore(newFSDirectory(storeDir));
             Engine engine = new InternalEngine(
                 copy(config(defaultSettings, store, translogDir, NoMergePolicy.INSTANCE, null),
                     randomFrom(EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG, EngineConfig.OpenMode.OPEN_INDEX_CREATE_TRANSLOG)))) {
            assertEquals(maxTimestamp12, engine.segmentsStats(false).getMaxUnsafeAutoIdTimestamp());
        }
    }

    public void testAppendConcurrently() throws InterruptedException, IOException {
        Thread[] thread = new Thread[randomIntBetween(3, 5)];
        int numDocs = randomIntBetween(1000, 10000);
        assertEquals(0, engine.getNumVersionLookups());
        assertEquals(0, engine.getNumIndexVersionsLookups());
        boolean primary = randomBoolean();
        List<Engine.Index> docs = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            final ParsedDocument doc = testParsedDocument(Integer.toString(i), null, testDocumentWithTextField(), new BytesArray("{}".getBytes(Charset.defaultCharset())), null);
            Engine.Index index = primary ? appendOnlyPrimary(doc, false, i) : appendOnlyReplica(doc, false, i, i);
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
            EngineConfig config = config(defaultSettings, store, translogPath, newMergePolicy(), new ReferenceManager.RefreshListener() {
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
            final ParsedDocument doc = testParsedDocument(Integer.toString(docId), null,
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

    public void testSequenceIDs() throws Exception {
        Tuple<Long, Long> seqID = getSequenceID(engine, new Engine.Get(false, "type", "2", newUid("1")));
        // Non-existent doc returns no seqnum and no primary term
        assertThat(seqID.v1(), equalTo(SequenceNumbers.UNASSIGNED_SEQ_NO));
        assertThat(seqID.v2(), equalTo(0L));

        // create a document
        Document document = testDocumentWithTextField();
        document.add(new Field(SourceFieldMapper.NAME, BytesReference.toBytes(B_1), SourceFieldMapper.Defaults.FIELD_TYPE));
        ParsedDocument doc = testParsedDocument("1", null, document, B_1, null);
        engine.index(indexForDoc(doc));
        engine.refresh("test");

        seqID = getSequenceID(engine, newGet(false, doc));
        logger.info("--> got seqID: {}", seqID);
        assertThat(seqID.v1(), equalTo(0L));
        assertThat(seqID.v2(), equalTo(2L));

        // Index the same document again
        document = testDocumentWithTextField();
        document.add(new Field(SourceFieldMapper.NAME, BytesReference.toBytes(B_1), SourceFieldMapper.Defaults.FIELD_TYPE));
        doc = testParsedDocument("1", null, document, B_1, null);
        engine.index(indexForDoc(doc));
        engine.refresh("test");

        seqID = getSequenceID(engine, newGet(false, doc));
        logger.info("--> got seqID: {}", seqID);
        assertThat(seqID.v1(), equalTo(1L));
        assertThat(seqID.v2(), equalTo(2L));

        // Index the same document for the third time, this time changing the primary term
        document = testDocumentWithTextField();
        document.add(new Field(SourceFieldMapper.NAME, BytesReference.toBytes(B_1), SourceFieldMapper.Defaults.FIELD_TYPE));
        doc = testParsedDocument("1", null, document, B_1, null);
        engine.index(new Engine.Index(newUid(doc), doc, SequenceNumbers.UNASSIGNED_SEQ_NO, 3,
                        Versions.MATCH_ANY, VersionType.INTERNAL, Engine.Operation.Origin.PRIMARY,
                        System.nanoTime(), -1, false));
        engine.refresh("test");

        seqID = getSequenceID(engine, newGet(false, doc));
        logger.info("--> got seqID: {}", seqID);
        assertThat(seqID.v1(), equalTo(2L));
        assertThat(seqID.v2(), equalTo(3L));

        // we can query by the _seq_no
        Engine.Searcher searchResult = engine.acquireSearcher("test");
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(1));
        MatcherAssert.assertThat(searchResult, EngineSearcherTotalHitsMatcher.engineSearcherTotalHits(LongPoint.newExactQuery("_seq_no", 2), 1));
        searchResult.close();
    }

    /**
     * A sequence number generator that will generate a sequence number and if {@code stall} is set to true will wait on the barrier and the
     * referenced latch before returning. If the local checkpoint should advance (because {@code stall} is false, then the value of
     * {@code expectedLocalCheckpoint} is set accordingly.
     *
     * @param latchReference          to latch the thread for the purpose of stalling
     * @param barrier                 to signal the thread has generated a new sequence number
     * @param stall                   whether or not the thread should stall
     * @param expectedLocalCheckpoint the expected local checkpoint after generating a new sequence
     *                                number
     * @return a sequence number generator
     */
    private ToLongBiFunction<Engine, Engine.Operation> getStallingSeqNoGenerator(
            final AtomicReference<CountDownLatch> latchReference,
            final CyclicBarrier barrier,
            final AtomicBoolean stall,
            final AtomicLong expectedLocalCheckpoint) {
        return (engine, operation) -> {
            final long seqNo = engine.seqNoService().generateSeqNo();
            final CountDownLatch latch = latchReference.get();
            if (stall.get()) {
                try {
                    barrier.await();
                    latch.await();
                } catch (BrokenBarrierException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            } else {
                if (expectedLocalCheckpoint.get() + 1 == seqNo) {
                    expectedLocalCheckpoint.set(seqNo);
                }
            }
            return seqNo;
        };
    }

    public void testSequenceNumberAdvancesToMaxSeqOnEngineOpenOnPrimary() throws BrokenBarrierException, InterruptedException, IOException {
        engine.close();
        final int docs = randomIntBetween(1, 32);
        InternalEngine initialEngine = null;
        try {
            final AtomicReference<CountDownLatch> latchReference = new AtomicReference<>(new CountDownLatch(1));
            final CyclicBarrier barrier = new CyclicBarrier(2);
            final AtomicBoolean stall = new AtomicBoolean();
            final AtomicLong expectedLocalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
            final List<Thread> threads = new ArrayList<>();
            initialEngine =
                    createEngine(defaultSettings, store, primaryTranslogDir, newMergePolicy(), null, InternalEngine::sequenceNumberService, getStallingSeqNoGenerator(latchReference, barrier, stall, expectedLocalCheckpoint));
            final InternalEngine finalInitialEngine = initialEngine;
            for (int i = 0; i < docs; i++) {
                final String id = Integer.toString(i);
                final ParsedDocument doc = testParsedDocument(id, null, testDocumentWithTextField(), SOURCE, null);

                stall.set(randomBoolean());
                final Thread thread = new Thread(() -> {
                    try {
                        finalInitialEngine.index(indexForDoc(doc));
                    } catch (IOException e) {
                        throw new AssertionError(e);
                    }
                });
                thread.start();
                if (stall.get()) {
                    threads.add(thread);
                    barrier.await();
                } else {
                    thread.join();
                }
            }

            assertThat(initialEngine.seqNoService().getLocalCheckpoint(), equalTo(expectedLocalCheckpoint.get()));
            assertThat(initialEngine.seqNoService().getMaxSeqNo(), equalTo((long) (docs - 1)));
            initialEngine.flush(true, true);

            latchReference.get().countDown();
            for (final Thread thread : threads) {
                thread.join();
            }
        } finally {
            IOUtils.close(initialEngine);
        }

        try (Engine recoveringEngine =
                 new InternalEngine(copy(initialEngine.config(), EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG))) {
            recoveringEngine.recoverFromTranslog();
            recoveringEngine.fillSeqNoGaps(2);
            assertThat(recoveringEngine.seqNoService().getLocalCheckpoint(), greaterThanOrEqualTo((long) (docs - 1)));
        }
    }

    public void testSequenceNumberAdvancesToMaxSeqNoOnEngineOpenOnReplica() throws IOException {
        final long v = 1;
        final VersionType t = VersionType.EXTERNAL;
        final long ts = IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP;
        final int docs = randomIntBetween(1, 32);
        InternalEngine initialEngine = null;
        try {
            initialEngine = engine;
            for (int i = 0; i < docs; i++) {
                final String id = Integer.toString(i);
                final ParsedDocument doc = testParsedDocument(id, null, testDocumentWithTextField(), SOURCE, null);
                final Term uid = newUid(doc);
                // create a gap at sequence number 3 * i + 1
                initialEngine.index(new Engine.Index(uid, doc, 3 * i, 1, v, t, REPLICA, System.nanoTime(), ts, false));
                initialEngine.delete(new Engine.Delete("type", id, uid, 3 * i + 2, 1, v, t, REPLICA, System.nanoTime()));
            }

            // bake the commit with the local checkpoint stuck at 0 and gaps all along the way up to the max sequence number
            assertThat(initialEngine.seqNoService().getLocalCheckpoint(), equalTo((long) 0));
            assertThat(initialEngine.seqNoService().getMaxSeqNo(), equalTo((long) (3 * (docs - 1) + 2)));
            initialEngine.flush(true, true);

            for (int i = 0; i < docs; i++) {
                final String id = Integer.toString(i);
                final ParsedDocument doc = testParsedDocument(id, null, testDocumentWithTextField(), SOURCE, null);
                final Term uid = newUid(doc);
                initialEngine.index(new Engine.Index(uid, doc, 3 * i + 1, 1, v, t, REPLICA, System.nanoTime(), ts, false));
            }
        } finally {
            IOUtils.close(initialEngine);
        }

        try (Engine recoveringEngine =
                 new InternalEngine(copy(initialEngine.config(), EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG))) {
            recoveringEngine.recoverFromTranslog();
            recoveringEngine.fillSeqNoGaps(1);
            assertThat(recoveringEngine.seqNoService().getLocalCheckpoint(), greaterThanOrEqualTo((long) (3 * (docs - 1) + 2 - 1)));
        }
    }

    /** java docs */
    public void testOutOfOrderSequenceNumbersWithVersionConflict() throws IOException {
        final List<Engine.Operation> operations = new ArrayList<>();

        final int numberOfOperations = randomIntBetween(16, 32);
        final Document document = testDocumentWithTextField();
        final AtomicLong sequenceNumber = new AtomicLong();
        final Engine.Operation.Origin origin = randomFrom(LOCAL_TRANSLOG_RECOVERY, PEER_RECOVERY, PRIMARY, REPLICA);
        final LongSupplier sequenceNumberSupplier =
            origin == PRIMARY ? () -> SequenceNumbers.UNASSIGNED_SEQ_NO : sequenceNumber::getAndIncrement;
        document.add(new Field(SourceFieldMapper.NAME, BytesReference.toBytes(B_1), SourceFieldMapper.Defaults.FIELD_TYPE));
        final ParsedDocument doc = testParsedDocument("1", null, document, B_1, null);
        final Term uid = newUid(doc);
        final BiFunction<String, Engine.SearcherScope, Searcher> searcherFactory = engine::acquireSearcher;
        for (int i = 0; i < numberOfOperations; i++) {
            if (randomBoolean()) {
                final Engine.Index index = new Engine.Index(
                    uid,
                    doc,
                    sequenceNumberSupplier.getAsLong(),
                    1,
                    i,
                    VersionType.EXTERNAL,
                    origin,
                    System.nanoTime(),
                    IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP,
                    false);
                operations.add(index);
            } else {
                final Engine.Delete delete = new Engine.Delete(
                    "test",
                    "1",
                    uid,
                    sequenceNumberSupplier.getAsLong(),
                    1,
                    i,
                    VersionType.EXTERNAL,
                    origin,
                    System.nanoTime());
                operations.add(delete);
            }
        }

        final boolean exists = operations.get(operations.size() - 1) instanceof Engine.Index;
        Randomness.shuffle(operations);

        for (final Engine.Operation operation : operations) {
            if (operation instanceof Engine.Index) {
                engine.index((Engine.Index) operation);
            } else {
                engine.delete((Engine.Delete) operation);
            }
        }

        final long expectedLocalCheckpoint;
        if (origin == PRIMARY) {
            // we can only advance as far as the number of operations that did not conflict
            int count = 0;

            // each time the version increments as we walk the list, that counts as a successful operation
            long version = -1;
            for (int i = 0; i < numberOfOperations; i++) {
                if (operations.get(i).version() >= version) {
                    count++;
                    version = operations.get(i).version();
                }
            }

            // sequence numbers start at zero, so the expected local checkpoint is the number of successful operations minus one
            expectedLocalCheckpoint = count - 1;
        } else {
            expectedLocalCheckpoint = numberOfOperations - 1;
        }

        assertThat(engine.seqNoService().getLocalCheckpoint(), equalTo(expectedLocalCheckpoint));
        try (Engine.GetResult result = engine.get(new Engine.Get(true, "type", "2", uid), searcherFactory)) {
            assertThat(result.exists(), equalTo(exists));
        }
    }

    /*
     * This test tests that a no-op does not generate a new sequence number, that no-ops can advance the local checkpoint, and that no-ops
     * are correctly added to the translog.
     */
    public void testNoOps() throws IOException {
        engine.close();
        InternalEngine noOpEngine = null;
        final int maxSeqNo = randomIntBetween(0, 128);
        final int localCheckpoint = randomIntBetween(0, maxSeqNo);
        final int globalCheckpoint = randomIntBetween(0, localCheckpoint);
        try {
            final BiFunction<EngineConfig, SeqNoStats, SequenceNumbersService> supplier = (engineConfig, ignored) -> new SequenceNumbersService(
                    engineConfig.getShardId(),
                    engineConfig.getAllocationId(),
                    engineConfig.getIndexSettings(),
                    maxSeqNo,
                    localCheckpoint,
                    globalCheckpoint);
            noOpEngine = new InternalEngine(copy(engine.config(), EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG), supplier) {
                @Override
                protected long doGenerateSeqNoForOperation(Operation operation) {
                    throw new UnsupportedOperationException();
                }
            };
            noOpEngine.recoverFromTranslog();
            final long primaryTerm = randomNonNegativeLong();
            final int gapsFilled = noOpEngine.fillSeqNoGaps(primaryTerm);
            final String reason = randomAlphaOfLength(16);
            noOpEngine.noOp(
                    new Engine.NoOp(
                            maxSeqNo + 1,
                            primaryTerm,
                            randomFrom(PRIMARY, REPLICA, PEER_RECOVERY, LOCAL_TRANSLOG_RECOVERY),
                            System.nanoTime(),
                            reason));
            assertThat(noOpEngine.seqNoService().getLocalCheckpoint(), equalTo((long) (maxSeqNo + 1)));
            assertThat(noOpEngine.getTranslog().uncommittedOperations(), equalTo(1 + gapsFilled));
            // skip to the op that we added to the translog
            Translog.Operation op;
            Translog.Operation last = null;
            try (Translog.Snapshot snapshot = noOpEngine.getTranslog().newSnapshot()) {
                while ((op = snapshot.next()) != null) {
                    last = op;
                }
            }
            assertNotNull(last);
            assertThat(last, instanceOf(Translog.NoOp.class));
            final Translog.NoOp noOp = (Translog.NoOp) last;
            assertThat(noOp.seqNo(), equalTo((long) (maxSeqNo + 1)));
            assertThat(noOp.primaryTerm(), equalTo(primaryTerm));
            assertThat(noOp.reason(), equalTo(reason));
        } finally {
            IOUtils.close(noOpEngine);
        }
    }

    public void testMinGenerationForSeqNo() throws IOException, BrokenBarrierException, InterruptedException {
        engine.close();
        final int numberOfTriplets = randomIntBetween(1, 32);
        InternalEngine actualEngine = null;
        try {
            final AtomicReference<CountDownLatch> latchReference = new AtomicReference<>();
            final CyclicBarrier barrier = new CyclicBarrier(2);
            final AtomicBoolean stall = new AtomicBoolean();
            final AtomicLong expectedLocalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
            final Map<Thread, CountDownLatch> threads = new LinkedHashMap<>();
            actualEngine =
                    createEngine(defaultSettings, store, primaryTranslogDir, newMergePolicy(), null, InternalEngine::sequenceNumberService, getStallingSeqNoGenerator(latchReference, barrier, stall, expectedLocalCheckpoint));
            final InternalEngine finalActualEngine = actualEngine;
            final Translog translog = finalActualEngine.getTranslog();
            final long generation = finalActualEngine.getTranslog().currentFileGeneration();
            for (int i = 0; i < numberOfTriplets; i++) {
                /*
                 * Index three documents with the first and last landing in the same generation and the middle document being stalled until
                 * a later generation.
                 */
                stall.set(false);
                index(finalActualEngine, 3 * i);

                final CountDownLatch latch = new CountDownLatch(1);
                latchReference.set(latch);
                final int skipId = 3 * i + 1;
                stall.set(true);
                final Thread thread = new Thread(() -> {
                    try {
                        index(finalActualEngine, skipId);
                    } catch (IOException e) {
                        throw new AssertionError(e);
                    }
                });
                thread.start();
                threads.put(thread, latch);
                barrier.await();

                stall.set(false);
                index(finalActualEngine, 3 * i + 2);
                finalActualEngine.flush();

                /*
                 * This sequence number landed in the last generation, but the lower and upper bounds for an earlier generation straddle
                 * this sequence number.
                 */
                assertThat(translog.getMinGenerationForSeqNo(3 * i + 1).translogFileGeneration, equalTo(i + generation));
            }

            int i = 0;
            for (final Map.Entry<Thread, CountDownLatch> entry : threads.entrySet()) {
                final Map<String, String> userData = finalActualEngine.commitStats().getUserData();
                assertThat(userData.get(SequenceNumbers.LOCAL_CHECKPOINT_KEY), equalTo(Long.toString(3 * i)));
                assertThat(userData.get(Translog.TRANSLOG_GENERATION_KEY), equalTo(Long.toString(i + generation)));
                entry.getValue().countDown();
                entry.getKey().join();
                finalActualEngine.flush();
                i++;
            }

        } finally {
            IOUtils.close(actualEngine);
        }
    }

    private void index(final InternalEngine engine, final int id) throws IOException {
        final String docId = Integer.toString(id);
        final ParsedDocument doc =
                testParsedDocument(docId, null, testDocumentWithTextField(), SOURCE, null);
        engine.index(indexForDoc(doc));
    }

    /**
     * Return a tuple representing the sequence ID for the given {@code Get}
     * operation. The first value in the tuple is the sequence number, the
     * second is the primary term.
     */
    private Tuple<Long, Long> getSequenceID(Engine engine, Engine.Get get) throws EngineException {
        try (Searcher searcher = engine.acquireSearcher("get")) {
            final long primaryTerm;
            final long seqNo;
            DocIdAndSeqNo docIdAndSeqNo = VersionsAndSeqNoResolver.loadDocIdAndSeqNo(searcher.reader(), get.uid());
            if (docIdAndSeqNo == null) {
                primaryTerm = 0;
                seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO;
            } else {
                seqNo = docIdAndSeqNo.seqNo;
                primaryTerm = VersionsAndSeqNoResolver.loadPrimaryTerm(docIdAndSeqNo, get.uid().field());
            }
            return new Tuple<>(seqNo, primaryTerm);
        } catch (Exception e) {
            throw new EngineException(shardId, "unable to retrieve sequence id", e);
        }
    }

    public void testRestoreLocalCheckpointFromTranslog() throws IOException {
        engine.close();
        InternalEngine actualEngine = null;
        try {
            final Set<Long> completedSeqNos = new HashSet<>();
            final BiFunction<EngineConfig, SeqNoStats, SequenceNumbersService> supplier = (engineConfig, seqNoStats) -> new SequenceNumbersService(
                    engineConfig.getShardId(),
                    engineConfig.getAllocationId(),
                    engineConfig.getIndexSettings(),
                    seqNoStats.getMaxSeqNo(),
                    seqNoStats.getLocalCheckpoint(),
                    seqNoStats.getGlobalCheckpoint()) {
                @Override
                public void markSeqNoAsCompleted(long seqNo) {
                    super.markSeqNoAsCompleted(seqNo);
                    completedSeqNos.add(seqNo);
                }
            };
            actualEngine = new InternalEngine(copy(engine.config(), EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG), supplier);
            final int operations = randomIntBetween(0, 1024);
            final Set<Long> expectedCompletedSeqNos = new HashSet<>();
            for (int i = 0; i < operations; i++) {
                if (rarely() && i < operations - 1) {
                    continue;
                }
                expectedCompletedSeqNos.add((long) i);
            }

            final ArrayList<Long> seqNos = new ArrayList<>(expectedCompletedSeqNos);
            Randomness.shuffle(seqNos);
            for (final long seqNo : seqNos) {
                final String id = Long.toString(seqNo);
                final ParsedDocument doc = testParsedDocument(id, null, testDocumentWithTextField(), SOURCE, null);
                final Term uid = newUid(doc);
                final long time = System.nanoTime();
                actualEngine.index(new Engine.Index(uid, doc, seqNo, 1, 1, VersionType.EXTERNAL, REPLICA, time, time, false));
                if (rarely()) {
                    actualEngine.rollTranslogGeneration();
                }
            }
            final long currentLocalCheckpoint = actualEngine.seqNoService().getLocalCheckpoint();
            final long resetLocalCheckpoint =
                    randomIntBetween(Math.toIntExact(SequenceNumbers.NO_OPS_PERFORMED), Math.toIntExact(currentLocalCheckpoint));
            actualEngine.seqNoService().resetLocalCheckpoint(resetLocalCheckpoint);
            completedSeqNos.clear();
            actualEngine.restoreLocalCheckpointFromTranslog();
            final Set<Long> intersection = new HashSet<>(expectedCompletedSeqNos);
            intersection.retainAll(LongStream.range(resetLocalCheckpoint + 1, operations).boxed().collect(Collectors.toSet()));
            assertThat(completedSeqNos, equalTo(intersection));
            assertThat(actualEngine.seqNoService().getLocalCheckpoint(), equalTo(currentLocalCheckpoint));
            assertThat(actualEngine.seqNoService().generateSeqNo(), equalTo((long) operations));
        } finally {
            IOUtils.close(actualEngine);
        }
    }

    public void testFillUpSequenceIdGapsOnRecovery() throws IOException {
        final int docs = randomIntBetween(1, 32);
        int numDocsOnReplica = 0;
        long maxSeqIDOnReplica = -1;
        long checkpointOnReplica;
        try {
            for (int i = 0; i < docs; i++) {
                final String docId = Integer.toString(i);
                final ParsedDocument doc =
                        testParsedDocument(docId, null, testDocumentWithTextField(), SOURCE, null);
                Engine.Index primaryResponse = indexForDoc(doc);
                Engine.IndexResult indexResult = engine.index(primaryResponse);
                if (randomBoolean()) {
                    numDocsOnReplica++;
                    maxSeqIDOnReplica = indexResult.getSeqNo();
                    replicaEngine.index(replicaIndexForDoc(doc, 1, indexResult.getSeqNo(), false));
                }
            }
            checkpointOnReplica = replicaEngine.seqNoService().getLocalCheckpoint();
        } finally {
            IOUtils.close(replicaEngine);
        }


        boolean flushed = false;
        Engine recoveringEngine = null;
        try {
            assertEquals(docs - 1, engine.seqNoService().getMaxSeqNo());
            assertEquals(docs - 1, engine.seqNoService().getLocalCheckpoint());
            assertEquals(maxSeqIDOnReplica, replicaEngine.seqNoService().getMaxSeqNo());
            assertEquals(checkpointOnReplica, replicaEngine.seqNoService().getLocalCheckpoint());
            recoveringEngine = new InternalEngine(copy(replicaEngine.config(), EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG));
            assertEquals(numDocsOnReplica, recoveringEngine.getTranslog().uncommittedOperations());
            recoveringEngine.recoverFromTranslog();
            assertEquals(maxSeqIDOnReplica, recoveringEngine.seqNoService().getMaxSeqNo());
            assertEquals(checkpointOnReplica, recoveringEngine.seqNoService().getLocalCheckpoint());
            assertEquals((maxSeqIDOnReplica + 1) - numDocsOnReplica, recoveringEngine.fillSeqNoGaps(2));

            // now snapshot the tlog and ensure the primary term is updated
            try (Translog.Snapshot snapshot = recoveringEngine.getTranslog().newSnapshot()) {
                assertTrue((maxSeqIDOnReplica + 1) - numDocsOnReplica <= snapshot.totalOperations());
                Translog.Operation operation;
                while ((operation = snapshot.next()) != null) {
                    if (operation.opType() == Translog.Operation.Type.NO_OP) {
                        assertEquals(2, operation.primaryTerm());
                    } else {
                        assertEquals(1, operation.primaryTerm());
                    }

                }
                assertEquals(maxSeqIDOnReplica, recoveringEngine.seqNoService().getMaxSeqNo());
                assertEquals(maxSeqIDOnReplica, recoveringEngine.seqNoService().getLocalCheckpoint());
                if ((flushed = randomBoolean())) {
                    recoveringEngine.flush(true, true);
                }
            }
        } finally {
            IOUtils.close(recoveringEngine);
        }

        // now do it again to make sure we preserve values etc.
        try {
            recoveringEngine = new InternalEngine(copy(replicaEngine.config(), EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG));
            if (flushed) {
                assertEquals(0, recoveringEngine.getTranslog().uncommittedOperations());
            }
            recoveringEngine.recoverFromTranslog();
            assertEquals(maxSeqIDOnReplica, recoveringEngine.seqNoService().getMaxSeqNo());
            assertEquals(maxSeqIDOnReplica, recoveringEngine.seqNoService().getLocalCheckpoint());
            assertEquals(0, recoveringEngine.fillSeqNoGaps(3));
            assertEquals(maxSeqIDOnReplica, recoveringEngine.seqNoService().getMaxSeqNo());
            assertEquals(maxSeqIDOnReplica, recoveringEngine.seqNoService().getLocalCheckpoint());
        } finally {
            IOUtils.close(recoveringEngine);
        }
    }


    public void assertSameReader(Searcher left, Searcher right) {
        List<LeafReaderContext> leftLeaves = ElasticsearchDirectoryReader.unwrap(left.getDirectoryReader()).leaves();
        List<LeafReaderContext> rightLeaves = ElasticsearchDirectoryReader.unwrap(right.getDirectoryReader()).leaves();
        assertEquals(rightLeaves.size(), leftLeaves.size());
        for (int i = 0; i < leftLeaves.size(); i++) {
            assertSame(leftLeaves.get(i).reader(), rightLeaves.get(i).reader());
        }
    }

    public void assertNotSameReader(Searcher left, Searcher right) {
        List<LeafReaderContext> leftLeaves = ElasticsearchDirectoryReader.unwrap(left.getDirectoryReader()).leaves();
        List<LeafReaderContext> rightLeaves = ElasticsearchDirectoryReader.unwrap(right.getDirectoryReader()).leaves();
        if (rightLeaves.size() == leftLeaves.size()) {
            for (int i = 0; i < leftLeaves.size(); i++) {
                if (leftLeaves.get(i).reader() != rightLeaves.get(i).reader()) {
                    return; // all is well
                }
            }
            fail("readers are same");
        }
    }

    public void testRefreshScopedSearcher() throws IOException {
        try (Searcher getSearcher = engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL);
                Searcher searchSearcher = engine.acquireSearcher("test", Engine.SearcherScope.EXTERNAL)){
            assertSameReader(getSearcher, searchSearcher);
        }
        for (int i = 0; i < 10; i++) {
            final String docId = Integer.toString(i);
            final ParsedDocument doc =
                testParsedDocument(docId, null, testDocumentWithTextField(), SOURCE, null);
            Engine.Index primaryResponse = indexForDoc(doc);
            engine.index(primaryResponse);
        }
        assertTrue(engine.refreshNeeded());
        engine.refresh("test", Engine.SearcherScope.INTERNAL);
        try (Searcher getSearcher = engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL);
             Searcher searchSearcher = engine.acquireSearcher("test", Engine.SearcherScope.EXTERNAL)){
            assertEquals(10, getSearcher.reader().numDocs());
            assertEquals(0, searchSearcher.reader().numDocs());
            assertNotSameReader(getSearcher, searchSearcher);
        }
        engine.refresh("test", Engine.SearcherScope.EXTERNAL);

        try (Searcher getSearcher = engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL);
             Searcher searchSearcher = engine.acquireSearcher("test", Engine.SearcherScope.EXTERNAL)){
            assertEquals(10, getSearcher.reader().numDocs());
            assertEquals(10, searchSearcher.reader().numDocs());
            assertSameReader(getSearcher, searchSearcher);
        }

        // now ensure external refreshes are reflected on the internal reader
        final String docId = Integer.toString(10);
        final ParsedDocument doc =
            testParsedDocument(docId, null, testDocumentWithTextField(), SOURCE, null);
        Engine.Index primaryResponse = indexForDoc(doc);
        engine.index(primaryResponse);

        engine.refresh("test", Engine.SearcherScope.EXTERNAL);

        try (Searcher getSearcher = engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL);
             Searcher searchSearcher = engine.acquireSearcher("test", Engine.SearcherScope.EXTERNAL)){
            assertEquals(11, getSearcher.reader().numDocs());
            assertEquals(11, searchSearcher.reader().numDocs());
            assertSameReader(getSearcher, searchSearcher);
        }

        try (Searcher searcher = engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL)){
            engine.refresh("test", Engine.SearcherScope.INTERNAL);
            try (Searcher nextSearcher = engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL)){
                assertSame(searcher.searcher(), nextSearcher.searcher());
            }
        }

        try (Searcher searcher = engine.acquireSearcher("test", Engine.SearcherScope.EXTERNAL)){
            engine.refresh("test", Engine.SearcherScope.EXTERNAL);
            try (Searcher nextSearcher = engine.acquireSearcher("test", Engine.SearcherScope.EXTERNAL)){
                assertSame(searcher.searcher(), nextSearcher.searcher());
            }
        }
    }

    public void testSeqNoGenerator() throws IOException {
        engine.close();
        final long seqNo = randomIntBetween(Math.toIntExact(SequenceNumbers.NO_OPS_PERFORMED), Integer.MAX_VALUE);
        final BiFunction<EngineConfig, SeqNoStats, SequenceNumbersService> seqNoService = (config, seqNoStats) -> new SequenceNumbersService(
                config.getShardId(),
                config.getAllocationId(),
                config.getIndexSettings(),
                SequenceNumbers.NO_OPS_PERFORMED,
                SequenceNumbers.NO_OPS_PERFORMED,
                SequenceNumbers.UNASSIGNED_SEQ_NO);
        final AtomicLong seqNoGenerator = new AtomicLong(seqNo);
        try (Engine e = createEngine(defaultSettings, store, primaryTranslogDir, newMergePolicy(), null, seqNoService, (engine, operation) -> seqNoGenerator.getAndIncrement())) {
            final String id = "id";
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

            final Engine.Index index = new Engine.Index(
                    new Term("_id", parsedDocument.id()),
                    parsedDocument,
                    SequenceNumbers.UNASSIGNED_SEQ_NO,
                    (long) randomIntBetween(1, 8),
                    Versions.MATCH_ANY,
                    VersionType.INTERNAL,
                    Engine.Operation.Origin.PRIMARY,
                    System.currentTimeMillis(),
                    System.currentTimeMillis(),
                    randomBoolean());
            final Engine.IndexResult indexResult = e.index(index);
            assertThat(indexResult.getSeqNo(), equalTo(seqNo));
            assertThat(seqNoGenerator.get(), equalTo(seqNo + 1));

            final Engine.Delete delete = new Engine.Delete(
                    type,
                    id,
                    new Term("_id", parsedDocument.id()),
                    SequenceNumbers.UNASSIGNED_SEQ_NO,
                    (long) randomIntBetween(1, 8),
                    Versions.MATCH_ANY,
                    VersionType.INTERNAL,
                    Engine.Operation.Origin.PRIMARY,
                    System.currentTimeMillis());
            final Engine.DeleteResult deleteResult = e.delete(delete);
            assertThat(deleteResult.getSeqNo(), equalTo(seqNo + 1));
            assertThat(seqNoGenerator.get(), equalTo(seqNo + 2));
        }
    }

}
