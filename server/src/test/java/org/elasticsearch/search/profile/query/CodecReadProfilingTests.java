/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.profile.query;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreScorerSupplier;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.codec.profiler.Counter;
import org.elasticsearch.codec.profiler.CounterMeasurement;
import org.elasticsearch.codec.profiler.CounterMetric;
import org.elasticsearch.codec.profiler.Metric;
import org.elasticsearch.codec.profiler.Scopes;
import org.elasticsearch.codec.profiler.Snapshot;
import org.elasticsearch.codec.profiler.TestCollectors;
import org.elasticsearch.columnar.ColumNARDocValuesFormat;
import org.elasticsearch.columnar.ColumnarFieldType;
import org.elasticsearch.columnar.ColumnarFormat;
import org.elasticsearch.columnar.ColumnarStringRangeQuery;
import org.elasticsearch.columnar.ColumnarStringTermQuery;
import org.elasticsearch.columnar.ScanBudget;
import org.elasticsearch.columnar.numeric.MultiLevelSkipIndexCodec;
import org.elasticsearch.columnar.numeric.NumericPipeline;
import org.elasticsearch.columnar.string.DictionaryPolicy;
import org.elasticsearch.columnar.string.StringColumnOptions;
import org.elasticsearch.columnar.string.StringColumnOptionsSelector;
import org.elasticsearch.columnar.string.SummaryPolicy;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.cache.query.TrivialQueryCachingPolicy;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.profile.CodecReadAttribution;
import org.elasticsearch.search.profile.CodecReads;
import org.elasticsearch.search.profile.ProfileResult;
import org.elasticsearch.search.profile.ProfileSelection;
import org.elasticsearch.search.profile.ProfileSelection.Names;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

public class CodecReadProfilingTests extends ESTestCase {

    private static final String COLUMNAR = "kw";
    private static final String CODEC = ColumnarFormat.NAME;
    private static final String OTHER = "other";
    private static final int WIDTH = 24;
    private static final CounterMetric READERS = CounterMetric.of("ColumNAR", "readers", "opened", Metric.Unit.COUNT, "readers opened");

    private Directory dir;
    private DirectoryReader reader;
    private int segments;
    private int docsPerSegment;

    @After
    public void closeIndex() throws IOException {
        IOUtils.close(reader, dir);
    }

    public void testAColumnarQueryReportsItsCodecWorkOnTheNodeThatRanIt() throws IOException {
        index(1, between(200, 600));
        final ContextIndexSearcher searcher = searcher(null);
        final QueryProfiler profiler = new QueryProfiler(ProfileSelection.NONE);
        searcher.setProfiler(profiler);
        final TopDocs hits = searcher.search(term(COLUMNAR, 7), 10);
        assertThat(hits.totalHits.value(), equalTo(1L));

        final List<ProfileResult> tree = profiler.getTree();
        assertThat(tree.size(), equalTo(1));
        final Map<String, Long> columnar = codecReadsOf(tree.get(0));
        assertThat(columnar.get("ColumNAR.readers.opened"), greaterThan(0L));
        assertThat("a plain column's values are read as compressed chunks", columnar.get("ColumNAR.chunks.decompressed"), greaterThan(0L));
        assertThat(columnar.get("ColumNAR.chunks.bytes_compressed"), greaterThan(0L));
        assertThat("opening the column took time", columnar.get("ColumNAR.readers.open_time_nanos"), greaterThan(0L));
        assertThat("inflating its chunks took time", columnar.get("ColumNAR.chunks.decompress_time_nanos"), greaterThan(0L));
        assertThat(
            "no more is inflated than the column holds",
            columnar.get("ColumNAR.chunks.bytes_inflated"),
            lessThanOrEqualTo((long) docsPerSegment * WIDTH)
        );
    }

    public void testWithoutCodecReadProfilingTheQueryIsProfiledAsBefore() throws IOException {
        index(1, between(200, 600));
        final ContextIndexSearcher searcher = searcher(null);
        final QueryProfiler profiler = new QueryProfiler(null);
        searcher.setProfiler(profiler);
        final TopDocs hits = searcher.search(term(COLUMNAR, 7), 10);
        assertThat(hits.totalHits.value(), equalTo(1L));

        final List<ProfileResult> tree = profiler.getTree();
        assertThat(tree.size(), equalTo(1));
        assertThat("the query is timed", tree.get(0).getTime(), greaterThan(0L));
        assertThat("no reader was given a recorder", tree.get(0).getTotals(), equalTo(Map.of()));
        assertThat(tree.get(0).getSegments(), equalTo(Map.of()));
        assertThat(tree.get(0).getFields(), equalTo(Map.of()));
    }

    public void testANodeReportsTheSelectedPairsItReadUnderTheirSegment() throws IOException {
        index(between(1, 3), between(100, 300));
        final List<String> segmentNames = segmentNames();
        final String someSegment = randomFrom(segmentNames);
        final ProfileSelection selection = randomFrom(
            ProfileSelection.NONE,
            ProfileSelection.EVERYTHING,
            ProfileSelection.of(Names.EVERY, Names.of(List.of(COLUMNAR))),
            ProfileSelection.of(Names.EVERY, Names.of(List.of(OTHER, "no_such_field"))),
            ProfileSelection.of(Names.of(List.of(someSegment)), Names.EVERY),
            ProfileSelection.of(Names.of(List.of("_no_such_segment")), Names.EVERY)
        );
        final ContextIndexSearcher searcher = searcher(null);
        final QueryProfiler profiler = new QueryProfiler(selection);
        searcher.setProfiler(profiler);
        searcher.search(term(COLUMNAR, between(0, docsPerSegment - 1)), 10);

        final ProfileResult node = profiler.getTree().get(0);
        assertThat("codec counts are not debug output", node.getDebugInfo(), not(hasKey("read")));
        assertThat("the totals are keyed by the doc values format", node.getTotals().keySet(), equalTo(Set.of(ColumnarFormat.NAME)));

        final Set<String> expected = new TreeSet<>();
        for (String segment : segmentNames) {
            if (selection.selectsSegment(segment, null)) {
                expected.add(segment);
            }
        }
        assertThat("a selected segment is listed whatever field it read", node.getSegments().keySet(), equalTo(expected));
        for (String segment : expected) {
            final Map<?, ?> described = (Map<?, ?>) node.getSegments().get(segment);
            assertThat(
                "the segment counts every field it read",
                ((Map<?, ?>) described.get("metrics")).keySet(),
                equalTo(Set.of(ColumnarFormat.NAME))
            );
            if (selection.breaksDown(COLUMNAR) == false) {
                assertFalse("a field that is not broken down is not listed", described.containsKey("fields"));
                continue;
            }
            assertThat(fieldsOf(node, segment).keySet(), equalTo(Set.of(COLUMNAR)));
            final Map<String, Object> pair = fieldsOf(node, segment).get(COLUMNAR);
            assertThat(
                "the pair's metrics are keyed by codec",
                ((Map<?, ?>) pair.get("metrics")).keySet(),
                equalTo(Set.of(ColumnarFormat.NAME))
            );
            assertThat(flatten(((Map<?, ?>) pair.get("metrics")).get(ColumnarFormat.NAME)).get("readers.opened"), greaterThan(0L));
            assertThat(
                "the segment read only this field, so its total is the pair's",
                described.get("metrics"),
                equalTo(pair.get("metrics"))
            );
            if (selection.describesSegments() == false) {
                assertFalse("only every segment describes the segments", described.containsKey("max_doc"));
            }
            if (selection.describesFields() == false) {
                assertThat("only every field describes the fields", List.copyOf(pair.keySet()), equalTo(List.of("metrics")));
                continue;
            }
            assertThat(List.copyOf(pair.keySet()), equalTo(List.of("field_info", "column", "metrics")));
            final Map<?, ?> fieldInfo = (Map<?, ?>) pair.get("field_info");
            assertThat("a ColumNAR column is a binary doc values field", fieldInfo.get("doc_values_type"), equalTo("BINARY"));
            assertThat(fieldInfo.get("doc_values_skip_index_type"), equalTo("NONE"));
            assertThat(fieldInfo.get("index_options"), equalTo("NONE"));
            assertThat(fieldInfo.get("doc_values_gen"), equalTo(-1L));
            assertThat(fieldInfo.get("sort_field"), equalTo(false));
            assertFalse("the field is not indexed", fieldInfo.containsKey("postings_format"));
            assertThat("a column of unique values has no skip index", ((Map<?, ?>) pair.get("column")).get("skip_index"), equalTo(false));
            if (selection.describesSegments() == false) {
                continue;
            }
            assertThat(described.get("max_doc"), equalTo(docsPerSegment));
            assertThat(described.get("num_docs"), equalTo(docsPerSegment));
            assertThat(described.get("root_docs"), equalTo(docsPerSegment));
            assertThat(described.get("nested_docs"), equalTo(0));
            final Map<?, ?> fieldInfos = (Map<?, ?>) described.get("field_infos");
            assertThat("the column and the indexed field", fieldInfos.get("count"), equalTo(2));
            assertThat(fieldInfos.get("doc_values_count"), equalTo(1));
            assertThat(fieldInfos.get("doc_values_formats"), equalTo(Map.of(ColumnarFormat.NAME, 1)));
            assertThat(described.get("lucene_version"), equalTo(org.apache.lucene.util.Version.LATEST.toString()));
            assertThat(described.get("compound_file"), org.hamcrest.Matchers.instanceOf(Boolean.class));
            assertFalse("the index is not sorted", described.containsKey("index_sort"));
        }
    }

    public void testWithEveryPairSelectedTheCountsOfThePairsAddUpToTheTotal() throws IOException {
        index(between(2, 4), between(100, 300));
        final ContextIndexSearcher searcher = searcher(null);
        final QueryProfiler profiler = new QueryProfiler(ProfileSelection.EVERYTHING);
        searcher.setProfiler(profiler);
        searcher.search(term(COLUMNAR, between(0, docsPerSegment - 1)), 10);

        final ProfileResult node = profiler.getTree().get(0);
        final Map<String, Long> total = countsOnly(flatten(node.getTotals().get(ColumnarFormat.NAME)));
        final Map<String, Long> summed = new TreeMap<>();
        for (String segment : node.getSegments().keySet()) {
            countsOnly(flatten(((Map<?, ?>) fieldsOf(node, segment).get(COLUMNAR).get("metrics")).get(ColumnarFormat.NAME))).forEach(
                (key, value) -> summed.merge(key, value, Long::sum)
            );
        }
        assertThat(node.getSegments().size(), equalTo(segments));
        assertThat("every counter of the pairs adds up to the codec total", summed, equalTo(total));
    }

    @SuppressWarnings("unchecked")
    public void testATimerIsAnObjectThatSaysWhetherItIsEstimated() throws IOException {
        index(1, between(200, 600));
        final ContextIndexSearcher searcher = searcher(null);
        final QueryProfiler profiler = new QueryProfiler(ProfileSelection.EVERYTHING);
        searcher.setProfiler(profiler);
        searcher.search(term(COLUMNAR, 7), 10);

        final ProfileResult node = profiler.getTree().get(0);
        final String segment = node.getSegments().keySet().iterator().next();
        for (Object tree : List.of(
            node.getTotals().get(ColumnarFormat.NAME),
            ((Map<?, ?>) fieldsOf(node, segment).get(COLUMNAR).get("metrics")).get(ColumnarFormat.NAME)
        )) {
            final Map<String, Map<String, Object>> components = (Map<String, Map<String, Object>>) tree;
            final Object openTime = components.get("readers").get("open_time_nanos");
            assertThat(
                openTime,
                equalTo(Map.of("value", ((Map<String, Object>) openTime).get("value"), "unit", "ns", "estimated", false, "total_calls", 1L))
            );
            assertThat("a counter stays a number", components.get("readers").get("opened"), equalTo(1L));
            final Map<String, Object> decompress = (Map<String, Object>) components.get("chunks").get("decompress_time_nanos");
            assertThat(decompress.get("unit"), equalTo("ns"));
            if (Boolean.TRUE.equals(decompress.get("estimated"))) {
                assertThat(decompress.keySet(), equalTo(Set.of("value", "unit", "estimated", "total_calls", "sampled_calls")));
                assertThat((Long) decompress.get("sampled_calls"), lessThan((Long) decompress.get("total_calls")));
            } else {
                assertThat(decompress.keySet(), equalTo(Set.of("value", "unit", "estimated", "total_calls")));
            }
        }
    }

    public void testOnlyTheSelectedPairsGetACollectorOfTheirOwn() {
        final ProfileSelection selection = randomFrom(
            ProfileSelection.NONE,
            ProfileSelection.EVERYTHING,
            ProfileSelection.of(Names.EVERY, Names.of(List.of("a"))),
            ProfileSelection.of(null, Names.of(List.of("a"))),
            ProfileSelection.of(Names.of(List.of("_1")), null),
            ProfileSelection.of(Names.of(List.of("_1")), Names.of(List.of("b")))
        );
        final CodecReadProfiling profiling = new CodecReadProfiling(selection, CodecReadAttribution.NO_CHARGE);
        final int leaves = between(1, 10);
        for (int leaf = 0; leaf < leaves; leaf++) {
            final String segment = "_" + (leaf % 2);
            try (Releasable ignored = profiling.leaf(null).bind()) {
                for (String field : List.of("a", "b", "c")) {
                    Scopes.current(ColumnarFormat.NAME, segment, field, Map.of()).counter(READERS).add(1);
                    assertSame(
                        "a pair is handed one collector per leaf",
                        Scopes.current(ColumnarFormat.NAME, segment, field, Map.of()),
                        Scopes.current(ColumnarFormat.NAME, segment, field, Map.of())
                    );
                }
            }
        }
        assertThat(profiling.leafCount(), equalTo(leaves));
        final CodecReads reads = profiling.snapshot();
        assertThat(reads.totals().keySet(), equalTo(Set.of(CODEC)));
        assertThat(reads.totals().get(CODEC), equalTo(Snapshot.of(List.of(new CounterMeasurement(READERS, 3L * leaves)))));
        final Map<String, Long> expected = new TreeMap<>();
        for (int leaf = 0; leaf < leaves; leaf++) {
            for (String field : List.of("a", "b", "c")) {
                final String segment = "_" + (leaf % 2);
                final boolean segmentSelected = selection.selectsSegment(segment, null);
                final boolean fieldReported = selection.selectsField(field) || (segmentSelected && selection.breaksDown(field));
                if (segmentSelected || fieldReported) {
                    expected.merge((segmentSelected ? segment : null) + "/" + (fieldReported ? field : null), 1L, Long::sum);
                }
            }
        }
        final Map<String, Long> actual = new TreeMap<>();
        for (CodecReads.Detail detail : reads.details()) {
            assertThat(detail.codec(), equalTo(CODEC));
            actual.put(detail.segment() + "/" + detail.field(), detail.reads().get(READERS).value());
        }
        assertThat("the leaves that read one detail are summed into one entry", actual, equalTo(expected));
    }

    public void testTheCollectorsOfTheSelectedPairsAreChargedBeforeTheyAreAllocated() {
        final AtomicLong charged = new AtomicLong();
        final CodecReadProfiling profiling = new CodecReadProfiling(ProfileSelection.EVERYTHING, charged::addAndGet);
        final int fields = between(1, 5);
        try (Releasable ignored = profiling.leaf(null).bind()) {
            for (int field = 0; field < fields; field++) {
                Scopes.current(CODEC, "_0", "f" + field, Map.of()).counter(READERS);
            }
        }
        assertThat(
            charged.get(),
            equalTo(fields * (CodecReadAttribution.PAIR_BYTES + TestCollectors.DETAIL_BYTES + TestCollectors.COUNTER_BYTES))
        );

        final CodecReadProfiling refusing = new CodecReadProfiling(ProfileSelection.EVERYTHING, bytes -> {
            throw new CircuitBreakingException("no memory", CircuitBreaker.Durability.TRANSIENT);
        });
        try (Releasable ignored = refusing.leaf(null).bind()) {
            expectThrows(CircuitBreakingException.class, () -> Scopes.current(CODEC, "_0", "f", Map.of()));
        }
        assertThat("a refused pair has no entry", refusing.snapshot().details(), equalTo(List.of()));
    }

    public void testProfilingDoesNotChangeTheHits() throws IOException {
        index(between(1, 3), between(100, 300));
        final Query query = term(COLUMNAR, between(0, docsPerSegment - 1));
        final TopDocs unprofiled = searcher(null).search(query, 10);
        final ContextIndexSearcher profiled = searcher(null);
        profiled.setProfiler(new QueryProfiler(ProfileSelection.NONE));
        final TopDocs hits = profiled.search(query, 10);
        assertThat(hits.totalHits.value(), equalTo(unprofiled.totalHits.value()));
        assertThat(hits.scoreDocs.length, equalTo(unprofiled.scoreDocs.length));
        for (int hit = 0; hit < hits.scoreDocs.length; hit++) {
            assertThat(hits.scoreDocs[hit].doc, equalTo(unprofiled.scoreDocs[hit].doc));
        }
    }

    public void testEverySegmentOpensItsOwnReaderAndAllAreCountedOnTheNode() throws IOException {
        index(between(2, 4), between(50, 150));
        final ContextIndexSearcher searcher = searcher(null);
        final QueryProfiler profiler = new QueryProfiler(ProfileSelection.NONE);
        searcher.setProfiler(profiler);
        searcher.search(term(COLUMNAR, 3), 10);
        assertThat(codecReadsOf(profiler.getTree().get(0)).get("ColumNAR.readers.opened"), greaterThanOrEqualTo((long) segments));
    }

    public void testSegmentsSearchedConcurrentlyAreAllCountedOnTheirNode() throws IOException {
        index(between(3, 6), between(50, 150));
        final ExecutorService executor = Executors.newFixedThreadPool(between(2, 4));
        try {
            final ContextIndexSearcher searcher = searcher(executor);
            final QueryProfiler profiler = new QueryProfiler(ProfileSelection.NONE);
            searcher.setProfiler(profiler);
            final TopDocs hits = searcher.search(term(COLUMNAR, 5), 10);
            assertThat(hits.totalHits.value(), equalTo((long) segments));
            final Map<String, Long> columnar = codecReadsOf(profiler.getTree().get(0));
            assertThat(
                "every segment's readers are counted, however the segments were sliced",
                columnar.get("ColumNAR.readers.opened"),
                greaterThanOrEqualTo((long) segments)
            );
            assertThat(columnar.get("ColumNAR.chunks.decompressed"), greaterThan(0L));
        } finally {
            terminate(executor);
        }
    }

    public void testNestedColumnarQueriesAreEachCountedOnTheirOwnNodeAndNotOnTheirParent() throws IOException {
        index(1, between(200, 400));
        final ContextIndexSearcher searcher = searcher(null);
        final QueryProfiler profiler = new QueryProfiler(ProfileSelection.NONE);
        searcher.setProfiler(profiler);
        final Query query = new BooleanQuery.Builder().add(term(COLUMNAR, 3), BooleanClause.Occur.SHOULD)
            .add(term(COLUMNAR, 9), BooleanClause.Occur.SHOULD)
            .build();
        assertThat(searcher.search(query, 10).totalHits.value(), equalTo(2L));

        final ProfileResult parent = profiler.getTree().get(0);
        assertThat(parent.getProfiledChildren().size(), equalTo(2));
        assertThat("the parent opened no reader itself", parent.getTotals(), equalTo(Map.of()));
        for (ProfileResult child : parent.getProfiledChildren()) {
            assertThat(codecReadsOf(child).get("ColumNAR.readers.opened"), greaterThan(0L));
            assertThat(
                "the time of a child's reader is the child's",
                codecReadsOf(child).get("ColumNAR.readers.open_time_nanos"),
                greaterThan(0L)
            );
        }
    }

    public void testAQueryThatOpensNoColumnarReaderReportsNothingAndKeepsItsDebugOutput() throws IOException {
        index(1, between(50, 100));
        final ContextIndexSearcher searcher = searcher(null);
        final QueryProfiler profiler = new QueryProfiler(ProfileSelection.NONE);
        searcher.setProfiler(profiler);
        assertThat(searcher.search(new TermQuery(new Term(OTHER, "bar")), 10).totalHits.value(), equalTo((long) docsPerSegment));
        assertThat(profiler.getTree().get(0).getDebugInfo(), equalTo(Map.of()));
        assertThat(profiler.getTree().get(0).getTotals(), equalTo(Map.of()));
    }

    public void testARangeOverAClusteredDictionaryColumnReportsTheSpansItSkippedOnItsNode() throws IOException {
        final StringColumnOptions options = StringColumnOptions.DEFAULT.withPolicies(
            new DictionaryPolicy(512 * 1024, 0.0, Double.MAX_VALUE),
            SummaryPolicy.NONE
        ).withSingleValued(true);
        final int runs = 4;
        final int perSegment = 4 * MultiLevelSkipIndexCodec.INTERVAL_SIZE + between(0, MultiLevelSkipIndexCodec.INTERVAL_SIZE);
        dir = newDirectory();
        final FieldType binary = new FieldType();
        binary.setDocValuesType(DocValuesType.BINARY);
        binary.freeze();
        final int docs = 3 * perSegment;
        try (
            IndexWriter writer = new IndexWriter(
                dir,
                new IndexWriterConfig().setCodec(columnarCodec(options)).setMergePolicy(NoMergePolicy.INSTANCE)
            )
        ) {
            for (int doc = 0; doc < docs; doc++) {
                final Document document = new Document();
                document.add(new Field(COLUMNAR, new BytesRef(value((doc % (docs / runs + 1)) * 40 / (docs / runs + 1))), binary));
                writer.addDocument(document);
                if ((doc + 1) % perSegment == 0) {
                    writer.commit();
                }
            }
        }
        try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig().setCodec(columnarCodec(options)))) {
            writer.forceMerge(1);
        }
        reader = DirectoryReader.open(dir);
        segments = 1;
        docsPerSegment = docs;

        final ContextIndexSearcher searcher = searcher(null);
        final QueryProfiler profiler = new QueryProfiler(ProfileSelection.EVERYTHING);
        searcher.setProfiler(profiler);
        final Query range = new ColumnarStringRangeQuery(
            COLUMNAR,
            new BytesRef(value(0)),
            true,
            new BytesRef(value(1)),
            true,
            true,
            ScanBudget.UNLIMITED
        );
        final TopDocs unprofiled = searcher(null).search(range, docs);
        final TopDocs hits = searcher.search(range, docs);
        assertThat(hits.totalHits.value(), equalTo(unprofiled.totalHits.value()));

        final Map<String, Long> columnar = codecReadsOf(profiler.getTree().get(0));
        assertThat(
            "the skip work done while the scorer was built is counted on the node; its values are pinned by the codec's tests",
            columnar,
            hasKey("ColumNAR.ordinal_skip.intervals_rejected")
        );
        final ProfileResult node = profiler.getTree().get(0);
        final String segment = node.getSegments().keySet().iterator().next();
        assertThat(
            "a clustered dictionary column has a skip index over its ordinals",
            ((Map<?, ?>) fieldsOf(node, segment).get(COLUMNAR).get("column")).get("skip_index"),
            equalTo(true)
        );
    }

    public void testAScopeThatOpensNoReaderAllocatesNoCollector() {
        final CodecReadProfiling profiling = new CodecReadProfiling(ProfileSelection.EVERYTHING, CodecReadAttribution.NO_CHARGE);
        for (int call = 0; call < between(1, 50); call++) {
            try (Releasable ignored = profiling.leaf(null).bind()) {
                assertTrue(TestCollectors.isBound());
            }
        }
        assertFalse(TestCollectors.isBound());
        assertThat(profiling.snapshot(), equalTo(CodecReads.EMPTY));
        assertThat(profiling.leafCount(), equalTo(0));
    }

    public void testOnlyTheScopesThatOpenAReaderAllocateACollector() {
        final CodecReadProfiling profiling = new CodecReadProfiling(ProfileSelection.NONE, CodecReadAttribution.NO_CHARGE);
        int opening = 0;
        for (int call = 0; call < 20; call++) {
            try (Releasable ignored = profiling.leaf(null).bind()) {
                if (randomBoolean()) {
                    opening++;
                    Scopes.current(CODEC, "_0", "f", Map.of()).counter(READERS).add(1);
                    assertSame(
                        "a scope hands out one collector",
                        Scopes.current(CODEC, "_0", "f", Map.of()),
                        Scopes.current(CODEC, "_0", "f", Map.of())
                    );
                }
            }
        }
        assertThat(profiling.leafCount(), equalTo(opening));
        final CounterMeasurement opened = profiling.snapshot().totals().getOrDefault(CODEC, Snapshot.EMPTY).get(READERS);
        assertThat(opened == null ? 0L : opened.value(), equalTo((long) opening));
    }

    public void testScopesOpenedOnManyThreadsAreAllSummedAndNeverShared() throws Exception {
        final ProfileSelection selection = randomFrom(ProfileSelection.NONE, ProfileSelection.EVERYTHING);
        final CodecReadProfiling profiling = new CodecReadProfiling(selection, CodecReadAttribution.NO_CHARGE);
        final int threads = between(2, 8);
        final int perThread = between(1, 30);
        final ExecutorService executor = Executors.newFixedThreadPool(threads);
        try {
            final List<Future<?>> done = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                done.add(executor.submit(() -> {
                    for (int call = 0; call < perThread; call++) {
                        try (Releasable ignored = profiling.leaf(null).bind()) {
                            final Counter counter = Scopes.current(CODEC, "_0", "f", Map.of()).counter(READERS);
                            for (int event = 0; event < 100; event++) {
                                counter.add(1);
                            }
                        }
                    }
                }));
            }
            for (Future<?> future : done) {
                future.get();
            }
            assertThat(profiling.leafCount(), equalTo(threads * perThread));
            final CodecReads reads = profiling.snapshot();
            assertThat(
                "no event is lost or counted twice",
                reads.totals().get(CODEC).get(READERS).value(),
                equalTo(100L * threads * perThread)
            );
            if (selection.isNone() == false) {
                assertThat(reads.details().get(0).reads().get(READERS).value(), equalTo(100L * threads * perThread));
            }
        } finally {
            terminate(executor);
        }
    }

    public void testAReaderOpenedWhileBuildingTheSupplierIsCountedOnTheNodeToo() throws IOException {
        index(between(1, 3), between(50, 100));
        final ContextIndexSearcher searcher = searcher(null);
        final QueryProfiler profiler = new QueryProfiler(ProfileSelection.NONE);
        searcher.setProfiler(profiler);
        assertThat(searcher.search(new OpensItsReaderEarly(COLUMNAR), 10).totalHits.value(), equalTo((long) segments * docsPerSegment));
        assertThat(codecReadsOf(profiler.getTree().get(0)).get("ColumNAR.readers.opened"), greaterThanOrEqualTo((long) segments));
    }

    public void testAReaderOpenedWhileCountingIsCountedOnTheNode() throws IOException {
        index(between(1, 3), between(50, 100));
        final ContextIndexSearcher searcher = searcher(null);
        final QueryProfiler profiler = new QueryProfiler(ProfileSelection.NONE);
        searcher.setProfiler(profiler);
        assertThat(searcher.count(new OpensItsReaderInCount(COLUMNAR)), equalTo(segments * docsPerSegment));
        // NOTE: IndexSearcher.count wraps the query in a ConstantScoreQuery, so the counting node is the wrapper's child.
        final ProfileResult wrapper = profiler.getTree().get(0);
        assertThat("the wrapper opened no reader itself", wrapper.getTotals(), equalTo(Map.of()));
        final ProfileResult counted = wrapper.getProfiledChildren().get(0);
        assertThat(counted.getQueryName(), equalTo(OpensItsReaderInCount.class.getSimpleName()));
        assertThat(codecReadsOf(counted).get("ColumNAR.readers.opened"), greaterThanOrEqualTo((long) segments));
        assertFalse("counting leaves no scope bound", TestCollectors.isBound());
    }

    public void testAReaderOpenedLateIsCountedOnTheNodeThatOpenedItAndNotOnItsParentOrSibling() throws IOException {
        index(between(1, 3), between(50, 100));
        for (LatePhase phase : LatePhase.values()) {
            final Query query = new BooleanQuery.Builder().add(new OpensItsReaderLate(COLUMNAR, phase, false), BooleanClause.Occur.MUST)
                .add(new TermQuery(new Term(OTHER, "bar")), BooleanClause.Occur.MUST)
                .build();
            final TopDocs unprofiled = searcher(null).search(query, 10);
            final ContextIndexSearcher searcher = searcher(null);
            final QueryProfiler profiler = new QueryProfiler(ProfileSelection.NONE);
            searcher.setProfiler(profiler);
            final TopDocs profiled = searcher.search(query, 10);

            assertThat(phase + ": same hits", profiled.totalHits, equalTo(unprofiled.totalHits));
            assertThat(profiled.scoreDocs.length, equalTo(unprofiled.scoreDocs.length));
            for (int hit = 0; hit < profiled.scoreDocs.length; hit++) {
                assertThat(profiled.scoreDocs[hit].doc, equalTo(unprofiled.scoreDocs[hit].doc));
                assertThat(profiled.scoreDocs[hit].score, equalTo(unprofiled.scoreDocs[hit].score));
            }
            final ProfileResult parent = profiler.getTree().get(0);
            assertThat(phase + ": the parent opened no reader", parent.getTotals(), equalTo(Map.of()));
            for (ProfileResult child : parent.getProfiledChildren()) {
                if (child.getQueryName().equals(OpensItsReaderLate.class.getSimpleName())) {
                    assertThat(phase.name(), codecReadsOf(child).get("ColumNAR.readers.opened"), greaterThanOrEqualTo((long) segments));
                } else {
                    assertThat(phase + ": the sibling opened no reader", child.getTotals(), equalTo(Map.of()));
                }
            }
            assertFalse(phase + ": no scope stays bound", TestCollectors.isBound());
        }
    }

    public void testAReaderOpenedLateByAFailingCallLeavesNoScopeBound() throws IOException {
        index(1, between(50, 100));
        final LatePhase phase = randomFrom(LatePhase.values());
        final Query query = new BooleanQuery.Builder().add(new OpensItsReaderLate(COLUMNAR, phase, true), BooleanClause.Occur.MUST)
            .add(new TermQuery(new Term(OTHER, "bar")), BooleanClause.Occur.MUST)
            .build();
        final ContextIndexSearcher searcher = searcher(null);
        searcher.setProfiler(new QueryProfiler(ProfileSelection.NONE));
        expectThrows(Exception.class, () -> searcher.search(query, 10));
        assertFalse(phase + ": no scope stays bound", TestCollectors.isBound());
    }

    private enum LatePhase {
        COST,
        NEXT_DOC,
        MATCHES,
        SCORE
    }

    private static final class OpensItsReaderLate extends Query {
        private final String field;
        private final LatePhase phase;
        private final boolean fail;

        OpensItsReaderLate(String field, LatePhase phase, boolean fail) {
            this.field = field;
            this.phase = phase;
            this.fail = fail;
        }

        private void open(LeafReaderContext context) throws IOException {
            context.reader().getBinaryDocValues(field);
            if (fail) {
                throw new IOException("failed after opening the reader in " + phase);
            }
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
            return new Weight(this) {
                @Override
                public ScorerSupplier scorerSupplier(LeafReaderContext context) {
                    final int maxDoc = context.reader().maxDoc();
                    return new ScorerSupplier() {
                        @Override
                        public Scorer get(long leadCost) {
                            return new OpensLate(context, maxDoc);
                        }

                        @Override
                        public long cost() {
                            if (phase == LatePhase.COST) {
                                try {
                                    open(context);
                                } catch (IOException e) {
                                    throw new UncheckedIOException(e);
                                }
                            }
                            return maxDoc;
                        }
                    };
                }

                @Override
                public Explanation explain(LeafReaderContext context, int doc) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public boolean isCacheable(LeafReaderContext ctx) {
                    return false;
                }
            };
        }

        private final class OpensLate extends Scorer {
            private final LeafReaderContext context;
            private final DocIdSetIterator all;
            private final TwoPhaseIterator twoPhase;
            private boolean opened;

            OpensLate(LeafReaderContext context, int maxDoc) {
                this.context = context;
                this.all = DocIdSetIterator.all(maxDoc);
                this.twoPhase = phase != LatePhase.MATCHES ? null : new TwoPhaseIterator(all) {
                    @Override
                    public boolean matches() throws IOException {
                        openOnce();
                        return true;
                    }

                    @Override
                    public float matchCost() {
                        return 1;
                    }
                };
            }

            private void openOnce() throws IOException {
                if (opened == false) {
                    opened = true;
                    open(context);
                }
            }

            @Override
            public int docID() {
                return all.docID();
            }

            @Override
            public DocIdSetIterator iterator() {
                if (twoPhase != null) {
                    return TwoPhaseIterator.asDocIdSetIterator(twoPhase);
                }
                if (phase != LatePhase.NEXT_DOC) {
                    return all;
                }
                return new DocIdSetIterator() {
                    @Override
                    public int docID() {
                        return all.docID();
                    }

                    @Override
                    public int nextDoc() throws IOException {
                        openOnce();
                        return all.nextDoc();
                    }

                    @Override
                    public int advance(int target) throws IOException {
                        openOnce();
                        return all.advance(target);
                    }

                    @Override
                    public long cost() {
                        return all.cost();
                    }
                };
            }

            @Override
            public TwoPhaseIterator twoPhaseIterator() {
                return twoPhase;
            }

            @Override
            public float getMaxScore(int upTo) {
                return 1;
            }

            @Override
            public float score() throws IOException {
                if (phase == LatePhase.SCORE) {
                    openOnce();
                }
                return 1;
            }
        }

        @Override
        public String toString(String defaultField) {
            return "opens_its_reader_late(" + field + ", " + phase + ")";
        }

        @Override
        public void visit(QueryVisitor visitor) {
            visitor.visitLeaf(this);
        }

        @Override
        public boolean equals(Object other) {
            return sameClassAs(other)
                && field.equals(((OpensItsReaderLate) other).field)
                && phase == ((OpensItsReaderLate) other).phase
                && fail == ((OpensItsReaderLate) other).fail;
        }

        @Override
        public int hashCode() {
            return 31 * (31 * classHash() + field.hashCode()) + phase.hashCode() + (fail ? 1 : 0);
        }
    }

    private static final class OpensItsReaderInCount extends Query {
        private final String field;

        OpensItsReaderInCount(String field) {
            this.field = field;
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
            return new ConstantScoreWeight(this, boost) {
                @Override
                public int count(LeafReaderContext context) throws IOException {
                    context.reader().getBinaryDocValues(field);
                    return context.reader().maxDoc();
                }

                @Override
                public ScorerSupplier scorerSupplier(LeafReaderContext context) {
                    throw new AssertionError("a count answered by count() never builds a scorer");
                }

                @Override
                public boolean isCacheable(LeafReaderContext ctx) {
                    return false;
                }
            };
        }

        @Override
        public String toString(String defaultField) {
            return "opens_its_reader_in_count(" + field + ")";
        }

        @Override
        public void visit(QueryVisitor visitor) {
            visitor.visitLeaf(this);
        }

        @Override
        public boolean equals(Object obj) {
            return sameClassAs(obj) && field.equals(((OpensItsReaderInCount) obj).field);
        }

        @Override
        public int hashCode() {
            return classHash() * 31 + field.hashCode();
        }
    }

    private static final class OpensItsReaderEarly extends Query {
        private final String field;

        OpensItsReaderEarly(String field) {
            this.field = field;
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
            return new ConstantScoreWeight(this, boost) {
                @Override
                public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                    context.reader().getBinaryDocValues(field);
                    return ConstantScoreScorerSupplier.fromIterator(
                        DocIdSetIterator.all(context.reader().maxDoc()),
                        score(),
                        scoreMode,
                        context.reader().maxDoc()
                    );
                }

                @Override
                public boolean isCacheable(LeafReaderContext ctx) {
                    return false;
                }
            };
        }

        @Override
        public String toString(String defaultField) {
            return "opens_its_reader_early(" + field + ")";
        }

        @Override
        public void visit(QueryVisitor visitor) {
            visitor.visitLeaf(this);
        }

        @Override
        public boolean equals(Object other) {
            return sameClassAs(other) && field.equals(((OpensItsReaderEarly) other).field);
        }

        @Override
        public int hashCode() {
            return 31 * classHash() + field.hashCode();
        }
    }

    private static Map<String, Long> codecReadsOf(ProfileResult result) {
        assertFalse("the node reported no codec read counts: " + result, result.getTotals().isEmpty());
        return totalOf(result);
    }

    private static Map<String, Long> totalOf(ProfileResult result) {
        final Map<String, Long> total = new TreeMap<>();
        result.getTotals().forEach((codec, metrics) -> flatten(metrics).forEach((key, value) -> total.put(codec + "." + key, value)));
        return total;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Long> flatten(Object tree) {
        final Map<String, Long> flat = new TreeMap<>();
        ((Map<String, Object>) tree).forEach((component, metrics) -> {
            if (metrics instanceof Map<?, ?> byName) {
                byName.forEach(
                    (name, value) -> flat.put(
                        component + "." + name,
                        value instanceof Map<?, ?> timer ? ((Number) timer.get("value")).longValue() : ((Number) value).longValue()
                    )
                );
            }
        });
        return flat;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Map<String, Object>> fieldsOf(ProfileResult result, String segment) {
        return (Map<String, Map<String, Object>>) ((Map<String, Object>) result.getSegments().get(segment)).get("fields");
    }

    private static Map<String, Long> countsOnly(Map<String, Long> counts) {
        final Map<String, Long> kept = new TreeMap<>(counts);
        kept.keySet().removeIf(name -> name.endsWith("_time_nanos"));
        return kept;
    }

    private List<String> segmentNames() {
        final List<String> names = new ArrayList<>();
        for (LeafReaderContext leaf : reader.leaves()) {
            names.add(((SegmentReader) FilterLeafReader.unwrap(leaf.reader())).getSegmentName());
        }
        return names;
    }

    private Query term(String field, int doc) {
        final String value = value(doc);
        return field.equals(COLUMNAR)
            ? ColumnarStringTermQuery.term(field, new BytesRef(value), true, ScanBudget.UNLIMITED)
            : new TermQuery(new Term(field, value));
    }

    private static String value(int doc) {
        return String.format(Locale.ROOT, "%0" + WIDTH + "d", doc);
    }

    private ContextIndexSearcher searcher(ExecutorService executor) throws IOException {
        return executor == null
            ? new ContextIndexSearcher(
                reader,
                IndexSearcher.getDefaultSimilarity(),
                IndexSearcher.getDefaultQueryCache(),
                TrivialQueryCachingPolicy.ALWAYS,
                true
            )
            : new ContextIndexSearcher(
                reader,
                IndexSearcher.getDefaultSimilarity(),
                IndexSearcher.getDefaultQueryCache(),
                TrivialQueryCachingPolicy.ALWAYS,
                true,
                executor,
                segments,
                1
            );
    }

    private void index(int segments, int docsPerSegment) throws IOException {
        this.segments = segments;
        this.docsPerSegment = docsPerSegment;
        dir = newDirectory();
        final FieldType binary = new FieldType();
        binary.setDocValuesType(DocValuesType.BINARY);
        binary.freeze();
        final IndexWriterConfig config = new IndexWriterConfig().setCodec(columnarCodec()).setMergePolicy(NoMergePolicy.INSTANCE);
        try (IndexWriter writer = new IndexWriter(dir, config)) {
            for (int segment = 0; segment < segments; segment++) {
                for (int doc = 0; doc < docsPerSegment; doc++) {
                    final Document document = new Document();
                    document.add(new Field(COLUMNAR, new BytesRef(value(doc)), binary));
                    document.add(new StringField(OTHER, value(doc).isEmpty() ? "" : "bar", Field.Store.NO));
                    writer.addDocument(document);
                }
                writer.commit();
            }
        }
        reader = DirectoryReader.open(dir);
        assertThat(reader.leaves().size(), equalTo(segments));
    }

    private static Codec columnarCodec() {
        return columnarCodec(StringColumnOptions.DEFAULT.withPolicies(DictionaryPolicy.NONE, SummaryPolicy.NONE).withSingleValued(true));
    }

    private static Codec columnarCodec(StringColumnOptions options) {
        final Codec base = TestUtil.getDefaultCodec();
        final DocValuesFormat columnar = new ColumNARDocValuesFormat(
            (fieldName, type) -> NumericPipeline::defaultPipeline,
            field -> ColumnarFieldType.STRING,
            ColumNARDocValuesFormat.DEFAULT_BLOCK_SIZE,
            StringColumnOptionsSelector.always(options)
        );
        return new FilterCodec(base.getName(), base) {
            private final DocValuesFormat perField = new PerFieldDocValuesFormat() {
                @Override
                public DocValuesFormat getDocValuesFormatForField(String field) {
                    return columnar;
                }
            };

            @Override
            public DocValuesFormat docValuesFormat() {
                return perField;
            }
        };
    }
}
