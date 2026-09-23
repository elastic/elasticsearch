/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query.bitmapterms;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.index.mapper.IndexType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorReducer;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.tasks.TaskCancelledException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static org.elasticsearch.test.InternalAggregationTestCase.DEFAULT_MAX_BUCKETS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class RoaringBitmapAggregatorTests extends AggregatorTestCase {

    private static final String FIELD = "id";
    // A second field carrying the same values but only as doc values, so its aggregation always takes
    // the doc-values path and acts as an oracle for the index_terms field's fast path and fallbacks.
    private static final String BKD_FIELD = "id_bkd";
    private static final String CATEGORY = "category";
    private static final FieldType INDEX_TERMS_TYPE;

    static {
        INDEX_TERMS_TYPE = new FieldType();
        INDEX_TERMS_TYPE.setIndexOptions(IndexOptions.DOCS);
        INDEX_TERMS_TYPE.setOmitNorms(true);
        INDEX_TERMS_TYPE.setTokenized(false);
        INDEX_TERMS_TYPE.freeze();
    }

    public void testIntegerValuesAreDistinctAndPortable() throws Exception {
        InternalRoaringBitmap result = aggregate(NumberFieldMapper.NumberType.INTEGER, 5, 10, 5);

        assertThat(result.width(), equalTo(InternalRoaringBitmap.BitmapFormat.INT));
        assertThat(drain(IntBitmap.deserialize(result.bitmap())), equalTo(List.of(5L, 10L)));
    }

    public void testLongValuesAcrossHighBitBuckets() throws Exception {
        long aboveIntRange = 1L << 40;
        InternalRoaringBitmap result = aggregate(NumberFieldMapper.NumberType.LONG, 1, aboveIntRange, Long.MAX_VALUE, aboveIntRange);

        assertThat(result.width(), equalTo(InternalRoaringBitmap.BitmapFormat.LONG));
        assertThat(drain(LongBitmap.deserializePortable(result.bitmap())), equalTo(List.of(1L, aboveIntRange, Long.MAX_VALUE)));
    }

    public void testMappedFieldWithoutValuesReturnsEmptyBitmap() throws Exception {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(FIELD, NumberFieldMapper.NumberType.INTEGER);
        try (Directory directory = newDirectory(); RandomIndexWriter writer = new RandomIndexWriter(random(), directory)) {
            // A document carrying no value for the field, so collection runs but never creates a bitmap.
            writer.addDocument(new Document());

            try (IndexReader reader = writer.getReader()) {
                InternalRoaringBitmap result = searchAndReduce(
                    reader,
                    new AggTestConfig(new RoaringBitmapAggregationBuilder("ids").field(FIELD), fieldType)
                );
                assertThat(result.width(), equalTo(InternalRoaringBitmap.BitmapFormat.INT));
                assertThat(drain(IntBitmap.deserialize(result.bitmap())), equalTo(List.of()));
            }
        }
    }

    public void testUnmappedFieldReturnsUnmappedBitmap() throws Exception {
        try (Directory directory = newDirectory(); RandomIndexWriter writer = new RandomIndexWriter(random(), directory)) {
            writer.addDocument(new Document());

            try (IndexReader reader = writer.getReader()) {
                InternalRoaringBitmap result = searchAndReduce(
                    reader,
                    new AggTestConfig(new RoaringBitmapAggregationBuilder("ids").field("missing_field"))
                );
                assertThat(result.width(), equalTo(InternalRoaringBitmap.BitmapFormat.UNMAPPED));
                assertThat(result.bitmap().length, equalTo(0));
            }
        }
    }

    public void testTermsIndexFastPathMatchesDocValues() throws Exception {
        long[] integerValues = { 5, 10, 5, Integer.MAX_VALUE };
        assertArrayEquals(
            aggregate(NumberFieldMapper.NumberType.INTEGER, integerValues).bitmap(),
            aggregateTerms(NumberFieldMapper.NumberType.INTEGER, integerValues).bitmap()
        );

        long[] longValues = { 1, 1L << 40, Long.MAX_VALUE, 1L << 40 };
        assertArrayEquals(
            aggregate(NumberFieldMapper.NumberType.LONG, longValues).bitmap(),
            aggregateTerms(NumberFieldMapper.NumberType.LONG, longValues).bitmap()
        );
    }

    /**
     * The index_terms fast path and the doc-values fallback must return the same distinct set no matter
     * the segment layout, deletions or query. This indexes the same values twice per document -- once as
     * indexed terms ({@link #FIELD}) and once as plain doc values ({@link #BKD_FIELD}) -- and asserts,
     * through every phase, that both fields agree with each other and with the set computed from the
     * live documents. {@link #BKD_FIELD} never has an index and so always takes the fallback, acting as
     * an oracle for the fast path. The path itself is only pinned where it is knowable at the aggregator
     * level: engaged on a clean match-all, disabled under a filtering query. Deletions force the
     * fallback per segment without changing that aggregator-level decision, so the path is not asserted
     * while they are present, only the result.
     */
    public void testTermsIndexMatchesDocValuesAcrossSegmentsAndDeletions() throws Exception {
        NumberFieldMapper.NumberType type = NumberFieldMapper.NumberType.LONG;
        MappedFieldType termsFieldType = indexTermsFieldType(type);
        MappedFieldType oracleFieldType = new NumberFieldMapper.NumberFieldType(BKD_FIELD, type);

        List<TermsDoc> segment1 = List.of(new TermsDoc("d1", "a", new long[] { 5, 10 }), new TermsDoc("d2", "b", new long[] { 10, 15 }));
        List<TermsDoc> segment2 = List.of(
            new TermsDoc("d3", "a", new long[] { 15, 1L << 40 }),
            new TermsDoc("d4", "b", new long[] { Long.MAX_VALUE })
        );
        List<TermsDoc> segment3 = List.of(new TermsDoc("d5", "a", new long[] { 1, 2, 3 }), new TermsDoc("d6", "b", new long[] { 5 }));
        List<List<TermsDoc>> segments = List.of(segment1, segment2, segment3);

        List<TermsDoc> all = segments.stream().flatMap(List::stream).toList();
        List<TermsDoc> live = all.stream().filter(doc -> doc.key().equals("d2") == false && doc.key().equals("d4") == false).toList();

        try (Directory directory = newDirectory()) {
            // A no-merge writer keeps each commit its own segment through phases 1 and 2. Closing it
            // commits the pending deletions so the phase-3 writer can force-merge over them.
            try (IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
                for (List<TermsDoc> segment : segments) {
                    for (TermsDoc doc : segment) {
                        Document document = new Document();
                        document.add(new StringField("key", doc.key(), Field.Store.NO));
                        document.add(new StringField(CATEGORY, doc.category(), Field.Store.NO));
                        for (long value : doc.values()) {
                            document.add(new Field(FIELD, encodeTerm(type, value), INDEX_TERMS_TYPE));
                            document.add(new SortedNumericDocValuesField(FIELD, value));
                            document.add(new SortedNumericDocValuesField(BKD_FIELD, value));
                        }
                        writer.addDocument(document);
                    }
                    writer.commit();
                }

                // Phase 1: three live segments, no deletions -- match-all takes the fast path.
                try (IndexReader reader = DirectoryReader.open(writer)) {
                    assertThat(reader.leaves().size(), equalTo(3));
                    assertFieldsAgree(reader, termsFieldType, oracleFieldType, all, true);

                    // A single-bucket filter parent is a legal placement and returns the same set, but
                    // reads doc values because it is a parent, so the fast path is off.
                    AggregationBuilder parent = new FilterAggregationBuilder("parent", new MatchAllQueryBuilder()).subAggregation(
                        new RoaringBitmapAggregationBuilder("ids").field(FIELD)
                    );
                    InternalFilter parentResult = searchAndReduce(reader, new AggTestConfig(parent, termsFieldType));
                    InternalRoaringBitmap nested = parentResult.getAggregations().get("ids");
                    assertThat(drain(LongBitmap.deserializePortable(nested.bitmap())), equalTo(distinctValues(all, doc -> true)));
                }

                // Phase 2: delete two documents -- their segments gain a live-docs bitset, so those
                // leaves fall back even though the aggregator-level decision is unchanged.
                writer.deleteDocuments(new Term("key", "d2"));
                writer.deleteDocuments(new Term("key", "d4"));
                try (IndexReader reader = DirectoryReader.open(writer)) {
                    assertTrue(reader.hasDeletions());
                    assertFieldsAgree(reader, termsFieldType, oracleFieldType, live, false);
                }
            }

            // Phase 3: force-merge to one segment -- deletes are purged, so match-all is fast-path
            // eligible again over exactly the surviving documents.
            try (IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig())) {
                writer.forceMerge(1);
                try (IndexReader reader = DirectoryReader.open(writer)) {
                    assertThat(reader.leaves().size(), equalTo(1));
                    assertFalse(reader.hasDeletions());
                    assertFieldsAgree(reader, termsFieldType, oracleFieldType, live, true);
                }
            }
        }
    }

    public void testTermsIndexFallsBackForMissingValue() throws Exception {
        NumberFieldMapper.NumberType type = NumberFieldMapper.NumberType.INTEGER;
        MappedFieldType fieldType = indexTermsFieldType(type);
        try (Directory directory = newDirectory(); RandomIndexWriter writer = new RandomIndexWriter(random(), directory)) {
            writer.addDocument(termsDocument(type, 5));
            writer.addDocument(new Document());

            try (IndexReader reader = writer.getReader()) {
                InternalRoaringBitmap result = searchAndReduce(
                    reader,
                    new AggTestConfig(new RoaringBitmapAggregationBuilder("ids").field(FIELD).missing(7), fieldType).withCheckAggregator(
                        aggregator -> assertFalse(((RoaringBitmapAggregator) aggregator).usesTermsIndex())
                    )
                );
                assertThat(drain(IntBitmap.deserialize(result.bitmap())), equalTo(List.of(5L, 7L)));
            }
        }
    }

    public void testRejectsMultiBucketParentAggregation() throws Exception {
        NumberFieldMapper.NumberType type = NumberFieldMapper.NumberType.LONG;
        MappedFieldType fieldType = indexTermsFieldType(type);
        try (Directory directory = newDirectory(); RandomIndexWriter writer = new RandomIndexWriter(random(), directory)) {
            // More than one distinct term, otherwise the terms aggregation collapses into a single
            // filter and legitimately passes CardinalityUpperBound.ONE through to its children.
            for (String category : List.of("books", "music")) {
                Document document = termsDocument(type, 5);
                document.add(new StringField(CATEGORY, category, Field.Store.NO));
                document.add(new SortedSetDocValuesField(CATEGORY, new BytesRef(category)));
                writer.addDocument(document);
            }

            try (IndexReader reader = writer.getReader()) {
                AggregationBuilder parent = new TermsAggregationBuilder("parent").field(CATEGORY)
                    .subAggregation(new RoaringBitmapAggregationBuilder("ids").field(FIELD));
                IllegalArgumentException exception = expectThrows(
                    IllegalArgumentException.class,
                    () -> searchAndReduce(reader, new AggTestConfig(parent, fieldType, keywordField(CATEGORY)))
                );
                assertThat(
                    exception.getMessage(),
                    containsString("cannot be nested inside an aggregation that collects more than a single bucket")
                );
            }
        }
    }

    /**
     * An unmapped field takes the {@code createUnmapped} path, which never reaches
     * {@code doCreateInternal}, so the multi-bucket restriction has to be enforced before that split or
     * the aggregator is built anyway and then asked to build one result per parent bucket.
     */
    public void testRejectsMultiBucketParentAggregationForUnmappedField() throws Exception {
        try (Directory directory = newDirectory(); RandomIndexWriter writer = new RandomIndexWriter(random(), directory)) {
            for (String category : List.of("books", "music")) {
                Document document = new Document();
                document.add(new StringField(CATEGORY, category, Field.Store.NO));
                document.add(new SortedSetDocValuesField(CATEGORY, new BytesRef(category)));
                writer.addDocument(document);
            }

            try (IndexReader reader = writer.getReader()) {
                AggregationBuilder parent = new TermsAggregationBuilder("parent").field(CATEGORY)
                    .subAggregation(new RoaringBitmapAggregationBuilder("ids").field("missing_field"));
                IllegalArgumentException exception = expectThrows(
                    IllegalArgumentException.class,
                    () -> searchAndReduce(reader, new AggTestConfig(parent, keywordField(CATEGORY)))
                );
                assertThat(
                    exception.getMessage(),
                    containsString("cannot be nested inside an aggregation that collects more than a single bucket")
                );
            }
        }
    }

    public void testTermsIndexRejectsNegativeMinimumBeforeCollection() throws Exception {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> aggregateTerms(NumberFieldMapper.NumberType.LONG, 1, -1)
        );
        assertThat(exception.getMessage(), containsString("only supports non-negative values"));
    }

    public void testMultiValuedField() throws Exception {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(FIELD, NumberFieldMapper.NumberType.LONG);
        try (Directory directory = newDirectory(); RandomIndexWriter writer = new RandomIndexWriter(random(), directory)) {
            Document document = new Document();
            document.add(new SortedNumericDocValuesField(FIELD, 3));
            document.add(new SortedNumericDocValuesField(FIELD, 7));
            writer.addDocument(document);

            try (IndexReader reader = writer.getReader()) {
                InternalRoaringBitmap result = searchAndReduce(
                    reader,
                    new AggTestConfig(new RoaringBitmapAggregationBuilder("ids").field(FIELD), fieldType)
                );
                assertThat(drain(LongBitmap.deserializePortable(result.bitmap())), equalTo(List.of(3L, 7L)));
            }
        }
    }

    public void testNegativeValuesAreRejected() throws Exception {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> aggregate(NumberFieldMapper.NumberType.LONG, 1, -1)
        );
        assertThat(exception.getMessage(), containsString("only supports non-negative values"));
    }

    public void testCollectionTripsBreakerOnMeasuredGrowth() throws Exception {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(FIELD, NumberFieldMapper.NumberType.LONG);
        CircuitBreakerService breakerService = LimitedBreaker.service(CircuitBreaker.REQUEST, ByteSizeValue.ofKb(128));
        CircuitBreaker breaker = breakerService.getBreaker(CircuitBreaker.REQUEST);
        long baseline = breaker.getUsed();
        try (Directory directory = newDirectory(); RandomIndexWriter writer = new RandomIndexWriter(random(), directory)) {
            Document document = new Document();
            // One value per high word is the shape costing the most heap per value, and enough of them
            // to carry the bitmap past a measurement point well over the limit.
            for (int i = 0; i < 4 * RoaringBitmapAggregator.MIN_VALUES_PER_MEASUREMENT; i++) {
                document.add(new SortedNumericDocValuesField(FIELD, (long) i << 32));
            }
            writer.addDocument(document);
            try (
                IndexReader reader = writer.getReader();
                AggregationContext context = createAggregationContext(
                    reader,
                    createIndexSettings(),
                    Queries.ALL_DOCS_INSTANCE,
                    breakerService,
                    0,
                    DEFAULT_MAX_BUCKETS,
                    false,
                    false,
                    fieldType
                )
            ) {
                Aggregator aggregator = createAggregator(new RoaringBitmapAggregationBuilder("ids").field(FIELD), context);
                aggregator.preCollection();
                expectThrows(CircuitBreakingException.class, () -> context.searcher().search(context.query(), aggregator.asCollector()));
            }
        }
        assertThat(breaker.getUsed(), equalTo(baseline));
    }

    /**
     * Only measured bytes are ever charged, so a value already in the bitmap costs nothing. This
     * collects 400,000 copies of one value under a breaker far too small to survive a charge per
     * collected value.
     */
    public void testRepeatedValuesDoNotTripBreaker() throws Exception {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(FIELD, NumberFieldMapper.NumberType.LONG);
        CircuitBreakerService breakerService = LimitedBreaker.service(CircuitBreaker.REQUEST, ByteSizeValue.ofKb(128));
        CircuitBreaker breaker = breakerService.getBreaker(CircuitBreaker.REQUEST);
        long baseline = breaker.getUsed();
        long value = 1L << 32;
        try (Directory directory = newDirectory(); RandomIndexWriter writer = new RandomIndexWriter(random(), directory)) {
            for (int i = 0; i < 400; i++) {
                Document document = new Document();
                for (int j = 0; j < 1000; j++) {
                    // SortedNumericDocValues keeps duplicates, so all of these reach the collector.
                    document.add(new SortedNumericDocValuesField(FIELD, value));
                }
                writer.addDocument(document);
            }
            try (
                IndexReader reader = writer.getReader();
                AggregationContext context = createAggregationContext(
                    reader,
                    createIndexSettings(),
                    Queries.ALL_DOCS_INSTANCE,
                    breakerService,
                    0,
                    DEFAULT_MAX_BUCKETS,
                    false,
                    false,
                    fieldType
                )
            ) {
                Aggregator aggregator = createAggregator(new RoaringBitmapAggregationBuilder("ids").field(FIELD), context);
                aggregator.preCollection();
                context.searcher().search(context.query(), aggregator.asCollector());
                aggregator.postCollection();
                InternalRoaringBitmap result = (InternalRoaringBitmap) aggregator.buildTopLevel();
                assertThat(drain(LongBitmap.deserializePortable(result.bitmap())), equalTo(List.of(value)));
            }
        }
        assertThat(breaker.getUsed(), equalTo(baseline));
    }

    public void testTermsIndexChecksCancellation() throws Exception {
        NumberFieldMapper.NumberType type = NumberFieldMapper.NumberType.LONG;
        MappedFieldType fieldType = indexTermsFieldType(type);
        CircuitBreakerService breakerService = LimitedBreaker.service(CircuitBreaker.REQUEST, ByteSizeValue.ofMb(64));
        CircuitBreaker breaker = breakerService.getBreaker(CircuitBreaker.REQUEST);
        long baseline = breaker.getUsed();
        try (Directory directory = newDirectory(); RandomIndexWriter writer = new RandomIndexWriter(random(), directory)) {
            writer.addDocument(termsDocument(type, 1));
            try (
                IndexReader reader = writer.getReader();
                AggregationContext realContext = createAggregationContext(
                    reader,
                    createIndexSettings(),
                    Queries.ALL_DOCS_INSTANCE,
                    breakerService,
                    0,
                    DEFAULT_MAX_BUCKETS,
                    false,
                    false,
                    fieldType
                )
            ) {
                // AggregatorTestCase does not expose a cancellation supplier, so spy on an otherwise
                // real aggregation context to exercise the production cancellation path.
                AggregationContext context = spy(realContext);
                when(context.isCancelled()).thenReturn(true);
                Aggregator aggregator = createAggregator(new RoaringBitmapAggregationBuilder("ids").field(FIELD), context);
                assertTrue(((RoaringBitmapAggregator) aggregator).usesTermsIndex());
                aggregator.preCollection();
                expectThrows(TaskCancelledException.class, () -> context.searcher().search(context.query(), aggregator.asCollector()));
            }
        }
        assertThat(breaker.getUsed(), equalTo(baseline));
    }

    public void testIntegerBitmapRejectsValuesAboveIntegerRange() {
        InternalRoaringBitmap.MutableBitmap bitmap = InternalRoaringBitmap.mutable(InternalRoaringBitmap.BitmapFormat.INT);
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> bitmap.add(1L + Integer.MAX_VALUE));
        assertThat(exception.getMessage(), containsString("integer field produced out-of-range value"));
    }

    public void testReducerRejectsWidthMismatch() throws Exception {
        InternalRoaringBitmap integerResult = result(InternalRoaringBitmap.BitmapFormat.INT, 1);
        InternalRoaringBitmap longResult = result(InternalRoaringBitmap.BitmapFormat.LONG, 1L << 40);
        AggregationReduceContext reduceContext = new AggregationReduceContext.ForFinal(
            BigArrays.NON_RECYCLING_INSTANCE,
            null,
            () -> false,
            AggregatorFactories.builder(),
            ignored -> {},
            null
        );

        try (AggregatorReducer reducer = integerResult.getReducer(reduceContext, 2)) {
            reducer.accept(integerResult);
            IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> reducer.accept(longResult));
            assertThat(exception.getMessage(), containsString("cannot reduce [integer] and [long] field results together"));
        }
    }

    private InternalRoaringBitmap aggregate(NumberFieldMapper.NumberType type, long... values) throws Exception {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(FIELD, type);
        try (Directory directory = newDirectory(); RandomIndexWriter writer = new RandomIndexWriter(random(), directory)) {
            for (long value : values) {
                Document document = new Document();
                document.add(new SortedNumericDocValuesField(FIELD, value));
                writer.addDocument(document);
            }
            try (IndexReader reader = writer.getReader()) {
                return searchAndReduce(reader, new AggTestConfig(new RoaringBitmapAggregationBuilder("ids").field(FIELD), fieldType));
            }
        }
    }

    private InternalRoaringBitmap aggregateTerms(NumberFieldMapper.NumberType type, long... values) throws Exception {
        MappedFieldType fieldType = indexTermsFieldType(type);
        try (Directory directory = newDirectory(); RandomIndexWriter writer = new RandomIndexWriter(random(), directory)) {
            for (long value : values) {
                writer.addDocument(termsDocument(type, value));
            }
            try (IndexReader reader = writer.getReader()) {
                return searchAndReduce(
                    reader,
                    new AggTestConfig(new RoaringBitmapAggregationBuilder("ids").field(FIELD), fieldType).withCheckAggregator(
                        aggregator -> assertTrue(((RoaringBitmapAggregator) aggregator).usesTermsIndex())
                    )
                );
            }
        }
    }

    /** A document in {@link #testTermsIndexMatchesDocValuesAcrossSegmentsAndDeletions}'s data model. */
    private record TermsDoc(String key, String category, long[] values) {}

    /**
     * Asserts that the index_terms field and the doc-values oracle field both return the distinct set of
     * the live documents, unfiltered and filtered by category. When {@code fastPathOnMatchAll} is true
     * the unfiltered index_terms run must engage the fast path; the filtered run must always disable it.
     */
    private void assertFieldsAgree(
        IndexReader reader,
        MappedFieldType termsFieldType,
        MappedFieldType oracleFieldType,
        List<TermsDoc> liveDocs,
        boolean fastPathOnMatchAll
    ) throws IOException {
        List<Long> expectedAll = distinctValues(liveDocs, doc -> true);
        Consumer<Aggregator> requireFastPath = fastPathOnMatchAll
            ? aggregator -> assertTrue(((RoaringBitmapAggregator) aggregator).usesTermsIndex())
            : null;
        assertThat(aggregateBitmap(reader, FIELD, termsFieldType, null, requireFastPath), equalTo(expectedAll));
        assertThat(aggregateBitmap(reader, BKD_FIELD, oracleFieldType, null, null), equalTo(expectedAll));

        List<Long> expectedCategoryA = distinctValues(liveDocs, doc -> doc.category().equals("a"));
        Query filter = new TermQuery(new Term(CATEGORY, "a"));
        Consumer<Aggregator> requireFallback = aggregator -> assertFalse(((RoaringBitmapAggregator) aggregator).usesTermsIndex());
        assertThat(aggregateBitmap(reader, FIELD, termsFieldType, filter, requireFallback), equalTo(expectedCategoryA));
        assertThat(aggregateBitmap(reader, BKD_FIELD, oracleFieldType, filter, null), equalTo(expectedCategoryA));
    }

    private List<Long> aggregateBitmap(
        IndexReader reader,
        String field,
        MappedFieldType fieldType,
        Query query,
        Consumer<Aggregator> checkAggregator
    ) throws IOException {
        AggTestConfig config = new AggTestConfig(new RoaringBitmapAggregationBuilder("ids").field(field), fieldType);
        if (query != null) {
            config = config.withQuery(query);
        }
        if (checkAggregator != null) {
            config = config.withCheckAggregator(checkAggregator);
        }
        InternalRoaringBitmap result = searchAndReduce(reader, config);
        return drain(LongBitmap.deserializePortable(result.bitmap()));
    }

    private static List<Long> distinctValues(List<TermsDoc> docs, Predicate<TermsDoc> filter) {
        return docs.stream().filter(filter).flatMapToLong(doc -> Arrays.stream(doc.values())).distinct().sorted().boxed().toList();
    }

    private static MappedFieldType indexTermsFieldType(NumberFieldMapper.NumberType type) {
        return new NumberFieldMapper.NumberFieldType(
            FIELD,
            type,
            IndexType.terms(true, true),
            false,
            true,
            null,
            Map.of(),
            null,
            false,
            null,
            null,
            false,
            false,
            true
        );
    }

    private static Document termsDocument(NumberFieldMapper.NumberType type, long... values) {
        Document document = new Document();
        for (long value : values) {
            document.add(new Field(FIELD, encodeTerm(type, value), INDEX_TERMS_TYPE));
            document.add(new SortedNumericDocValuesField(FIELD, value));
        }
        return document;
    }

    private static BytesRef encodeTerm(NumberFieldMapper.NumberType type, long value) {
        return switch (type) {
            case INTEGER -> {
                byte[] bytes = new byte[Integer.BYTES];
                NumericUtils.intToSortableBytes(Math.toIntExact(value), bytes, 0);
                yield new BytesRef(bytes);
            }
            case LONG -> {
                byte[] bytes = new byte[Long.BYTES];
                NumericUtils.longToSortableBytes(value, bytes, 0);
                yield new BytesRef(bytes);
            }
            default -> throw new IllegalArgumentException("unsupported type [" + type + "]");
        };
    }

    private static InternalRoaringBitmap result(InternalRoaringBitmap.BitmapFormat width, long value) throws IOException {
        InternalRoaringBitmap.MutableBitmap bitmap = InternalRoaringBitmap.mutable(width);
        bitmap.add(value);
        return new InternalRoaringBitmap("ids", width, bitmap.serialize(), null);
    }

    private static List<Long> drain(BitmapValues values) throws IOException {
        List<Long> result = new ArrayList<>();
        BitmapValues.PeekableIterator iterator = values.iterator();
        while (iterator.hasNext()) {
            result.add(iterator.next());
        }
        return result;
    }

    @Override
    protected List<SearchPlugin> getSearchPlugins() {
        return List.of(new BitmapPlugin());
    }
}
