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
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
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
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.LongUnaryOperator;

import static org.elasticsearch.test.InternalAggregationTestCase.DEFAULT_MAX_BUCKETS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class RoaringBitmapAggregatorTests extends AggregatorTestCase {

    private static final String FIELD = "id";
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

    public void testTermsIndexFastPathHandlesMultiValuedFields() throws Exception {
        NumberFieldMapper.NumberType type = NumberFieldMapper.NumberType.LONG;
        MappedFieldType fieldType = indexTermsFieldType(type);
        try (Directory directory = newDirectory(); RandomIndexWriter writer = new RandomIndexWriter(random(), directory)) {
            writer.addDocument(termsDocument(type, 3, 7));

            try (IndexReader reader = writer.getReader()) {
                InternalRoaringBitmap result = searchAndReduce(
                    reader,
                    new AggTestConfig(new RoaringBitmapAggregationBuilder("ids").field(FIELD), fieldType).withCheckAggregator(
                        aggregator -> assertTrue(((RoaringBitmapAggregator) aggregator).usesTermsIndex())
                    )
                );
                assertThat(drain(LongBitmap.deserializePortable(result.bitmap())), equalTo(List.of(3L, 7L)));
            }
        }
    }

    public void testTermsIndexFastPathAcrossSegments() throws Exception {
        NumberFieldMapper.NumberType type = NumberFieldMapper.NumberType.INTEGER;
        MappedFieldType fieldType = indexTermsFieldType(type);
        try (
            Directory directory = newDirectory();
            IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))
        ) {
            writer.addDocument(termsDocument(type, 5, 10));
            writer.commit();
            writer.addDocument(termsDocument(type, 10, 15));
            writer.commit();

            try (IndexReader reader = DirectoryReader.open(writer)) {
                assertThat(reader.leaves().size(), equalTo(2));
                InternalRoaringBitmap result = searchAndReduce(
                    reader,
                    new AggTestConfig(new RoaringBitmapAggregationBuilder("ids").field(FIELD), fieldType).withCheckAggregator(
                        aggregator -> assertTrue(((RoaringBitmapAggregator) aggregator).usesTermsIndex())
                    )
                );
                assertThat(drain(IntBitmap.deserialize(result.bitmap())), equalTo(List.of(5L, 10L, 15L)));
            }
        }
    }

    public void testTermsIndexFallsBackForFilteredQuery() throws Exception {
        NumberFieldMapper.NumberType type = NumberFieldMapper.NumberType.INTEGER;
        MappedFieldType fieldType = indexTermsFieldType(type);
        try (Directory directory = newDirectory(); RandomIndexWriter writer = new RandomIndexWriter(random(), directory)) {
            Document odd = termsDocument(type, 5);
            odd.add(new StringField(CATEGORY, "odd", Field.Store.NO));
            writer.addDocument(odd);
            Document even = termsDocument(type, 10);
            even.add(new StringField(CATEGORY, "even", Field.Store.NO));
            writer.addDocument(even);

            try (IndexReader reader = writer.getReader()) {
                InternalRoaringBitmap result = searchAndReduce(
                    reader,
                    new AggTestConfig(new RoaringBitmapAggregationBuilder("ids").field(FIELD), fieldType).withQuery(
                        new TermQuery(new Term(CATEGORY, "odd"))
                    ).withCheckAggregator(aggregator -> assertFalse(((RoaringBitmapAggregator) aggregator).usesTermsIndex()))
                );
                assertThat(drain(IntBitmap.deserialize(result.bitmap())), equalTo(List.of(5L)));
            }
        }
    }

    public void testTermsIndexFallsBackWhenSegmentHasDeletions() throws Exception {
        NumberFieldMapper.NumberType type = NumberFieldMapper.NumberType.LONG;
        MappedFieldType fieldType = indexTermsFieldType(type);
        try (
            Directory directory = newDirectory();
            IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))
        ) {
            Document live = termsDocument(type, 5);
            live.add(new StringField("key", "live", Field.Store.NO));
            writer.addDocument(live);
            Document deleted = termsDocument(type, 10);
            deleted.add(new StringField("key", "deleted", Field.Store.NO));
            writer.addDocument(deleted);
            writer.commit();
            writer.deleteDocuments(new Term("key", "deleted"));

            try (IndexReader reader = DirectoryReader.open(writer)) {
                assertTrue(reader.hasDeletions());
                InternalRoaringBitmap result = searchAndReduce(
                    reader,
                    new AggTestConfig(new RoaringBitmapAggregationBuilder("ids").field(FIELD), fieldType)
                );
                assertThat(drain(LongBitmap.deserializePortable(result.bitmap())), equalTo(List.of(5L)));
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

    public void testTermsIndexFallsBackUnderParentAggregation() throws Exception {
        NumberFieldMapper.NumberType type = NumberFieldMapper.NumberType.LONG;
        MappedFieldType fieldType = indexTermsFieldType(type);
        try (Directory directory = newDirectory(); RandomIndexWriter writer = new RandomIndexWriter(random(), directory)) {
            writer.addDocument(termsDocument(type, 5, 10));

            try (IndexReader reader = writer.getReader()) {
                AggregationBuilder parent = new FilterAggregationBuilder("parent", new MatchAllQueryBuilder()).subAggregation(
                    new RoaringBitmapAggregationBuilder("ids").field(FIELD)
                );
                InternalFilter result = searchAndReduce(reader, new AggTestConfig(parent, fieldType));
                InternalRoaringBitmap bitmap = result.getAggregations().get("ids");
                assertThat(drain(LongBitmap.deserializePortable(bitmap.bitmap())), equalTo(List.of(5L, 10L)));
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

    public void testRequestBreakerBytesAreReleasedOnClose() throws Exception {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(FIELD, NumberFieldMapper.NumberType.LONG);
        CircuitBreakerService breakerService = LimitedBreaker.service(CircuitBreaker.REQUEST, ByteSizeValue.ofMb(64));
        CircuitBreaker breaker = breakerService.getBreaker(CircuitBreaker.REQUEST);
        long baseline = breaker.getUsed();
        try (Directory directory = newDirectory(); RandomIndexWriter writer = new RandomIndexWriter(random(), directory)) {
            for (int i = 0; i < 20_000; i++) {
                Document document = new Document();
                document.add(new SortedNumericDocValuesField(FIELD, ((long) i << 32) | i));
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
                aggregator.buildTopLevel();
                assertThat(breaker.getUsed(), greaterThan(baseline));
            }
        }

        assertThat(breaker.getUsed(), equalTo(baseline));
    }

    public void testTermsIndexBreakerBytesAreReleasedOnClose() throws Exception {
        NumberFieldMapper.NumberType type = NumberFieldMapper.NumberType.LONG;
        MappedFieldType fieldType = indexTermsFieldType(type);
        CircuitBreakerService breakerService = LimitedBreaker.service(CircuitBreaker.REQUEST, ByteSizeValue.ofMb(64));
        CircuitBreaker breaker = breakerService.getBreaker(CircuitBreaker.REQUEST);
        long baseline = breaker.getUsed();
        try (Directory directory = newDirectory(); RandomIndexWriter writer = new RandomIndexWriter(random(), directory)) {
            for (int i = 0; i < 20_000; i++) {
                writer.addDocument(termsDocument(type, ((long) i << 32) | i));
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
                assertTrue(((RoaringBitmapAggregator) aggregator).usesTermsIndex());
                aggregator.preCollection();
                context.searcher().search(context.query(), aggregator.asCollector());
                aggregator.postCollection();
                aggregator.buildTopLevel();
                assertThat(breaker.getUsed(), greaterThan(baseline));
            }
        }

        assertThat(breaker.getUsed(), equalTo(baseline));
    }

    public void testIntegerBitmapRejectsValuesAboveIntegerRange() {
        InternalRoaringBitmap.MutableBitmap bitmap = InternalRoaringBitmap.mutable(InternalRoaringBitmap.BitmapFormat.INT);
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> bitmap.add(1L + Integer.MAX_VALUE));
        assertThat(exception.getMessage(), containsString("integer field produced out-of-range value"));
    }

    // This checks that the reservation rate stays clear of ramBytesUsed()'s own worst-case growth for
    // these patterns, with real margin -- not that ramBytesUsed() itself tracks true JVM heap use. That
    // latter guarantee comes from the overhead-correction factors on IntMutableBitmap/LongMutableBitmap
    // #ramBytesUsed, calibrated against JVM heap measurements from code review, which this in-JVM test
    // has no portable way to re-verify independently.
    public void testBreakerReservationsCoverWorstCaseContainerGrowth() {
        assertReservationCoversReportedGrowth(
            InternalRoaringBitmap.BitmapFormat.INT,
            RoaringBitmapAggregator.INT_BYTES_PER_VALUE,
            value -> value << 16
        );
        assertReservationCoversReportedGrowth(
            InternalRoaringBitmap.BitmapFormat.LONG,
            RoaringBitmapAggregator.LONG_BYTES_PER_VALUE,
            value -> value << 32
        );
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

    // The reduce path must reserve before deserializing, not after, so a bitmap too large for the
    // breaker is refused instead of being fully allocated and only then accounted for. Asserting that
    // the trip requested exactly the pre-deserialization estimate is what distinguishes the two
    // orderings: a post-deserialization trip would request the deserialized ramBytesUsed() instead.
    public void testReduceTripsBreakerBeforeDeserializing() throws Exception {
        InternalRoaringBitmap result = aggregate(NumberFieldMapper.NumberType.LONG, sparseLongValues(2_000));
        long expectedReservation = result.bitmap().length * InternalRoaringBitmap.DESERIALIZATION_EXPANSION_FACTOR;
        assertThat(expectedReservation, greaterThan(0L));

        CircuitBreakerService breakerService = LimitedBreaker.service(
            CircuitBreaker.REQUEST,
            ByteSizeValue.ofBytes(expectedReservation - 1)
        );
        AggregationReduceContext reduceContext = new AggregationReduceContext.ForFinal(
            new BigArrays(null, breakerService, CircuitBreaker.REQUEST),
            null,
            () -> false,
            AggregatorFactories.builder(),
            ignored -> {},
            null
        );

        try (AggregatorReducer reducer = result.getReducer(reduceContext, 1)) {
            CircuitBreakingException exception = expectThrows(CircuitBreakingException.class, () -> reducer.accept(result));
            assertThat(exception.getBytesWanted(), equalTo(expectedReservation));
        }
        // The refused reservation must not be left charged to the breaker.
        assertThat(breakerService.getBreaker(CircuitBreaker.REQUEST).getUsed(), equalTo(0L));
    }

    private static long[] sparseLongValues(int count) {
        long[] values = new long[count];
        for (int i = 0; i < count; i++) {
            // Spread across high words so the bitmap stays sparse rather than collapsing into runs.
            values[i] = ((long) i << 32) | i;
        }
        return values;
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

    private static void assertReservationCoversReportedGrowth(
        InternalRoaringBitmap.BitmapFormat width,
        long estimatedBytesPerValue,
        LongUnaryOperator value
    ) {
        InternalRoaringBitmap.MutableBitmap bitmap = InternalRoaringBitmap.mutable(width);
        long initialBytes = bitmap.ramBytesUsed();
        for (int i = 0; i < RoaringBitmapAggregator.BREAKER_RESERVATION_VALUES; i++) {
            bitmap.add(value.applyAsLong(i));
        }
        long reportedGrowth = bitmap.ramBytesUsed() - initialBytes;
        long reservedBytes = estimatedBytesPerValue * RoaringBitmapAggregator.BREAKER_RESERVATION_VALUES;
        assertThat(reportedGrowth, lessThanOrEqualTo(reservedBytes));
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
