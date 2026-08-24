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
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorReducer;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongUnaryOperator;

import static org.elasticsearch.test.InternalAggregationTestCase.DEFAULT_MAX_BUCKETS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class RoaringBitmapAggregatorTests extends AggregatorTestCase {

    private static final String FIELD = "id";

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

    public void testIntegerBitmapRejectsValuesAboveIntegerRange() {
        InternalRoaringBitmap.MutableBitmap bitmap = InternalRoaringBitmap.mutable(InternalRoaringBitmap.BitmapFormat.INT);
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> bitmap.add(1L + Integer.MAX_VALUE));
        assertThat(exception.getMessage(), containsString("integer field produced out-of-range value"));
    }

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
