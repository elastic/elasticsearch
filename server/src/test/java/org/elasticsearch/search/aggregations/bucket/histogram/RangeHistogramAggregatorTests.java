/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.histogram;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.mapper.RangeFieldMapper;
import org.elasticsearch.index.mapper.RangeType;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.metrics.InternalMin;
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class RangeHistogramAggregatorTests extends AggregatorTestCase {
    public void testDoubles() throws Exception {
        RangeType rangeType = RangeType.DOUBLE;
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (RangeFieldMapper.Range range : new RangeFieldMapper.Range[] {
                new RangeFieldMapper.Range(rangeType, 1.0D, 5.0D, true, true), // bucket 0 5
                new RangeFieldMapper.Range(rangeType, -3.1, 4.2, true, true), // bucket -5, 0
                new RangeFieldMapper.Range(rangeType, 4.2, 13.3, true, true), // bucket 0, 5, 10
                new RangeFieldMapper.Range(rangeType, 22.5, 29.3, true, true), // bucket 20, 25
            }) {
                Document doc = new Document();
                BytesRef encodedRange = rangeType.encodeRanges(Collections.singleton(range));
                doc.add(new BinaryDocValuesField("field", encodedRange));
                w.addDocument(doc);
            }

            HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg").field("field").interval(5);

            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalHistogram histogram = searchAndReduce(
                    searcher,
                    new MatchAllDocsQuery(),
                    aggBuilder,
                    rangeField("field", rangeType)
                );
                assertEquals(7, histogram.getBuckets().size());

                assertEquals(-5d, histogram.getBuckets().get(0).getKey());
                assertEquals(1, histogram.getBuckets().get(0).getDocCount());

                assertEquals(0d, histogram.getBuckets().get(1).getKey());
                assertEquals(3, histogram.getBuckets().get(1).getDocCount());

                assertEquals(5d, histogram.getBuckets().get(2).getKey());
                assertEquals(2, histogram.getBuckets().get(2).getDocCount());

                assertEquals(10d, histogram.getBuckets().get(3).getKey());
                assertEquals(1, histogram.getBuckets().get(3).getDocCount());

                assertEquals(15d, histogram.getBuckets().get(4).getKey());
                assertEquals(0, histogram.getBuckets().get(4).getDocCount());

                assertEquals(20d, histogram.getBuckets().get(5).getKey());
                assertEquals(1, histogram.getBuckets().get(5).getDocCount());

                assertEquals(25d, histogram.getBuckets().get(6).getKey());
                assertEquals(1, histogram.getBuckets().get(6).getDocCount());
            }
        }
    }

    public void testLongs() throws Exception {
        RangeType rangeType = RangeType.LONG;
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (RangeFieldMapper.Range range : new RangeFieldMapper.Range[] {
                new RangeFieldMapper.Range(rangeType, 1L, 5L, true, true), // bucket 0 5
                new RangeFieldMapper.Range(rangeType, -3L, 4L, true, true), // bucket -5, 0
                new RangeFieldMapper.Range(rangeType, 4L, 13L, true, true), // bucket 0, 5, 10
                new RangeFieldMapper.Range(rangeType, 22L, 29L, true, true), // bucket 20, 25
            }) {
                Document doc = new Document();
                BytesRef encodedRange = rangeType.encodeRanges(Collections.singleton(range));
                doc.add(new BinaryDocValuesField("field", encodedRange));
                w.addDocument(doc);
            }

            HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg").field("field").interval(5);

            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalHistogram histogram = searchAndReduce(
                    searcher,
                    new MatchAllDocsQuery(),
                    aggBuilder,
                    rangeField("field", rangeType)
                );
                assertEquals(7, histogram.getBuckets().size());

                assertEquals(-5d, histogram.getBuckets().get(0).getKey());
                assertEquals(1, histogram.getBuckets().get(0).getDocCount());

                assertEquals(0d, histogram.getBuckets().get(1).getKey());
                assertEquals(3, histogram.getBuckets().get(1).getDocCount());

                assertEquals(5d, histogram.getBuckets().get(2).getKey());
                assertEquals(2, histogram.getBuckets().get(2).getDocCount());

                assertEquals(10d, histogram.getBuckets().get(3).getKey());
                assertEquals(1, histogram.getBuckets().get(3).getDocCount());

                assertEquals(15d, histogram.getBuckets().get(4).getKey());
                assertEquals(0, histogram.getBuckets().get(4).getDocCount());

                assertEquals(20d, histogram.getBuckets().get(5).getKey());
                assertEquals(1, histogram.getBuckets().get(5).getDocCount());

                assertEquals(25d, histogram.getBuckets().get(6).getKey());
                assertEquals(1, histogram.getBuckets().get(6).getDocCount());
            }
        }
    }

    public void testMultipleRanges() throws Exception {
        RangeType rangeType = RangeType.LONG;
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            Document doc = new Document();
            BytesRef encodedRange = rangeType.encodeRanges(
                Set.of(
                    new RangeFieldMapper.Range(rangeType, 1L, 5L, true, true), // bucket 0 5
                    new RangeFieldMapper.Range(rangeType, -3L, 4L, true, true), // bucket -5, 0
                    new RangeFieldMapper.Range(rangeType, 4L, 13L, true, true), // bucket 0, 5, 10
                    new RangeFieldMapper.Range(rangeType, 22L, 29L, true, true) // bucket 20, 25, 30
                )
            );
            doc.add(new BinaryDocValuesField("field", encodedRange));
            w.addDocument(doc);

            HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg").field("field").interval(5);

            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalHistogram histogram = searchAndReduce(
                    searcher,
                    new MatchAllDocsQuery(),
                    aggBuilder,
                    rangeField("field", rangeType)
                );
                assertEquals(7, histogram.getBuckets().size());

                assertEquals(-5d, histogram.getBuckets().get(0).getKey());
                assertEquals(1, histogram.getBuckets().get(0).getDocCount());

                assertEquals(0d, histogram.getBuckets().get(1).getKey());
                assertEquals(1, histogram.getBuckets().get(1).getDocCount());

                assertEquals(5d, histogram.getBuckets().get(2).getKey());
                assertEquals(1, histogram.getBuckets().get(2).getDocCount());

                assertEquals(10d, histogram.getBuckets().get(3).getKey());
                assertEquals(1, histogram.getBuckets().get(3).getDocCount());

                assertEquals(15d, histogram.getBuckets().get(4).getKey());
                assertEquals(0, histogram.getBuckets().get(4).getDocCount());

                assertEquals(20d, histogram.getBuckets().get(5).getKey());
                assertEquals(1, histogram.getBuckets().get(5).getDocCount());

                assertEquals(25d, histogram.getBuckets().get(6).getKey());
                assertEquals(1, histogram.getBuckets().get(6).getDocCount());
            }
        }

    }

    public void testMultipleRangesLotsOfOverlap() throws Exception {
        RangeType rangeType = RangeType.LONG;
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            Document doc = new Document();
            BytesRef encodedRange = rangeType.encodeRanges(
                Set.of(
                    new RangeFieldMapper.Range(rangeType, 1L, 2L, true, true), // bucket 0
                    new RangeFieldMapper.Range(rangeType, 1L, 4L, true, true), // bucket 0
                    new RangeFieldMapper.Range(rangeType, 1L, 13L, true, true), // bucket 0, 5, 10
                    new RangeFieldMapper.Range(rangeType, 1L, 5L, true, true) // bucket 0, 5
                )
            );
            doc.add(new BinaryDocValuesField("field", encodedRange));
            w.addDocument(doc);

            HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg").field("field").interval(5);

            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalHistogram histogram = searchAndReduce(
                    searcher,
                    new MatchAllDocsQuery(),
                    aggBuilder,
                    rangeField("field", rangeType)
                );
                assertEquals(3, histogram.getBuckets().size());

                assertEquals(0d, histogram.getBuckets().get(0).getKey());
                assertEquals(1, histogram.getBuckets().get(0).getDocCount());

                assertEquals(5d, histogram.getBuckets().get(1).getKey());
                assertEquals(1, histogram.getBuckets().get(1).getDocCount());

                assertEquals(10d, histogram.getBuckets().get(2).getKey());
                assertEquals(1, histogram.getBuckets().get(2).getDocCount());
            }
        }

    }

    public void testLongsIrrationalInterval() throws Exception {
        RangeType rangeType = RangeType.LONG;
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (RangeFieldMapper.Range range : new RangeFieldMapper.Range[] {
                new RangeFieldMapper.Range(rangeType, 1L, 5L, true, true), // bucket 0 5
                new RangeFieldMapper.Range(rangeType, -3L, 4L, true, true), // bucket -5, 0
                new RangeFieldMapper.Range(rangeType, 4L, 13L, true, true), // bucket 0, 5, 10
            }) {
                Document doc = new Document();
                BytesRef encodedRange = rangeType.encodeRanges(Collections.singleton(range));
                doc.add(new BinaryDocValuesField("field", encodedRange));
                w.addDocument(doc);
            }

            HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg").field("field").interval(Math.PI);

            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalHistogram histogram = searchAndReduce(
                    searcher,
                    new MatchAllDocsQuery(),
                    aggBuilder,
                    rangeField("field", rangeType)
                );
                assertEquals(6, histogram.getBuckets().size());

                assertEquals(-1 * Math.PI, histogram.getBuckets().get(0).getKey());
                assertEquals(1, histogram.getBuckets().get(0).getDocCount());

                assertEquals(0 * Math.PI, histogram.getBuckets().get(1).getKey());
                assertEquals(2, histogram.getBuckets().get(1).getDocCount());

                assertEquals(1 * Math.PI, histogram.getBuckets().get(2).getKey());
                assertEquals(3, histogram.getBuckets().get(2).getDocCount());

                assertEquals(2 * Math.PI, histogram.getBuckets().get(3).getKey());
                assertEquals(1, histogram.getBuckets().get(3).getDocCount());

                assertEquals(3 * Math.PI, histogram.getBuckets().get(4).getKey());
                assertEquals(1, histogram.getBuckets().get(4).getDocCount());

                assertEquals(4 * Math.PI, histogram.getBuckets().get(5).getKey());
                assertEquals(1, histogram.getBuckets().get(5).getDocCount());
            }
        }
    }

    public void testMinDocCount() throws Exception {
        RangeType rangeType = RangeType.LONG;
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (RangeFieldMapper.Range range : new RangeFieldMapper.Range[] {
                new RangeFieldMapper.Range(rangeType, -14L, -11L, true, true), // bucket -15
                new RangeFieldMapper.Range(rangeType, 0L, 9L, true, true), // bucket 0, 5
                new RangeFieldMapper.Range(rangeType, 6L, 12L, true, true), // bucket 5, 10
                new RangeFieldMapper.Range(rangeType, 13L, 14L, true, true), // bucket 10
            }) {
                Document doc = new Document();
                BytesRef encodedRange = rangeType.encodeRanges(Collections.singleton(range));
                doc.add(new BinaryDocValuesField("field", encodedRange));
                w.addDocument(doc);
            }

            HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg").field("field").interval(5).minDocCount(2);

            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalHistogram histogram = searchAndReduce(
                    searcher,
                    new MatchAllDocsQuery(),
                    aggBuilder,
                    rangeField("field", rangeType)
                );
                assertEquals(2, histogram.getBuckets().size());

                assertEquals(5d, histogram.getBuckets().get(0).getKey());
                assertEquals(2, histogram.getBuckets().get(0).getDocCount());

                assertEquals(10d, histogram.getBuckets().get(1).getKey());
                assertEquals(2, histogram.getBuckets().get(1).getDocCount());
            }
        }
    }

    public void testOffset() throws Exception {
        RangeType rangeType = RangeType.DOUBLE;
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (RangeFieldMapper.Range range : new RangeFieldMapper.Range[] {
                new RangeFieldMapper.Range(rangeType, 1.0D, 5.0D, true, true), // bucket -1, 4
                new RangeFieldMapper.Range(rangeType, -3.1, 4.2, true, true), // bucket -6 -1 4
                new RangeFieldMapper.Range(rangeType, 4.2, 13.3, true, true), // bucket 4, 9
                new RangeFieldMapper.Range(rangeType, 22.5, 29.3, true, true), // bucket 19, 24, 29
            }) {
                Document doc = new Document();
                BytesRef encodedRange = rangeType.encodeRanges(Collections.singleton(range));
                doc.add(new BinaryDocValuesField("field", encodedRange));
                w.addDocument(doc);
            }

            HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg").field("field").interval(5).offset(4);

            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalHistogram histogram = searchAndReduce(
                    searcher,
                    new MatchAllDocsQuery(),
                    aggBuilder,
                    rangeField("field", rangeType)
                );
                assertEquals(8, histogram.getBuckets().size());

                assertEquals(-6d, histogram.getBuckets().get(0).getKey());
                assertEquals(1, histogram.getBuckets().get(0).getDocCount());

                assertEquals(-1d, histogram.getBuckets().get(1).getKey());
                assertEquals(2, histogram.getBuckets().get(1).getDocCount());

                assertEquals(4d, histogram.getBuckets().get(2).getKey());
                assertEquals(3, histogram.getBuckets().get(2).getDocCount());

                assertEquals(9d, histogram.getBuckets().get(3).getKey());
                assertEquals(1, histogram.getBuckets().get(3).getDocCount());

                assertEquals(14d, histogram.getBuckets().get(4).getKey());
                assertEquals(0, histogram.getBuckets().get(4).getDocCount());

                assertEquals(19d, histogram.getBuckets().get(5).getKey());
                assertEquals(1, histogram.getBuckets().get(5).getDocCount());

                assertEquals(24d, histogram.getBuckets().get(6).getKey());
                assertEquals(1, histogram.getBuckets().get(6).getDocCount());

                assertEquals(29d, histogram.getBuckets().get(7).getKey());
                assertEquals(1, histogram.getBuckets().get(7).getDocCount());
            }
        }
    }

    public void testOffsetGtInterval() throws Exception {
        RangeType rangeType = RangeType.DOUBLE;
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (RangeFieldMapper.Range range : new RangeFieldMapper.Range[] {
                new RangeFieldMapper.Range(rangeType, 1.0D, 5.0D, true, true), // bucket 0 5
                new RangeFieldMapper.Range(rangeType, -3.1, 4.2, true, true), // bucket -5, 0
                new RangeFieldMapper.Range(rangeType, 4.2, 13.3, true, true), // bucket 0, 5, 10
                new RangeFieldMapper.Range(rangeType, 22.5, 29.3, true, true), // bucket 20, 25
            }) {
                Document doc = new Document();
                BytesRef encodedRange = rangeType.encodeRanges(Collections.singleton(range));
                doc.add(new BinaryDocValuesField("field", encodedRange));
                w.addDocument(doc);
            }

            // I'd like to randomize the offset here, like I did in the test for the numeric side, but there's no way I can think of to
            // construct the intervals such that they wouldn't "slosh" between buckets.
            final double offset = 20;
            final double interval = 5;
            final double expectedOffset = offset % interval;

            HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg").field("field")
                .interval(interval)
                .offset(offset);

            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalHistogram histogram = searchAndReduce(
                    searcher,
                    new MatchAllDocsQuery(),
                    aggBuilder,
                    rangeField("field", rangeType)
                );
                assertEquals(7, histogram.getBuckets().size());

                assertEquals(-5d + expectedOffset, histogram.getBuckets().get(0).getKey());
                assertEquals(1, histogram.getBuckets().get(0).getDocCount());

                assertEquals(0d + expectedOffset, histogram.getBuckets().get(1).getKey());
                assertEquals(3, histogram.getBuckets().get(1).getDocCount());

                assertEquals(5d + expectedOffset, histogram.getBuckets().get(2).getKey());
                assertEquals(2, histogram.getBuckets().get(2).getDocCount());

                assertEquals(10d + expectedOffset, histogram.getBuckets().get(3).getKey());
                assertEquals(1, histogram.getBuckets().get(3).getDocCount());

                assertEquals(15d + expectedOffset, histogram.getBuckets().get(4).getKey());
                assertEquals(0, histogram.getBuckets().get(4).getDocCount());

                assertEquals(20d + expectedOffset, histogram.getBuckets().get(5).getKey());
                assertEquals(1, histogram.getBuckets().get(5).getDocCount());

                assertEquals(25d + expectedOffset, histogram.getBuckets().get(6).getKey());
                assertEquals(1, histogram.getBuckets().get(6).getDocCount());
            }
        }
    }

    public void testIpRangesUnsupported() throws Exception {
        RangeType rangeType = RangeType.IP;
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            Document doc = new Document();
            BytesRef encodedRange = rangeType.encodeRanges(
                Collections.singleton(
                    new RangeFieldMapper.Range(
                        rangeType,
                        InetAddresses.forString("10.0.0.1"),
                        InetAddresses.forString("10.0.0.10"),
                        true,
                        true
                    )
                )
            );
            doc.add(new BinaryDocValuesField("field", encodedRange));
            w.addDocument(doc);

            HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg").field("field").interval(5);

            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                Exception e = expectThrows(
                    IllegalArgumentException.class,
                    () -> searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, rangeField("field", rangeType))
                );
                assertThat(e.getMessage(), equalTo("Expected numeric range type but found non-numeric range [ip_range]"));
            }
        }
    }

    public void testAsSubAgg() throws IOException {
        AggregationBuilder request = new HistogramAggregationBuilder("outer").field("outer")
            .interval(5)
            .subAggregation(
                new HistogramAggregationBuilder("inner").field("inner")
                    .interval(5)
                    .subAggregation(new MinAggregationBuilder("min").field("n"))
            );
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex = iw -> {
            List<List<IndexableField>> docs = new ArrayList<>();
            for (int n = 0; n < 10000; n++) {
                BytesRef outerRange = RangeType.LONG.encodeRanges(
                    Set.of(new RangeFieldMapper.Range(RangeType.LONG, n % 100, n % 100 + 10, true, true))
                );
                BytesRef innerRange = RangeType.LONG.encodeRanges(
                    Set.of(new RangeFieldMapper.Range(RangeType.LONG, n / 100, n / 100 + 10, true, true))
                );

                docs.add(
                    List.of(
                        new BinaryDocValuesField("outer", outerRange),
                        new BinaryDocValuesField("inner", innerRange),
                        new SortedNumericDocValuesField("n", n)
                    )
                );
            }
            iw.addDocuments(docs);
        };
        Consumer<InternalHistogram> verify = outer -> {
            assertThat(outer.getBuckets(), hasSize(22));
            for (int outerIdx = 0; outerIdx < 22; outerIdx++) {
                InternalHistogram.Bucket outerBucket = outer.getBuckets().get(outerIdx);
                assertThat(outerBucket.getKey(), equalTo(5.0 * outerIdx));
                InternalHistogram inner = outerBucket.getAggregations().get("inner");
                assertThat(inner.getBuckets(), hasSize(22));
                for (int innerIdx = 0; innerIdx < 22; innerIdx++) {
                    InternalHistogram.Bucket innerBucket = inner.getBuckets().get(innerIdx);
                    assertThat(innerBucket.getKey(), equalTo(5.0 * innerIdx));
                    InternalMin min = innerBucket.getAggregations().get("min");
                    int minOuterIdxWithOverlappingRange = Math.max(0, outerIdx - 2);
                    int minInnerIdxWithOverlappingRange = Math.max(0, innerIdx - 2);
                    assertThat(min.getValue(), equalTo(minOuterIdxWithOverlappingRange * 5.0 + minInnerIdxWithOverlappingRange * 500.0));
                }
            }
        };
        testCase(
            request,
            new MatchAllDocsQuery(),
            buildIndex,
            verify,
            rangeField("outer", RangeType.LONG),
            rangeField("inner", RangeType.LONG),
            longField("n")
        );
    }
}
