/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.histogram;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.metrics.Min;
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class NumericHistogramAggregatorTests extends AggregatorTestCase {

    public void testLongs() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (long value : new long[] { 7, 3, -10, -6, 5, 15 }) {
                Document doc = new Document();
                doc.add(new SortedNumericDocValuesField("field", value));
                w.addDocument(doc);
            }

            HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg").field("field").interval(5);
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalHistogram histogram = searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, longField("field"));
                assertEquals(6, histogram.getBuckets().size());
                assertEquals(-10d, histogram.getBuckets().get(0).getKey());
                assertEquals(2, histogram.getBuckets().get(0).getDocCount());
                assertEquals(-5d, histogram.getBuckets().get(1).getKey());
                assertEquals(0, histogram.getBuckets().get(1).getDocCount());
                assertEquals(0d, histogram.getBuckets().get(2).getKey());
                assertEquals(1, histogram.getBuckets().get(2).getDocCount());
                assertEquals(5d, histogram.getBuckets().get(3).getKey());
                assertEquals(2, histogram.getBuckets().get(3).getDocCount());
                assertEquals(10d, histogram.getBuckets().get(4).getKey());
                assertEquals(0, histogram.getBuckets().get(4).getDocCount());
                assertEquals(15d, histogram.getBuckets().get(5).getKey());
                assertEquals(1, histogram.getBuckets().get(5).getDocCount());
                assertTrue(AggregationInspectionHelper.hasValue(histogram));
            }
        }
    }

    public void testDoubles() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (double value : new double[] { 9.3, 3.2, -10, -6.5, 5.3, 15.1 }) {
                Document doc = new Document();
                doc.add(new SortedNumericDocValuesField("field", NumericUtils.doubleToSortableLong(value)));
                w.addDocument(doc);
            }

            HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg").field("field").interval(5);
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalHistogram histogram = searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, doubleField("field"));
                assertEquals(6, histogram.getBuckets().size());
                assertEquals(6, histogram.getBuckets().size());
                assertEquals(-10d, histogram.getBuckets().get(0).getKey());
                assertEquals(2, histogram.getBuckets().get(0).getDocCount());
                assertEquals(-5d, histogram.getBuckets().get(1).getKey());
                assertEquals(0, histogram.getBuckets().get(1).getDocCount());
                assertEquals(0d, histogram.getBuckets().get(2).getKey());
                assertEquals(1, histogram.getBuckets().get(2).getDocCount());
                assertEquals(5d, histogram.getBuckets().get(3).getKey());
                assertEquals(2, histogram.getBuckets().get(3).getDocCount());
                assertEquals(10d, histogram.getBuckets().get(4).getKey());
                assertEquals(0, histogram.getBuckets().get(4).getDocCount());
                assertEquals(15d, histogram.getBuckets().get(5).getKey());
                assertEquals(1, histogram.getBuckets().get(5).getDocCount());
                assertTrue(AggregationInspectionHelper.hasValue(histogram));
            }
        }
    }

    public void testDates() throws Exception {
        List<String> dataset = Arrays.asList(
            "2019-11-01T01:07:45",
            "2019-11-02T03:43:34",
            "2019-11-03T04:11:00",
            "2019-11-04T05:11:31",
            "2019-11-05T08:24:05",
            "2019-11-06T13:09:32",
            "2019-11-07T13:47:43",
            "2019-11-08T16:14:34",
            "2019-11-09T17:09:50",
            "2019-11-10T22:55:46"
        );

        String fieldName = "date_field";
        DateFieldMapper.DateFieldType fieldType = dateField(fieldName, DateFieldMapper.Resolution.MILLISECONDS);

        try (Directory dir = newDirectory(); RandomIndexWriter indexWriter = new RandomIndexWriter(random(), dir)) {
            Document document = new Document();
            for (String date : dataset) {
                long instant = fieldType.parse(date);
                document.add(new SortedNumericDocValuesField(fieldName, instant));
                indexWriter.addDocument(document);
                document.clear();
            }

            HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg").field(fieldName)
                .interval(1000 * 60 * 60 * 24);
            try (IndexReader reader = indexWriter.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalHistogram histogram = searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType);
                assertTrue(AggregationInspectionHelper.hasValue(histogram));
            }
        }
    }

    public void testIrrationalInterval() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (long value : new long[] { 3, 2, -10, 5, -9 }) {
                Document doc = new Document();
                doc.add(new SortedNumericDocValuesField("field", value));
                w.addDocument(doc);
            }

            HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg").field("field").interval(Math.PI);
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalHistogram histogram = searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, longField("field"));
                assertEquals(6, histogram.getBuckets().size());
                assertEquals(-4 * Math.PI, histogram.getBuckets().get(0).getKey());
                assertEquals(1, histogram.getBuckets().get(0).getDocCount());
                assertEquals(-3 * Math.PI, histogram.getBuckets().get(1).getKey());
                assertEquals(1, histogram.getBuckets().get(1).getDocCount());
                assertEquals(-2 * Math.PI, histogram.getBuckets().get(2).getKey());
                assertEquals(0, histogram.getBuckets().get(2).getDocCount());
                assertEquals(-Math.PI, histogram.getBuckets().get(3).getKey());
                assertEquals(0, histogram.getBuckets().get(3).getDocCount());
                assertEquals(0d, histogram.getBuckets().get(4).getKey());
                assertEquals(2, histogram.getBuckets().get(4).getDocCount());
                assertEquals(Math.PI, histogram.getBuckets().get(5).getKey());
                assertEquals(1, histogram.getBuckets().get(5).getDocCount());
                assertTrue(AggregationInspectionHelper.hasValue(histogram));
            }
        }
    }

    public void testMinDocCount() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (long value : new long[] { 7, 3, -10, -6, 5, 50 }) {
                Document doc = new Document();
                doc.add(new SortedNumericDocValuesField("field", value));
                w.addDocument(doc);
            }

            HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg").field("field").interval(10).minDocCount(2);
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalHistogram histogram = searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, longField("field"));
                assertEquals(2, histogram.getBuckets().size());
                assertEquals(-10d, histogram.getBuckets().get(0).getKey());
                assertEquals(2, histogram.getBuckets().get(0).getDocCount());
                assertEquals(0d, histogram.getBuckets().get(1).getKey());
                assertEquals(3, histogram.getBuckets().get(1).getDocCount());
                assertTrue(AggregationInspectionHelper.hasValue(histogram));
            }
        }
    }

    public void testMissing() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (long value : new long[] { 7, 3, -10, -6, 5, 15 }) {
                Document doc = new Document();
                doc.add(new SortedNumericDocValuesField("field", value));
                w.addDocument(doc);
                w.addDocument(new Document());
            }

            HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg").field("field").interval(5).missing(2d);
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalHistogram histogram = searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, longField("field"));
                assertEquals(6, histogram.getBuckets().size());
                assertEquals(-10d, histogram.getBuckets().get(0).getKey());
                assertEquals(2, histogram.getBuckets().get(0).getDocCount());
                assertEquals(-5d, histogram.getBuckets().get(1).getKey());
                assertEquals(0, histogram.getBuckets().get(1).getDocCount());
                assertEquals(0d, histogram.getBuckets().get(2).getKey());
                assertEquals(7, histogram.getBuckets().get(2).getDocCount());
                assertEquals(5d, histogram.getBuckets().get(3).getKey());
                assertEquals(2, histogram.getBuckets().get(3).getDocCount());
                assertEquals(10d, histogram.getBuckets().get(4).getKey());
                assertEquals(0, histogram.getBuckets().get(4).getDocCount());
                assertEquals(15d, histogram.getBuckets().get(5).getKey());
                assertEquals(1, histogram.getBuckets().get(5).getDocCount());
                assertTrue(AggregationInspectionHelper.hasValue(histogram));
            }
        }
    }

    public void testMissingUnmappedField() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (int i = 0; i < 7; i++) {
                Document doc = new Document();
                w.addDocument(doc);
            }

            HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg").field("field").interval(5).missing(2d);
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalHistogram histogram = searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder);

                assertEquals(1, histogram.getBuckets().size());

                assertEquals(0d, histogram.getBuckets().get(0).getKey());
                assertEquals(7, histogram.getBuckets().get(0).getDocCount());

                assertTrue(AggregationInspectionHelper.hasValue(histogram));
            }
        }
    }

    public void testMissingUnmappedFieldBadType() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (int i = 0; i < 7; i++) {
                w.addDocument(new Document());
            }

            String missingValue = "🍌🍌🍌";
            HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg").field("field")
                .interval(5)
                .missing(missingValue);
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                Throwable t = expectThrows(
                    IllegalArgumentException.class,
                    () -> { searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder); }
                );
                // This throws a number format exception (which is a subclass of IllegalArgumentException) and might be ok?
                assertThat(t.getMessage(), containsString(missingValue));
            }
        }
    }

    public void testIncorrectFieldType() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (String value : new String[] { "foo", "bar", "baz", "quux" }) {
                Document doc = new Document();
                doc.add(new SortedSetDocValuesField("field", new BytesRef(value)));
                w.addDocument(doc);
            }

            HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg").field("field").interval(5);
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);

                expectThrows(
                    IllegalArgumentException.class,
                    () -> { searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, keywordField("field")); }
                );
            }
        }

    }

    public void testOffset() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (double value : new double[] { 9.3, 3.2, -5, -6.5, 5.3 }) {
                Document doc = new Document();
                doc.add(new SortedNumericDocValuesField("field", NumericUtils.doubleToSortableLong(value)));
                w.addDocument(doc);
            }

            HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg").field("field").interval(5).offset(Math.PI);
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalHistogram histogram = searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, doubleField("field"));
                assertEquals(4, histogram.getBuckets().size());
                assertEquals(-10 + Math.PI, histogram.getBuckets().get(0).getKey());
                assertEquals(2, histogram.getBuckets().get(0).getDocCount());
                assertEquals(-5 + Math.PI, histogram.getBuckets().get(1).getKey());
                assertEquals(0, histogram.getBuckets().get(1).getDocCount());
                assertEquals(Math.PI, histogram.getBuckets().get(2).getKey());
                assertEquals(2, histogram.getBuckets().get(2).getDocCount());
                assertEquals(5 + Math.PI, histogram.getBuckets().get(3).getKey());
                assertEquals(1, histogram.getBuckets().get(3).getDocCount());
                assertTrue(AggregationInspectionHelper.hasValue(histogram));
            }
        }
    }

    public void testRandomOffset() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            // Note, these values are carefully chosen to ensure that no matter what offset we pick, no two can end up in the same bucket
            for (double value : new double[] { 9.3, 3.2, -5 }) {
                Document doc = new Document();
                doc.add(new SortedNumericDocValuesField("field", NumericUtils.doubleToSortableLong(value)));
                w.addDocument(doc);
            }

            final double offset = randomDouble();
            final double interval = 5;
            final double expectedOffset = offset % interval;
            HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg").field("field")
                .interval(interval)
                .offset(offset);
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalHistogram histogram = searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, doubleField("field"));
                assertEquals(4, histogram.getBuckets().size());

                assertEquals(-10 + expectedOffset, histogram.getBuckets().get(0).getKey());
                assertEquals(1, histogram.getBuckets().get(0).getDocCount());

                assertEquals(-5 + expectedOffset, histogram.getBuckets().get(1).getKey());
                assertEquals(0, histogram.getBuckets().get(1).getDocCount());

                assertEquals(expectedOffset, histogram.getBuckets().get(2).getKey());
                assertEquals(1, histogram.getBuckets().get(2).getDocCount());

                assertEquals(5 + expectedOffset, histogram.getBuckets().get(3).getKey());
                assertEquals(1, histogram.getBuckets().get(3).getDocCount());

                assertTrue(AggregationInspectionHelper.hasValue(histogram));
            }
        }
    }

    public void testExtendedBounds() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (double value : new double[] { 3.2, -5, -4.5, 4.3 }) {
                Document doc = new Document();
                doc.add(new SortedNumericDocValuesField("field", NumericUtils.doubleToSortableLong(value)));
                w.addDocument(doc);
            }

            HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg").field("field")
                .interval(5)
                .extendedBounds(-12, 13);
            MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("field", NumberFieldMapper.NumberType.DOUBLE);
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalHistogram histogram = searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, doubleField("field"));
                assertEquals(6, histogram.getBuckets().size());
                assertEquals(-15d, histogram.getBuckets().get(0).getKey());
                assertEquals(0, histogram.getBuckets().get(0).getDocCount());
                assertEquals(-10d, histogram.getBuckets().get(1).getKey());
                assertEquals(0, histogram.getBuckets().get(1).getDocCount());
                assertEquals(-5d, histogram.getBuckets().get(2).getKey());
                assertEquals(2, histogram.getBuckets().get(2).getDocCount());
                assertEquals(0d, histogram.getBuckets().get(3).getKey());
                assertEquals(2, histogram.getBuckets().get(3).getDocCount());
                assertEquals(5d, histogram.getBuckets().get(4).getKey());
                assertEquals(0, histogram.getBuckets().get(4).getDocCount());
                assertEquals(10d, histogram.getBuckets().get(5).getKey());
                assertEquals(0, histogram.getBuckets().get(5).getDocCount());
                assertTrue(AggregationInspectionHelper.hasValue(histogram));
            }
        }
    }

    public void testHardBounds() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (double value : new double[] { 3.2, -5, -4.5, 4.3 }) {
                Document doc = new Document();
                doc.add(new SortedNumericDocValuesField("field", NumericUtils.doubleToSortableLong(value)));
                w.addDocument(doc);
            }

            HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg").field("field")
                .interval(5)
                .hardBounds(new DoubleBounds(0.0, 10.0));
            MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("field", NumberFieldMapper.NumberType.DOUBLE);
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalHistogram histogram = searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType);
                assertEquals(1, histogram.getBuckets().size());
                assertEquals(0d, histogram.getBuckets().get(0).getKey());
                assertEquals(2, histogram.getBuckets().get(0).getDocCount());
                assertTrue(AggregationInspectionHelper.hasValue(histogram));
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
                docs.add(
                    List.of(
                        new SortedNumericDocValuesField("outer", n % 100),
                        new SortedNumericDocValuesField("inner", n / 100),
                        new SortedNumericDocValuesField("n", n)
                    )
                );
            }
            iw.addDocuments(docs);
        };
        Consumer<InternalHistogram> verify = outer -> {
            assertThat(outer.getBuckets(), hasSize(20));
            for (int outerIdx = 0; outerIdx < 20; outerIdx++) {
                InternalHistogram.Bucket outerBucket = outer.getBuckets().get(outerIdx);
                assertThat(outerBucket.getKey(), equalTo(5.0 * outerIdx));
                InternalHistogram inner = outerBucket.getAggregations().get("inner");
                assertThat(inner.getBuckets(), hasSize(20));
                for (int innerIdx = 0; innerIdx < 20; innerIdx++) {
                    InternalHistogram.Bucket innerBucket = inner.getBuckets().get(innerIdx);
                    assertThat(innerBucket.getKey(), equalTo(5.0 * innerIdx));
                    Min min = innerBucket.getAggregations().get("min");
                    assertThat(min.value(), equalTo(outerIdx * 5.0 + innerIdx * 500.0));
                }
            }
        };
        testCase(request, new MatchAllDocsQuery(), buildIndex, verify, longField("outer"), longField("inner"), longField("n"));
    }
}
