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

package org.elasticsearch.search.aggregations.bucket.histogram;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.RangeFieldMapper;
import org.elasticsearch.index.mapper.RangeType;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.util.Collections;
import java.util.Set;

public class RangeHistogramAggregatorTests extends AggregatorTestCase {

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    public void testDoubles() throws Exception {
        RangeType rangeType = RangeType.DOUBLE;
        try (Directory dir = newDirectory();
             RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (RangeFieldMapper.Range range : new RangeFieldMapper.Range[] {
                new RangeFieldMapper.Range(rangeType, 1.0D, 5.0D, true, true), // bucket 0 5
                new RangeFieldMapper.Range(rangeType, -3.1, 4.2, true, true), // bucket -5, 0
                new RangeFieldMapper.Range(rangeType, 4.2, 13.3, true, true), // bucket 0, 5, 10
                new RangeFieldMapper.Range(rangeType, 42.5, 49.3, true, true), // bucket 40, 45
            }) {
                Document doc = new Document();
                BytesRef encodedRange = rangeType.encodeRanges(Collections.singleton(range));
                doc.add(new BinaryDocValuesField("field", encodedRange));
                w.addDocument(doc);
            }

            HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg")
                .field("field")
                .interval(5);
            MappedFieldType fieldType = new RangeFieldMapper.Builder("field", rangeType).fieldType();
            fieldType.setName("field");

            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalHistogram histogram = search(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType);
                assertEquals(6, histogram.getBuckets().size());

                assertEquals(-5d, histogram.getBuckets().get(0).getKey());
                assertEquals(1, histogram.getBuckets().get(0).getDocCount());

                assertEquals(0d, histogram.getBuckets().get(1).getKey());
                assertEquals(3, histogram.getBuckets().get(1).getDocCount());

                assertEquals(5d, histogram.getBuckets().get(2).getKey());
                assertEquals(2, histogram.getBuckets().get(2).getDocCount());

                assertEquals(10d, histogram.getBuckets().get(3).getKey());
                assertEquals(1, histogram.getBuckets().get(3).getDocCount());

                assertEquals(40d, histogram.getBuckets().get(4).getKey());
                assertEquals(1, histogram.getBuckets().get(4).getDocCount());

                assertEquals(45d, histogram.getBuckets().get(5).getKey());
                assertEquals(1, histogram.getBuckets().get(5).getDocCount());
            }
        }
    }

    public void testLongs() throws Exception {
        RangeType rangeType = RangeType.LONG;
        try (Directory dir = newDirectory();
             RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (RangeFieldMapper.Range range : new RangeFieldMapper.Range[] {
                new RangeFieldMapper.Range(rangeType, 1L, 5L, true, true), // bucket 0 5
                new RangeFieldMapper.Range(rangeType, -3L, 4L, true, true), // bucket -5, 0
                new RangeFieldMapper.Range(rangeType, 4L, 13L, true, true), // bucket 0, 5, 10
                new RangeFieldMapper.Range(rangeType, 42L, 49L, true, true), // bucket 40, 45
            }) {
                Document doc = new Document();
                BytesRef encodedRange = rangeType.encodeRanges(Collections.singleton(range));
                doc.add(new BinaryDocValuesField("field", encodedRange));
                w.addDocument(doc);
            }

            HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg")
                .field("field")
                .interval(5);
            MappedFieldType fieldType = new RangeFieldMapper.Builder("field", rangeType).fieldType();
            fieldType.setName("field");

            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalHistogram histogram = search(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType);
                assertEquals(6, histogram.getBuckets().size());

                assertEquals(-5d, histogram.getBuckets().get(0).getKey());
                assertEquals(1, histogram.getBuckets().get(0).getDocCount());

                assertEquals(0d, histogram.getBuckets().get(1).getKey());
                assertEquals(3, histogram.getBuckets().get(1).getDocCount());

                assertEquals(5d, histogram.getBuckets().get(2).getKey());
                assertEquals(2, histogram.getBuckets().get(2).getDocCount());

                assertEquals(10d, histogram.getBuckets().get(3).getKey());
                assertEquals(1, histogram.getBuckets().get(3).getDocCount());

                assertEquals(40d, histogram.getBuckets().get(4).getKey());
                assertEquals(1, histogram.getBuckets().get(4).getDocCount());

                assertEquals(45d, histogram.getBuckets().get(5).getKey());
                assertEquals(1, histogram.getBuckets().get(5).getDocCount());
            }
        }
    }

    public void testMultipleRanges() throws Exception {
        RangeType rangeType = RangeType.LONG;
        try (Directory dir = newDirectory();
             RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            Document doc = new Document();
            BytesRef encodedRange = rangeType.encodeRanges(Set.of(
                new RangeFieldMapper.Range(rangeType, 1L, 5L, true, true), // bucket 0 5
                new RangeFieldMapper.Range(rangeType, -3L, 4L, true, true), // bucket -5, 0
                new RangeFieldMapper.Range(rangeType, 4L, 13L, true, true), // bucket 0, 5, 10
                new RangeFieldMapper.Range(rangeType, 42L, 49L, true, true) // bucket 40, 45
            ));
            doc.add(new BinaryDocValuesField("field", encodedRange));
            w.addDocument(doc);

            HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg")
                .field("field")
                .interval(5);
            MappedFieldType fieldType = new RangeFieldMapper.Builder("field", rangeType).fieldType();
            fieldType.setName("field");

            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalHistogram histogram = search(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType);
                assertEquals(6, histogram.getBuckets().size());

                assertEquals(-5d, histogram.getBuckets().get(0).getKey());
                assertEquals(1, histogram.getBuckets().get(0).getDocCount());

                assertEquals(0d, histogram.getBuckets().get(1).getKey());
                assertEquals(1, histogram.getBuckets().get(1).getDocCount());

                assertEquals(5d, histogram.getBuckets().get(2).getKey());
                assertEquals(1, histogram.getBuckets().get(2).getDocCount());

                assertEquals(10d, histogram.getBuckets().get(3).getKey());
                assertEquals(1, histogram.getBuckets().get(3).getDocCount());

                assertEquals(40d, histogram.getBuckets().get(4).getKey());
                assertEquals(1, histogram.getBuckets().get(4).getDocCount());

                assertEquals(45d, histogram.getBuckets().get(5).getKey());
                assertEquals(1, histogram.getBuckets().get(5).getDocCount());
            }
        }

    }

    public void testMultipleRangesLotsOfOverlap() throws Exception {
        RangeType rangeType = RangeType.LONG;
        try (Directory dir = newDirectory();
             RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            Document doc = new Document();
            BytesRef encodedRange = rangeType.encodeRanges(Set.of(
                new RangeFieldMapper.Range(rangeType, 1L, 2L, true, true), // bucket 0
                new RangeFieldMapper.Range(rangeType, 1L, 4L, true, true), // bucket 0
                new RangeFieldMapper.Range(rangeType, 1L, 13L, true, true), // bucket 0, 5, 10
                new RangeFieldMapper.Range(rangeType, 1L, 5L, true, true) // bucket 0, 5
            ));
            doc.add(new BinaryDocValuesField("field", encodedRange));
            w.addDocument(doc);

            HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg")
                .field("field")
                .interval(5);
            MappedFieldType fieldType = new RangeFieldMapper.Builder("field", rangeType).fieldType();
            fieldType.setName("field");

            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalHistogram histogram = search(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType);
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
        try (Directory dir = newDirectory();
             RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
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

            HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg")
                .field("field")
                .interval(Math.PI);
            MappedFieldType fieldType = new RangeFieldMapper.Builder("field", rangeType).fieldType();
            fieldType.setName("field");

            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalHistogram histogram = search(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType);
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
        try (Directory dir = newDirectory();
             RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (RangeFieldMapper.Range range : new RangeFieldMapper.Range[] {
                new RangeFieldMapper.Range(rangeType, -14L, -11L, true, true), // bucket -15
                new RangeFieldMapper.Range(rangeType, 0L, 9L, true, true), // bucket 0, 5
                new RangeFieldMapper.Range(rangeType, 6L, 12L, true, true), // bucket  5, 10
                new RangeFieldMapper.Range(rangeType, 13L, 14L, true, true), // bucket 10
            }) {
                Document doc = new Document();
                BytesRef encodedRange = rangeType.encodeRanges(Collections.singleton(range));
                doc.add(new BinaryDocValuesField("field", encodedRange));
                w.addDocument(doc);
            }

            HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg")
                .field("field")
                .interval(5)
                .minDocCount(2);
            MappedFieldType fieldType = new RangeFieldMapper.Builder("field", rangeType).fieldType();
            fieldType.setName("field");

            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalHistogram histogram = searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType);
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
        try (Directory dir = newDirectory();
             RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (RangeFieldMapper.Range range : new RangeFieldMapper.Range[] {
                new RangeFieldMapper.Range(rangeType, 1.0D, 5.0D, true, true), // bucket -1, 4
                new RangeFieldMapper.Range(rangeType, -3.1, 4.2, true, true), // bucket -6 -1 4
                new RangeFieldMapper.Range(rangeType, 4.2, 13.3, true, true), // bucket 4, 9
                new RangeFieldMapper.Range(rangeType, 42.5, 49.3, true, true), // bucket 39, 44, 49
            }) {
                Document doc = new Document();
                BytesRef encodedRange = rangeType.encodeRanges(Collections.singleton(range));
                doc.add(new BinaryDocValuesField("field", encodedRange));
                w.addDocument(doc);
            }

            HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg")
                .field("field")
                .interval(5)
                .offset(4);
            MappedFieldType fieldType = new RangeFieldMapper.Builder("field", rangeType).fieldType();
            fieldType.setName("field");

            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalHistogram histogram = search(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType);
                //assertEquals(7, histogram.getBuckets().size());

                assertEquals(-6d, histogram.getBuckets().get(0).getKey());
                assertEquals(1, histogram.getBuckets().get(0).getDocCount());

                assertEquals(-1d, histogram.getBuckets().get(1).getKey());
                assertEquals(2, histogram.getBuckets().get(1).getDocCount());

                assertEquals(4d, histogram.getBuckets().get(2).getKey());
                assertEquals(3, histogram.getBuckets().get(2).getDocCount());

                assertEquals(9d, histogram.getBuckets().get(3).getKey());
                assertEquals(1, histogram.getBuckets().get(3).getDocCount());

                assertEquals(39d, histogram.getBuckets().get(4).getKey());
                assertEquals(1, histogram.getBuckets().get(4).getDocCount());

                assertEquals(44d, histogram.getBuckets().get(5).getKey());
                assertEquals(1, histogram.getBuckets().get(5).getDocCount());

                assertEquals(49d, histogram.getBuckets().get(6).getKey());
                assertEquals(1, histogram.getBuckets().get(6).getDocCount());
            }
        }
    }

    public void testOffsetGtInterval() throws Exception {
        RangeType rangeType = RangeType.DOUBLE;
        try (Directory dir = newDirectory();
             RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (RangeFieldMapper.Range range : new RangeFieldMapper.Range[] {
                new RangeFieldMapper.Range(rangeType, 1.0D, 5.0D, true, true), // bucket 0 5
                new RangeFieldMapper.Range(rangeType, -3.1, 4.2, true, true), // bucket -5, 0
                new RangeFieldMapper.Range(rangeType, 4.2, 13.3, true, true), // bucket 0, 5, 10
                new RangeFieldMapper.Range(rangeType, 42.5, 49.3, true, true), // bucket 40, 45
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

            HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg")
                .field("field")
                .interval(interval)
                .offset(offset);
            MappedFieldType fieldType = new RangeFieldMapper.Builder("field", rangeType).fieldType();
            fieldType.setName("field");

            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalHistogram histogram = search(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType);
                assertEquals(6, histogram.getBuckets().size());

                assertEquals(-5d + expectedOffset, histogram.getBuckets().get(0).getKey());
                assertEquals(1, histogram.getBuckets().get(0).getDocCount());

                assertEquals(0d + expectedOffset, histogram.getBuckets().get(1).getKey());
                assertEquals(3, histogram.getBuckets().get(1).getDocCount());

                assertEquals(5d + expectedOffset, histogram.getBuckets().get(2).getKey());
                assertEquals(2, histogram.getBuckets().get(2).getDocCount());

                assertEquals(10d + expectedOffset, histogram.getBuckets().get(3).getKey());
                assertEquals(1, histogram.getBuckets().get(3).getDocCount());

                assertEquals(40d + expectedOffset, histogram.getBuckets().get(4).getKey());
                assertEquals(1, histogram.getBuckets().get(4).getDocCount());

                assertEquals(45d + expectedOffset, histogram.getBuckets().get(5).getKey());
                assertEquals(1, histogram.getBuckets().get(5).getDocCount());
            }
        }
    }


    public void testIpRangesUnsupported() throws Exception {
        RangeType rangeType = RangeType.IP;
        try (Directory dir = newDirectory();
             RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            Document doc = new Document();
            BytesRef encodedRange =
                rangeType.encodeRanges(Collections.singleton(new RangeFieldMapper.Range(rangeType, InetAddresses.forString("10.0.0.1"),
                    InetAddresses.forString("10.0.0.10"), true, true)));
            doc.add(new BinaryDocValuesField("field", encodedRange));
            w.addDocument(doc);

            HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("my_agg")
                .field("field")
                .interval(5);
            MappedFieldType fieldType = new RangeFieldMapper.Builder("field", rangeType).fieldType();
            fieldType.setName("field");

            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                expectedException.expect(IllegalArgumentException.class);
                search(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType);
            }
        }

    }

}
