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

package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.metrics.HDRPercentilesAggregator;
import org.elasticsearch.search.aggregations.metrics.InternalHDRPercentiles;
import org.elasticsearch.search.aggregations.metrics.PercentilesAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.PercentilesMethod;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;

import java.io.IOException;
import java.util.function.Consumer;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;

public class HDRPercentilesAggregatorTests extends AggregatorTestCase {

    public void testNoDocs() throws IOException {
        testCase(new MatchAllDocsQuery(), iw -> {
            // Intentionally not writing any docs
        }, hdr -> {
            assertEquals(0L, hdr.state.getTotalCount());
            assertFalse(AggregationInspectionHelper.hasValue(hdr));
        });
    }

    public void testNoMatchingField() throws IOException {
        testCase(new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField("wrong_number", 7)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("wrong_number", 1)));
        }, hdr -> {
            assertEquals(0L, hdr.state.getTotalCount());
            assertFalse(AggregationInspectionHelper.hasValue(hdr));
        });
    }

    public void testSomeMatchesSortedNumericDocValues() throws IOException {
        testCase(new DocValuesFieldExistsQuery("number"), iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 60)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 40)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 20)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 10)));
        }, hdr -> {
            assertEquals(4L, hdr.state.getTotalCount());
            double approximation = 0.05d;
            assertEquals(10.0d, hdr.percentile(25), approximation);
            assertEquals(20.0d, hdr.percentile(50), approximation);
            assertEquals(40.0d, hdr.percentile(75), approximation);
            assertEquals(60.0d, hdr.percentile(99), approximation);
            assertTrue(AggregationInspectionHelper.hasValue(hdr));
        });
    }

    public void testSomeMatchesNumericDocValues() throws IOException {
        testCase(new DocValuesFieldExistsQuery("number"), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("number", 60)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 40)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 20)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 10)));
        }, hdr -> {
            assertEquals(4L, hdr.state.getTotalCount());
            double approximation = 0.05d;
            assertEquals(10.0d, hdr.percentile(25), approximation);
            assertEquals(20.0d, hdr.percentile(50), approximation);
            assertEquals(40.0d, hdr.percentile(75), approximation);
            assertEquals(60.0d, hdr.percentile(99), approximation);
            assertTrue(AggregationInspectionHelper.hasValue(hdr));
        });
    }

    public void testQueryFiltering() throws IOException {
        final CheckedConsumer<RandomIndexWriter, IOException> docs = iw -> {
            iw.addDocument(asList(new LongPoint("row", 4), new SortedNumericDocValuesField("number", 60)));
            iw.addDocument(asList(new LongPoint("row", 3), new SortedNumericDocValuesField("number", 40)));
            iw.addDocument(asList(new LongPoint("row", 2), new SortedNumericDocValuesField("number", 20)));
            iw.addDocument(asList(new LongPoint("row", 1), new SortedNumericDocValuesField("number", 10)));
        };

        testCase(LongPoint.newRangeQuery("row", 0, 2), docs, hdr -> {
            assertEquals(2L, hdr.state.getTotalCount());
            assertEquals(10.0d, hdr.percentile(randomDoubleBetween(1, 50, true)), 0.05d);
            assertTrue(AggregationInspectionHelper.hasValue(hdr));
        });

        testCase(LongPoint.newRangeQuery("row", 5, 10), docs, hdr -> {
            assertEquals(0L, hdr.state.getTotalCount());
            assertFalse(AggregationInspectionHelper.hasValue(hdr));
        });
    }

    private void testCase(Query query, CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
                          Consumer<InternalHDRPercentiles> verify) throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                buildIndex.accept(indexWriter);
            }

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

                PercentilesAggregationBuilder builder =
                        new PercentilesAggregationBuilder("test").field("number").method(PercentilesMethod.HDR);

                MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.LONG);
                fieldType.setName("number");
                HDRPercentilesAggregator aggregator = createAggregator(builder, indexSearcher, fieldType);
                aggregator.preCollection();
                indexSearcher.search(query, aggregator);
                aggregator.postCollection();
                verify.accept((InternalHDRPercentiles) aggregator.buildAggregation(0L));

            }
        }
    }
}
