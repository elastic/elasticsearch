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

package org.elasticsearch.search.aggregations.metrics.percentiles.tdigest;

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
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentilesAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentilesMethod;

import java.io.IOException;
import java.util.function.Consumer;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;

public class TDigestPercentilesAggregatorTests extends AggregatorTestCase {

    public void testNoDocs() throws IOException {
        testCase(new MatchAllDocsQuery(), iw -> {
            // Intentionally not writing any docs
        }, tdigest -> {
            assertEquals(0L, tdigest.state.size());
        });
    }

    public void testNoMatchingField() throws IOException {
        testCase(new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField("wrong_number", 7)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("wrong_number", 1)));
        }, tdigest -> {
            assertEquals(0L, tdigest.state.size());
        });
    }

    public void testSomeMatchesSortedNumericDocValues() throws IOException {
        testCase(new DocValuesFieldExistsQuery("number"), iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 8)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 5)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 3)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 2)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 1)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 1)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 0)));
        }, tdigest -> {
            assertEquals(7L, tdigest.state.size());
            assertEquals(7L, tdigest.state.centroidCount());
            assertEquals(4.0d, tdigest.percentile(75), 0.0d);
            assertEquals("4.0", tdigest.percentileAsString(75));
            assertEquals(2.0d, tdigest.percentile(50), 0.0d);
            assertEquals("2.0", tdigest.percentileAsString(50));
            assertEquals(1.0d, tdigest.percentile(20), 0.0d);
            assertEquals("1.0", tdigest.percentileAsString(20));
        });
    }

    public void testSomeMatchesNumericDocValues() throws IOException {
        testCase(new DocValuesFieldExistsQuery("number"), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("number", 8)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 5)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 3)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 2)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 1)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 1)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 0)));
        }, tdigest -> {
            assertEquals(tdigest.state.size(), 7L);
            assertEquals(tdigest.state.centroidCount(), 7L);
            assertEquals(8.0d, tdigest.percentile(100), 0.0d);
            assertEquals("8.0", tdigest.percentileAsString(100));
            assertEquals(5.48d, tdigest.percentile(86), 0.0d);
            assertEquals("5.48", tdigest.percentileAsString(86));
            assertEquals(1.0d, tdigest.percentile(33), 0.0d);
            assertEquals("1.0", tdigest.percentileAsString(33));
            assertEquals(1.0d, tdigest.percentile(25), 0.0d);
            assertEquals("1.0", tdigest.percentileAsString(25));
            assertEquals(0.06d, tdigest.percentile(1), 0.0d);
            assertEquals("0.06", tdigest.percentileAsString(1));
        });
    }

    public void testQueryFiltering() throws IOException {
        final CheckedConsumer<RandomIndexWriter, IOException> docs = iw -> {
            iw.addDocument(asList(new LongPoint("row", 7), new SortedNumericDocValuesField("number", 8)));
            iw.addDocument(asList(new LongPoint("row", 6), new SortedNumericDocValuesField("number", 5)));
            iw.addDocument(asList(new LongPoint("row", 5), new SortedNumericDocValuesField("number", 3)));
            iw.addDocument(asList(new LongPoint("row", 4), new SortedNumericDocValuesField("number", 2)));
            iw.addDocument(asList(new LongPoint("row", 3), new SortedNumericDocValuesField("number", 1)));
            iw.addDocument(asList(new LongPoint("row", 2), new SortedNumericDocValuesField("number", 1)));
            iw.addDocument(asList(new LongPoint("row", 1), new SortedNumericDocValuesField("number", 0)));
        };

        testCase(LongPoint.newRangeQuery("row", 1, 4), docs, tdigest -> {
            assertEquals(4L, tdigest.state.size());
            assertEquals(4L, tdigest.state.centroidCount());
            assertEquals(2.0d, tdigest.percentile(100), 0.0d);
            assertEquals(1.0d, tdigest.percentile(50), 0.0d);
            assertEquals(0.75d, tdigest.percentile(25), 0.0d);
        });

        testCase(LongPoint.newRangeQuery("row", 100, 110), docs, tdigest -> {
            assertEquals(0L, tdigest.state.size());
            assertEquals(0L, tdigest.state.centroidCount());
        });
    }

    private void testCase(Query query, CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
                          Consumer<InternalTDigestPercentiles> verify) throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                buildIndex.accept(indexWriter);
            }

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

                PercentilesAggregationBuilder builder =
                        new PercentilesAggregationBuilder("test").field("number").method(PercentilesMethod.TDIGEST);

                MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.LONG);
                fieldType.setName("number");
                TDigestPercentilesAggregator aggregator = createAggregator(builder, indexSearcher, fieldType);
                aggregator.preCollection();
                indexSearcher.search(query, aggregator);
                aggregator.postCollection();
                verify.accept((InternalTDigestPercentiles) aggregator.buildAggregation(0L));
            }
        }
    }
}
