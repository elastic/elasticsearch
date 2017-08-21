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

import org.apache.lucene.document.IntPoint;
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
import org.elasticsearch.search.aggregations.metrics.cardinality.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.cardinality.CardinalityAggregator;
import org.elasticsearch.search.aggregations.metrics.cardinality.InternalCardinality;
import org.elasticsearch.search.aggregations.support.ValueType;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.Consumer;

import static java.util.Collections.singleton;

public class CardinalityAggregatorTests extends AggregatorTestCase {
    public void testNoDocs() throws IOException {
        testCase(new MatchAllDocsQuery(), iw -> {
            // Intentionally not writing any docs
        }, card -> {
            assertEquals(0.0, card.getValue(), 0);
        });
    }

    public void testNoMatchingField() throws IOException {
        testCase(new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField("wrong_number", 7)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("wrong_number", 1)));
        }, card -> {
            assertEquals(0.0, card.getValue(), 0);
        });
    }

    public void testSomeMatchesSortedNumericDocValues() throws IOException {
        testCase(new DocValuesFieldExistsQuery("number"), iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 7)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 1)));
        }, card -> {
            assertEquals(2, card.getValue(), 0);
        });
    }

    public void testSomeMatchesNumericDocValues() throws IOException {
        testCase(new DocValuesFieldExistsQuery("number"), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("number", 7)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 1)));
        }, card -> {
            assertEquals(2, card.getValue(), 0);
        });
    }

    public void testQueryFiltering() throws IOException {
        testCase(IntPoint.newRangeQuery("number", 0, 5), iw -> {
            iw.addDocument(Arrays.asList(new IntPoint("number", 7),
                    new SortedNumericDocValuesField("number", 7)));
            iw.addDocument(Arrays.asList(new IntPoint("number", 1),
                    new SortedNumericDocValuesField("number", 1)));
        }, card -> {
            assertEquals(1, card.getValue(), 0);
        });
    }

    public void testQueryFiltersAll() throws IOException {
        testCase(IntPoint.newRangeQuery("number", -1, 0), iw -> {
            iw.addDocument(Arrays.asList(new IntPoint("number", 7),
                    new SortedNumericDocValuesField("number", 7)));
            iw.addDocument(Arrays.asList(new IntPoint("number", 1),
                    new SortedNumericDocValuesField("number", 1)));
        }, card -> {
            assertEquals(0.0, card.getValue(), 0);
        });
    }

    private void testCase(Query query, CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
            Consumer<InternalCardinality> verify) throws IOException {
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        buildIndex.accept(indexWriter);
        indexWriter.close();

        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

        CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder(
                "_name", ValueType.NUMERIC).field("number");
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(
                NumberFieldMapper.NumberType.LONG);
        fieldType.setName("number");
        CardinalityAggregator aggregator = createAggregator(aggregationBuilder, indexSearcher,
            fieldType);
        aggregator.preCollection();
        indexSearcher.search(query, aggregator);
        aggregator.postCollection();
        verify.accept((InternalCardinality) aggregator.buildAggregation(0L));

        indexReader.close();
        directory.close();
    }
}
