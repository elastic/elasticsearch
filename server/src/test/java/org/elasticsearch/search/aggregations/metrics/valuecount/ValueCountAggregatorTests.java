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

package org.elasticsearch.search.aggregations.metrics.valuecount;

import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.index.mapper.BooleanFieldMapper;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.IpFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.support.ValueType;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.Consumer;

import static java.util.Collections.singleton;

public class ValueCountAggregatorTests extends AggregatorTestCase {

    private static final String FIELD_NAME = "field";

    public void testNoDocs() throws IOException {
        for (ValueType valueType : ValueType.values()) {
            testCase(new MatchAllDocsQuery(), valueType, iw -> {
                // Intentionally not writing any docs
            }, count -> assertEquals(0L, count.getValue()));
        }
    }

    public void testNoMatchingField() throws IOException {
        testCase(new MatchAllDocsQuery(), ValueType.LONG, iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField("wrong_number", 7)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("wrong_number", 1)));
        }, count -> assertEquals(0L, count.getValue()));
    }

    public void testSomeMatchesSortedNumericDocValues() throws IOException {
        testCase(new DocValuesFieldExistsQuery(FIELD_NAME), ValueType.NUMERIC, iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField("wrong_number", 7)));
            iw.addDocument(singleton(new SortedNumericDocValuesField(FIELD_NAME, 7)));
            iw.addDocument(singleton(new SortedNumericDocValuesField(FIELD_NAME, 1)));
        }, count -> assertEquals(2L, count.getValue()));
    }

    public void testSomeMatchesNumericDocValues() throws IOException {
        testCase(new DocValuesFieldExistsQuery(FIELD_NAME), ValueType.NUMBER, iw -> {
            iw.addDocument(singleton(new NumericDocValuesField(FIELD_NAME, 7)));
            iw.addDocument(singleton(new NumericDocValuesField(FIELD_NAME, 1)));
        }, count -> assertEquals(2L, count.getValue()));
    }

    public void testQueryFiltering() throws IOException {
        testCase(IntPoint.newRangeQuery("level", 0, 5), ValueType.STRING, iw -> {
            iw.addDocument(Arrays.asList(new IntPoint("level", 0), new SortedDocValuesField(FIELD_NAME, new BytesRef("foo"))));
            iw.addDocument(Arrays.asList(new IntPoint("level", 1), new SortedDocValuesField(FIELD_NAME, new BytesRef("bar"))));
            iw.addDocument(Arrays.asList(new IntPoint("level", 3), new SortedDocValuesField(FIELD_NAME, new BytesRef("foo"))));
            iw.addDocument(Arrays.asList(new IntPoint("level", 5), new SortedDocValuesField(FIELD_NAME, new BytesRef("baz"))));
            iw.addDocument(Arrays.asList(new IntPoint("level", 7), new SortedDocValuesField(FIELD_NAME, new BytesRef("baz"))));
        }, count -> assertEquals(4L, count.getValue()));
    }

    public void testQueryFiltersAll() throws IOException {
        testCase(IntPoint.newRangeQuery("level", -1, 0), ValueType.STRING, iw -> {
            iw.addDocument(Arrays.asList(new IntPoint("level", 3), new SortedDocValuesField(FIELD_NAME, new BytesRef("foo"))));
            iw.addDocument(Arrays.asList(new IntPoint("level", 5), new SortedDocValuesField(FIELD_NAME, new BytesRef("baz"))));
        }, count -> assertEquals(0L, count.getValue()));
    }

    private void testCase(Query query,
                          ValueType valueType,
                          CheckedConsumer<RandomIndexWriter, IOException> indexer,
                          Consumer<ValueCount> verify) throws IOException {

        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                indexer.accept(indexWriter);
            }

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

                MappedFieldType fieldType = createMappedFieldType(valueType);
                fieldType.setName(FIELD_NAME);
                fieldType.setHasDocValues(true);

                ValueCountAggregationBuilder aggregationBuilder = new ValueCountAggregationBuilder("_name", valueType);
                aggregationBuilder.field(FIELD_NAME);

                ValueCountAggregator aggregator = createAggregator(aggregationBuilder, indexSearcher, fieldType);
                aggregator.preCollection();
                indexSearcher.search(query, aggregator);
                aggregator.postCollection();
                verify.accept((ValueCount) aggregator.buildAggregation(0L));
            }
        }
    }

    private static MappedFieldType createMappedFieldType(ValueType valueType) {
        switch (valueType) {
            case BOOLEAN:
                return new BooleanFieldMapper.BooleanFieldType();
            case STRING:
                return new KeywordFieldMapper.KeywordFieldType();
            case DOUBLE:
                return new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.DOUBLE);
            case NUMBER:
            case NUMERIC:
            case LONG:
                return new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.LONG);
            case DATE:
                return new DateFieldMapper.Builder("_name").fieldType();
            case IP:
                return new IpFieldMapper.Builder("_name").fieldType();
            case GEOPOINT:
                return new GeoPointFieldMapper.Builder("_name").fieldType();
            default:
                throw new IllegalArgumentException("Test does not support value type [" + valueType + "]");
        }
    }
}
