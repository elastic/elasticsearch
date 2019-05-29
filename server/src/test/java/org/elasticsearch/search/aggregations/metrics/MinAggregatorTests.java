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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.elasticsearch.search.aggregations.support.FieldContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.DoubleConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MinAggregatorTests extends AggregatorTestCase {

    public void testMinAggregator_numericDv() throws Exception {
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        Document document = new Document();
        document.add(new NumericDocValuesField("number", 9));
        document.add(new LongPoint("number", 9));
        indexWriter.addDocument(document);
        document = new Document();
        document.add(new NumericDocValuesField("number", 7));
        document.add(new LongPoint("number", 7));
        indexWriter.addDocument(document);
        document = new Document();
        document.add(new NumericDocValuesField("number", 5));
        document.add(new LongPoint("number", 5));
        indexWriter.addDocument(document);
        document = new Document();
        document.add(new NumericDocValuesField("number", 3));
        document.add(new LongPoint("number", 3));
        indexWriter.addDocument(document);
        document = new Document();
        document.add(new NumericDocValuesField("number", 1));
        document.add(new LongPoint("number", 1));
        indexWriter.addDocument(document);
        document = new Document();
        document.add(new NumericDocValuesField("number", -1));
        document.add(new LongPoint("number", -1));
        indexWriter.addDocument(document);
        indexWriter.close();

        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

        MinAggregationBuilder aggregationBuilder = new MinAggregationBuilder("_name").field("number");
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.LONG);
        fieldType.setName("number");

        testMinCase(indexSearcher, aggregationBuilder, fieldType, min -> assertEquals(-1.0d, min, 0));

        MinAggregator aggregator = createAggregator(aggregationBuilder, indexSearcher, fieldType);
        aggregator.preCollection();
        indexSearcher.search(new MatchAllDocsQuery(), aggregator);
        aggregator.postCollection();
        InternalMin result = (InternalMin) aggregator.buildAggregation(0L);
        assertEquals(-1.0, result.getValue(), 0);
        assertTrue(AggregationInspectionHelper.hasValue(result));

        indexReader.close();
        directory.close();
    }

    public void testMinAggregator_sortedNumericDv() throws Exception {
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        Document document = new Document();
        document.add(new SortedNumericDocValuesField("number", 9));
        document.add(new SortedNumericDocValuesField("number", 7));
        document.add(new LongPoint("number", 9));
        document.add(new LongPoint("number", 7));
        indexWriter.addDocument(document);
        document = new Document();
        document.add(new SortedNumericDocValuesField("number", 5));
        document.add(new SortedNumericDocValuesField("number", 3));
        document.add(new LongPoint("number", 5));
        document.add(new LongPoint("number", 3));
        indexWriter.addDocument(document);
        document = new Document();
        document.add(new SortedNumericDocValuesField("number", 1));
        document.add(new SortedNumericDocValuesField("number", -1));
        document.add(new LongPoint("number", 1));
        document.add(new LongPoint("number", -1));
        indexWriter.addDocument(document);
        indexWriter.close();

        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

        MinAggregationBuilder aggregationBuilder = new MinAggregationBuilder("_name").field("number");
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.LONG);
        fieldType.setName("number");

        MinAggregator aggregator = createAggregator(aggregationBuilder, indexSearcher, fieldType);
        aggregator.preCollection();
        indexSearcher.search(new MatchAllDocsQuery(), aggregator);
        aggregator.postCollection();
        InternalMin result = (InternalMin) aggregator.buildAggregation(0L);
        assertEquals(-1.0, result.getValue(), 0);
        assertTrue(AggregationInspectionHelper.hasValue(result));

        indexReader.close();
        directory.close();
    }

    public void testMinAggregator_noValue() throws Exception {
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        Document document = new Document();
        document.add(new SortedNumericDocValuesField("number1", 7));
        indexWriter.addDocument(document);
        document = new Document();
        document.add(new SortedNumericDocValuesField("number1", 3));
        indexWriter.addDocument(document);
        document = new Document();
        document.add(new SortedNumericDocValuesField("number1", 1));
        indexWriter.addDocument(document);
        indexWriter.close();

        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

        MinAggregationBuilder aggregationBuilder = new MinAggregationBuilder("_name").field("number2");
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.LONG);
        fieldType.setName("number2");

        MinAggregator aggregator = createAggregator(aggregationBuilder, indexSearcher, fieldType);
        aggregator.preCollection();
        indexSearcher.search(new MatchAllDocsQuery(), aggregator);
        aggregator.postCollection();
        InternalMin result = (InternalMin) aggregator.buildAggregation(0L);
        assertEquals(Double.POSITIVE_INFINITY, result.getValue(), 0);
        assertFalse(AggregationInspectionHelper.hasValue(result));

        indexReader.close();
        directory.close();
    }

    public void testMinAggregator_noDocs() throws Exception {
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        indexWriter.close();

        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

        MinAggregationBuilder aggregationBuilder = new MinAggregationBuilder("_name").field("number");
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.LONG);
        fieldType.setName("number");

        MinAggregator aggregator = createAggregator(aggregationBuilder, indexSearcher, fieldType);
        aggregator.preCollection();
        indexSearcher.search(new MatchAllDocsQuery(), aggregator);
        aggregator.postCollection();
        InternalMin result = (InternalMin) aggregator.buildAggregation(0L);
        assertEquals(Double.POSITIVE_INFINITY, result.getValue(), 0);
        assertFalse(AggregationInspectionHelper.hasValue(result));

        indexReader.close();
        directory.close();
    }

    public void testShortcutIsApplicable() {
        for (NumberFieldMapper.NumberType type : NumberFieldMapper.NumberType.values()) {
            assertNotNull(
                MinAggregator.getPointReaderOrNull(
                    mockSearchContext(new MatchAllDocsQuery()),
                    null,
                    mockNumericValuesSourceConfig("number", type, true)
                )
            );
            assertNotNull(
                MinAggregator.getPointReaderOrNull(
                    mockSearchContext(null),
                    null,
                    mockNumericValuesSourceConfig("number", type, true)
                )
            );
            assertNull(
                MinAggregator.getPointReaderOrNull(
                    mockSearchContext(null),
                    mockAggregator(),
                    mockNumericValuesSourceConfig("number", type, true)
                )
            );
            assertNull(
                MinAggregator.getPointReaderOrNull(
                    mockSearchContext(new TermQuery(new Term("foo", "bar"))),
                    null,
                    mockNumericValuesSourceConfig("number", type, true)
                )
            );
            assertNull(
                MinAggregator.getPointReaderOrNull(
                    mockSearchContext(null),
                    mockAggregator(),
                    mockNumericValuesSourceConfig("number", type, true)
                )
            );
            assertNull(
                MinAggregator.getPointReaderOrNull(
                    mockSearchContext(null),
                    null,
                    mockNumericValuesSourceConfig("number", type, false)
                )
            );
        }
        assertNotNull(
            MinAggregator.getPointReaderOrNull(
                mockSearchContext(new MatchAllDocsQuery()),
                null,
                mockDateValuesSourceConfig("number", true)
            )
        );
        assertNull(
            MinAggregator.getPointReaderOrNull(
                mockSearchContext(new MatchAllDocsQuery()),
                mockAggregator(),
                mockDateValuesSourceConfig("number", true)
            )
        );
        assertNull(
            MinAggregator.getPointReaderOrNull(
                mockSearchContext(new TermQuery(new Term("foo", "bar"))),
                null,
                mockDateValuesSourceConfig("number", true)
            )
        );
        assertNull(
            MinAggregator.getPointReaderOrNull(
                mockSearchContext(null),
                mockAggregator(),
                mockDateValuesSourceConfig("number", true)
            )
        );
        assertNull(
            MinAggregator.getPointReaderOrNull(
                mockSearchContext(null),
                null,
                mockDateValuesSourceConfig("number", false)
            )
        );
    }

    public void testMinShortcutRandom() throws Exception {
        testMinShortcutCase(
            () -> randomLongBetween(Integer.MIN_VALUE, Integer.MAX_VALUE),
            (n) -> new LongPoint("number", n.longValue()),
            (v) -> LongPoint.decodeDimension(v, 0));

        testMinShortcutCase(
            () -> randomInt(),
            (n) -> new IntPoint("number", n.intValue()),
            (v) -> IntPoint.decodeDimension(v, 0));

        testMinShortcutCase(
            () -> randomFloat(),
            (n) -> new FloatPoint("number", n.floatValue()),
            (v) -> FloatPoint.decodeDimension(v, 0));

        testMinShortcutCase(
            () -> randomDouble(),
            (n) -> new DoublePoint("number", n.doubleValue()),
            (v) -> DoublePoint.decodeDimension(v, 0));
    }

    private void testMinCase(IndexSearcher searcher,
                                AggregationBuilder aggregationBuilder,
                                MappedFieldType ft,
                                DoubleConsumer testResult) throws IOException {
        Collection<Query> queries = Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery(ft.name()));
        for (Query query : queries) {
            MinAggregator aggregator = createAggregator(query, aggregationBuilder, searcher, createIndexSettings(), ft);
            aggregator.preCollection();
            searcher.search(new MatchAllDocsQuery(), aggregator);
            aggregator.postCollection();
            InternalMin result = (InternalMin) aggregator.buildAggregation(0L);
            testResult.accept(result.getValue());
        }
    }

    private void testMinShortcutCase(Supplier<Number> randomNumber,
                                        Function<Number, Field> pointFieldFunc,
                                        Function<byte[], Number> pointConvertFunc) throws IOException {
        Directory directory = newDirectory();
        IndexWriterConfig config = newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE);
        IndexWriter indexWriter = new IndexWriter(directory, config);
        List<Document> documents = new ArrayList<>();
        List<Tuple<Integer, Number>> values = new ArrayList<>();
        int numValues = atLeast(50);
        int docID = 0;
        for (int i = 0; i < numValues; i++) {
            int numDup = randomIntBetween(1, 3);
            for (int j = 0; j < numDup; j++) {
                Document document = new Document();
                Number nextValue = randomNumber.get();
                values.add(new Tuple<>(docID, nextValue));
                document.add(new StringField("id", Integer.toString(docID), Field.Store.NO));
                document.add(pointFieldFunc.apply(nextValue));
                document.add(pointFieldFunc.apply(nextValue));
                documents.add(document);
                docID ++;
            }
        }
        // insert some documents without a value for the metric field.
        for (int i = 0; i < 3; i++) {
            Document document = new Document();
            documents.add(document);
        }
        indexWriter.addDocuments(documents);
        Collections.sort(values, Comparator.comparingDouble(t -> t.v2().doubleValue()));
        try (IndexReader reader = DirectoryReader.open(indexWriter)) {
            LeafReaderContext ctx = reader.leaves().get(0);
            Number res = MinAggregator.findLeafMinValue(ctx.reader(), "number", pointConvertFunc);
            assertThat(res, equalTo(values.get(0).v2()));
        }
        for (int i = 1; i < values.size(); i++) {
            indexWriter.deleteDocuments(new Term("id", values.get(i-1).v1().toString()));
            try (IndexReader reader = DirectoryReader.open(indexWriter)) {
                LeafReaderContext ctx = reader.leaves().get(0);
                Number res = MinAggregator.findLeafMinValue(ctx.reader(), "number", pointConvertFunc);
                assertThat(res, equalTo(values.get(i).v2()));
            }
        }
        indexWriter.deleteDocuments(new Term("id", values.get(values.size()-1).v1().toString()));
        try (IndexReader reader = DirectoryReader.open(indexWriter)) {
            LeafReaderContext ctx = reader.leaves().get(0);
            Number res = MinAggregator.findLeafMinValue(ctx.reader(), "number", pointConvertFunc);
            assertThat(res, equalTo(null));
        }
        indexWriter.close();
        directory.close();
    }

    private SearchContext mockSearchContext(Query query) {
        SearchContext searchContext = mock(SearchContext.class);
        when(searchContext.query()).thenReturn(query);
        return searchContext;
    }

    private Aggregator mockAggregator() {
        return mock(Aggregator.class);
    }

    private ValuesSourceConfig<ValuesSource.Numeric> mockNumericValuesSourceConfig(String fieldName,
                                                                                   NumberFieldMapper.NumberType numType,
                                                                                   boolean indexed) {
        ValuesSourceConfig<ValuesSource.Numeric> config = mock(ValuesSourceConfig.class);
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType(numType);
        ft.setName(fieldName);
        ft.setIndexOptions(indexed ? IndexOptions.DOCS : IndexOptions.NONE);
        ft.freeze();
        when(config.fieldContext()).thenReturn(new FieldContext(fieldName, null, ft));
        return config;
    }

    private ValuesSourceConfig<ValuesSource.Numeric> mockDateValuesSourceConfig(String fieldName, boolean indexed) {
        ValuesSourceConfig<ValuesSource.Numeric> config = mock(ValuesSourceConfig.class);
        MappedFieldType ft = new DateFieldMapper.Builder(fieldName).fieldType();
        ft.setName(fieldName);
        ft.setIndexOptions(indexed ? IndexOptions.DOCS : IndexOptions.NONE);
        ft.freeze();
        when(config.fieldContext()).thenReturn(new FieldContext(fieldName, null, ft));
        return config;
    }
}
