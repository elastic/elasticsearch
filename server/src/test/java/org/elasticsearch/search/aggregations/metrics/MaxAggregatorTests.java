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
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FutureArrays;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Collections.singleton;
import static org.hamcrest.Matchers.equalTo;

public class MaxAggregatorTests extends AggregatorTestCase {
    public void testNoDocs() throws IOException {
        testCase(new MatchAllDocsQuery(), iw -> {
            // Intentionally not writing any docs
        }, max -> {
            assertEquals(Double.NEGATIVE_INFINITY, max.getValue(), 0);
            assertFalse(AggregationInspectionHelper.hasValue(max));
        });
    }

    public void testNoMatchingField() throws IOException {
        testCase(new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField("wrong_number", 7)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("wrong_number", 1)));
        }, max -> {
            assertEquals(Double.NEGATIVE_INFINITY, max.getValue(), 0);
            assertFalse(AggregationInspectionHelper.hasValue(max));
        });
    }

    public void testSomeMatchesSortedNumericDocValues() throws IOException {
        testCase(new DocValuesFieldExistsQuery("number"), iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 7)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 1)));
        }, max -> {
            assertEquals(7, max.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(max));
        });
    }

    public void testSomeMatchesNumericDocValues() throws IOException {
        testCase(new DocValuesFieldExistsQuery("number"), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("number", 7)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 1)));
        }, max -> {
            assertEquals(7, max.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(max));
        });
    }

    public void testQueryFiltering() throws IOException {
        testCase(IntPoint.newRangeQuery("number", 0, 5), iw -> {
            iw.addDocument(Arrays.asList(new IntPoint("number", 7), new SortedNumericDocValuesField("number", 7)));
            iw.addDocument(Arrays.asList(new IntPoint("number", 1), new SortedNumericDocValuesField("number", 1)));
        }, max -> {
            assertEquals(1, max.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(max));
        });
    }

    public void testQueryFiltersAll() throws IOException {
        testCase(IntPoint.newRangeQuery("number", -1, 0), iw -> {
            iw.addDocument(Arrays.asList(new IntPoint("number", 7), new SortedNumericDocValuesField("number", 7)));
            iw.addDocument(Arrays.asList(new IntPoint("number", 1), new SortedNumericDocValuesField("number", 1)));
        }, max -> {
            assertEquals(Double.NEGATIVE_INFINITY, max.getValue(), 0);
            assertFalse(AggregationInspectionHelper.hasValue(max));
        });
    }

    public void testUnmappedField() throws IOException {
        MaxAggregationBuilder aggregationBuilder = new MaxAggregationBuilder("_name").field("number");
        testCase(aggregationBuilder, new DocValuesFieldExistsQuery("number"), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("number", 7)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 1)));
        }, max -> {
            assertEquals(max.getValue(), Double.NEGATIVE_INFINITY, 0);
            assertFalse(AggregationInspectionHelper.hasValue(max));
        }, null);
    }

    public void testUnmappedWithMissingField() throws IOException {
        MaxAggregationBuilder aggregationBuilder = new MaxAggregationBuilder("_name").field("number").missing(19L);

        testCase(aggregationBuilder, new DocValuesFieldExistsQuery("number"), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("number", 7)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 1)));
        }, max -> {
            assertEquals(max.getValue(), 19.0, 0);
            assertTrue(AggregationInspectionHelper.hasValue(max));
        }, null);
    }

    private void testCase(Query query,
                          CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
                          Consumer<InternalMax> verify) throws IOException {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.INTEGER);
        fieldType.setName("number");
        MaxAggregationBuilder aggregationBuilder = new MaxAggregationBuilder("_name").field("number");
        testCase(aggregationBuilder, query, buildIndex, verify, fieldType);
    }

    private void testCase(MaxAggregationBuilder aggregationBuilder, Query query,
                          CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
                          Consumer<InternalMax> verify, MappedFieldType fieldType) throws IOException {
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        buildIndex.accept(indexWriter);
        indexWriter.close();

        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

        MaxAggregator aggregator = createAggregator(query, aggregationBuilder, indexSearcher, createIndexSettings(), fieldType);
        aggregator.preCollection();
        indexSearcher.search(query, aggregator);
        aggregator.postCollection();
        verify.accept((InternalMax) aggregator.buildAggregation(0L));

        indexReader.close();
        directory.close();
    }

    public void testMaxShortcutRandom() throws Exception {
        testMaxShortcutCase(
            () -> randomLongBetween(Integer.MIN_VALUE, Integer.MAX_VALUE),
            (n) -> new LongPoint("number", n.longValue()),
            (v) -> LongPoint.decodeDimension(v, 0));

        testMaxShortcutCase(
            () -> randomInt(),
            (n) -> new IntPoint("number", n.intValue()),
            (v) -> IntPoint.decodeDimension(v, 0));

        testMaxShortcutCase(
            () -> randomFloat(),
            (n) -> new FloatPoint("number", n.floatValue()),
            (v) -> FloatPoint.decodeDimension(v, 0));

        testMaxShortcutCase(
            () -> randomDouble(),
            (n) -> new DoublePoint("number", n.doubleValue()),
            (v) -> DoublePoint.decodeDimension(v, 0));
    }

    private void testMaxShortcutCase(Supplier<Number> randomNumber,
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
            Number res = MaxAggregator.findLeafMaxValue(ctx.reader(), "number" , pointConvertFunc);
            assertThat(res, equalTo(values.get(values.size()-1).v2()));
        }
        for (int i = values.size()-1; i > 0; i--) {
            indexWriter.deleteDocuments(new Term("id", values.get(i).v1().toString()));
            try (IndexReader reader = DirectoryReader.open(indexWriter)) {
                LeafReaderContext ctx = reader.leaves().get(0);
                Number res = MaxAggregator.findLeafMaxValue(ctx.reader(), "number" , pointConvertFunc);
                if (res != null) {
                    assertThat(res, equalTo(values.get(i - 1).v2()));
                } else {
                    assertAllDeleted(ctx.reader().getLiveDocs(), ctx.reader().getPointValues("number"));
                }
            }
        }
        indexWriter.deleteDocuments(new Term("id", values.get(0).v1().toString()));
        try (IndexReader reader = DirectoryReader.open(indexWriter)) {
            LeafReaderContext ctx = reader.leaves().get(0);
            Number res = MaxAggregator.findLeafMaxValue(ctx.reader(), "number" , pointConvertFunc);
            assertThat(res, equalTo(null));
        }
        indexWriter.close();
        directory.close();
    }

    // checks that documents inside the max leaves are all deleted
    private void assertAllDeleted(Bits liveDocs, PointValues values) throws IOException {
        final byte[] maxValue = values.getMaxPackedValue();
        int numBytes = values.getBytesPerDimension();
        final boolean[] seen = new boolean[1];
        values.intersect(new PointValues.IntersectVisitor() {
            @Override
            public void visit(int docID) {
                throw new AssertionError();
            }

            @Override
            public void visit(int docID, byte[] packedValue) {
                assertFalse(liveDocs.get(docID));
                seen[0] = true;
            }

            @Override
            public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                if (FutureArrays.equals(maxPackedValue, 0,  numBytes, maxValue, 0, numBytes)) {
                    return PointValues.Relation.CELL_CROSSES_QUERY;
                }
                return PointValues.Relation.CELL_OUTSIDE_QUERY;
            }
        });
        assertTrue(seen[0]);
    }
}
