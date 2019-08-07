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

import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.Consumer;

import static java.util.Collections.singleton;

public class SumAggregatorTests extends AggregatorTestCase {

    private static final String FIELD_NAME = "field";

    public void testNoDocs() throws IOException {
        testCase(new MatchAllDocsQuery(), iw -> {
            // Intentionally not writing any docs
        }, count -> {
            assertEquals(0L, count.getValue(), 0d);
            assertFalse(AggregationInspectionHelper.hasValue(count));
        });
    }

    public void testNoMatchingField() throws IOException {
        testCase(new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("wrong_number", 7)));
            iw.addDocument(singleton(new NumericDocValuesField("wrong_number", 1)));
        }, count -> {
            assertEquals(0L, count.getValue(), 0d);
            assertFalse(AggregationInspectionHelper.hasValue(count));
        });
    }

    public void testNumericDocValues() throws IOException {
        testCase(new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField(FIELD_NAME, 1)));
            iw.addDocument(singleton(new NumericDocValuesField(FIELD_NAME, 2)));
            iw.addDocument(singleton(new NumericDocValuesField(FIELD_NAME, 1)));
            iw.addDocument(singleton(new NumericDocValuesField(FIELD_NAME, 2)));
            iw.addDocument(singleton(new NumericDocValuesField(FIELD_NAME, 1)));
            iw.addDocument(singleton(new NumericDocValuesField(FIELD_NAME, 2)));
            iw.addDocument(singleton(new NumericDocValuesField(FIELD_NAME, 1)));
            iw.addDocument(singleton(new NumericDocValuesField(FIELD_NAME, 2)));
            iw.addDocument(singleton(new NumericDocValuesField(FIELD_NAME, 1)));
            iw.addDocument(singleton(new NumericDocValuesField(FIELD_NAME, 2)));
            iw.addDocument(singleton(new NumericDocValuesField(FIELD_NAME, 1)));
            iw.addDocument(singleton(new NumericDocValuesField(FIELD_NAME, 2)));
            iw.addDocument(singleton(new NumericDocValuesField(FIELD_NAME, 1)));
            iw.addDocument(singleton(new NumericDocValuesField(FIELD_NAME, 2)));
            iw.addDocument(singleton(new NumericDocValuesField(FIELD_NAME, 1)));
            iw.addDocument(singleton(new NumericDocValuesField(FIELD_NAME, 2)));
        }, count -> {
            assertEquals(24L, count.getValue(), 0d);
            assertTrue(AggregationInspectionHelper.hasValue(count));
        });
    }

    public void testSortedNumericDocValues() throws IOException {
        testCase(new DocValuesFieldExistsQuery(FIELD_NAME), iw -> {
            iw.addDocument(Arrays.asList(new SortedNumericDocValuesField(FIELD_NAME, 3),
                new SortedNumericDocValuesField(FIELD_NAME, 4)));
            iw.addDocument(Arrays.asList(new SortedNumericDocValuesField(FIELD_NAME, 3),
                new SortedNumericDocValuesField(FIELD_NAME, 4)));
            iw.addDocument(singleton(new SortedNumericDocValuesField(FIELD_NAME, 1)));
        }, count -> {
            assertEquals(15L, count.getValue(), 0d);
            assertTrue(AggregationInspectionHelper.hasValue(count));
        });
    }

    public void testQueryFiltering() throws IOException {
        testCase(new TermQuery(new Term("match", "yes")), iw -> {
            iw.addDocument(Arrays.asList(new StringField("match", "yes", Field.Store.NO), new NumericDocValuesField(FIELD_NAME, 1)));
            iw.addDocument(Arrays.asList(new StringField("match", "no", Field.Store.NO), new NumericDocValuesField(FIELD_NAME, 2)));
            iw.addDocument(Arrays.asList(new StringField("match", "yes", Field.Store.NO), new NumericDocValuesField(FIELD_NAME, 3)));
            iw.addDocument(Arrays.asList(new StringField("match", "no", Field.Store.NO), new NumericDocValuesField(FIELD_NAME, 4)));
            iw.addDocument(Arrays.asList(new StringField("match", "yes", Field.Store.NO), new NumericDocValuesField(FIELD_NAME, 5)));
        }, count -> {
            assertEquals(9L, count.getValue(), 0d);
            assertTrue(AggregationInspectionHelper.hasValue(count));
        });
    }

    public void testStringField() throws IOException {
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> {
            testCase(new MatchAllDocsQuery(), iw -> {
                iw.addDocument(singleton(new SortedDocValuesField(FIELD_NAME, new BytesRef("1"))));
            }, count -> {
                assertEquals(0L, count.getValue(), 0d);
                assertFalse(AggregationInspectionHelper.hasValue(count));
            });
        });
        assertEquals("unexpected docvalues type SORTED for field 'field' (expected one of [SORTED_NUMERIC, NUMERIC]). " +
            "Re-index with correct docvalues type.", e.getMessage());
    }

    public void testSummationAccuracy() throws IOException {
        // Summing up a normal array and expect an accurate value
        double[] values = new double[]{0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7};
        verifySummationOfDoubles(values, 15.3, 0d);

        // Summing up an array which contains NaN and infinities and expect a result same as naive summation
        int n = randomIntBetween(5, 10);
        values = new double[n];
        double sum = 0;
        for (int i = 0; i < n; i++) {
            values[i] = frequently()
                ? randomFrom(Double.NaN, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY)
                : randomDoubleBetween(Double.MIN_VALUE, Double.MAX_VALUE, true);
            sum += values[i];
        }
        verifySummationOfDoubles(values, sum, 1e-10);

        // Summing up some big double values and expect infinity result
        n = randomIntBetween(5, 10);
        double[] largeValues = new double[n];
        for (int i = 0; i < n; i++) {
            largeValues[i] = Double.MAX_VALUE;
        }
        verifySummationOfDoubles(largeValues, Double.POSITIVE_INFINITY, 0d);

        for (int i = 0; i < n; i++) {
            largeValues[i] = -Double.MAX_VALUE;
        }
        verifySummationOfDoubles(largeValues, Double.NEGATIVE_INFINITY, 0d);
    }

    private void verifySummationOfDoubles(double[] values, double expected, double delta) throws IOException {
        testCase(new MatchAllDocsQuery(),
            iw -> {
                for (double value : values) {
                    iw.addDocument(singleton(new NumericDocValuesField(FIELD_NAME, NumericUtils.doubleToSortableLong(value))));
                }
            },
            result -> assertEquals(expected, result.getValue(), delta),
            NumberFieldMapper.NumberType.DOUBLE
        );
    }

    private void testCase(Query query,
                          CheckedConsumer<RandomIndexWriter, IOException> indexer,
                          Consumer<InternalSum> verify) throws IOException {
        testCase(query, indexer, verify, NumberFieldMapper.NumberType.LONG);
    }

    private void testCase(Query query,
                          CheckedConsumer<RandomIndexWriter, IOException> indexer,
                          Consumer<InternalSum> verify,
                          NumberFieldMapper.NumberType fieldNumberType) throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                indexer.accept(indexWriter);
            }

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

                MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(fieldNumberType);
                fieldType.setName(FIELD_NAME);
                fieldType.setHasDocValues(true);

                SumAggregationBuilder aggregationBuilder = new SumAggregationBuilder("_name");
                aggregationBuilder.field(FIELD_NAME);

                SumAggregator aggregator = createAggregator(aggregationBuilder, indexSearcher, fieldType);
                aggregator.preCollection();
                indexSearcher.search(query, aggregator);
                aggregator.postCollection();

                verify.accept((InternalSum) aggregator.buildAggregation(0L));
            }
        }
    }

}
