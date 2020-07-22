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
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.lookup.LeafDocLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Collections.singleton;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

public class AvgAggregatorTests extends AggregatorTestCase {

    /** Script to take a field name in params and sum the values of the field. */
    public static final String SUM_FIELD_PARAMS_SCRIPT = "sum_field_params";

    /** Script to sum the values of a field named {@code values}. */
    public static final String SUM_VALUES_FIELD_SCRIPT = "sum_values_field";

    /** Script to return the value of a field named {@code value}. */
    public static final String VALUE_FIELD_SCRIPT = "value_field";

    /** Script to return the {@code _value} provided by aggs framework. */
    public static final String VALUE_SCRIPT = "_value";

    /** Script to return a random double */
    public static final String RANDOM_SCRIPT = "Math.random()";

    @Override
    protected ScriptService getMockScriptService() {
        Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();
        Function<Map<String, Object>, Integer> getInc = vars -> {
            if (vars == null || vars.containsKey("inc") == false) {
                return 0;
            } else {
                return ((Number) vars.get("inc")).intValue();
            }
        };

        BiFunction<Map<String, Object>, String, Object> sum = (vars, fieldname) -> {
            int inc = getInc.apply(vars);
            LeafDocLookup docLookup = (LeafDocLookup) vars.get("doc");
            List<Long> values = new ArrayList<>();
            for (Object v : docLookup.get(fieldname)) {
                values.add(((Number) v).longValue() + inc);
            }
            return values;
        };

        scripts.put(SUM_FIELD_PARAMS_SCRIPT, vars -> {
            String fieldname = (String) vars.get("field");
            return sum.apply(vars, fieldname);
        });
        scripts.put(SUM_VALUES_FIELD_SCRIPT, vars -> sum.apply(vars, "values"));
        scripts.put(VALUE_FIELD_SCRIPT, vars -> sum.apply(vars, "value"));
        scripts.put(VALUE_SCRIPT, vars -> {
            int inc = getInc.apply(vars);
            return ((Number) vars.get("_value")).doubleValue() + inc;
        });

        Map<String, Function<Map<String, Object>, Object>> nonDeterministicScripts = new HashMap<>();
        nonDeterministicScripts.put(RANDOM_SCRIPT, vars -> AvgAggregatorTests.randomDouble());

        MockScriptEngine scriptEngine = new MockScriptEngine(MockScriptEngine.NAME,
            scripts,
            nonDeterministicScripts,
            Collections.emptyMap());
        Map<String, ScriptEngine> engines = Collections.singletonMap(scriptEngine.getType(), scriptEngine);

        return new ScriptService(Settings.EMPTY, engines, ScriptModule.CORE_CONTEXTS);
    }

    public void testNoDocs() throws IOException {
        testAggregation(new MatchAllDocsQuery(), iw -> {
            // Intentionally not writing any docs
        }, avg -> {
            assertEquals(Double.NaN, avg.getValue(), 0);
            assertFalse(AggregationInspectionHelper.hasValue(avg));
        });
    }

    public void testNoMatchingField() throws IOException {
        testAggregation(new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField("wrong_number", 7)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("wrong_number", 3)));
        }, avg -> {
            assertEquals(Double.NaN, avg.getValue(), 0);
            assertFalse(AggregationInspectionHelper.hasValue(avg));
        });
    }

    public void testSomeMatchesSortedNumericDocValues() throws IOException {
        testAggregation(new DocValuesFieldExistsQuery("number"), iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 7)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 2)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 3)));
        }, avg -> {
            assertEquals(4, avg.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(avg));
        });
    }

    public void testSomeMatchesNumericDocValues() throws IOException {
        testAggregation(new DocValuesFieldExistsQuery("number"), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("number", 7)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 2)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 3)));
        }, avg -> {
            assertEquals(4, avg.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(avg));
        });
    }

    public void testQueryFiltering() throws IOException {
        testAggregation(IntPoint.newRangeQuery("number", 0, 3), iw -> {
            iw.addDocument(Arrays.asList(new IntPoint("number", 7), new SortedNumericDocValuesField("number", 7)));
            iw.addDocument(Arrays.asList(new IntPoint("number", 1), new SortedNumericDocValuesField("number", 2)));
            iw.addDocument(Arrays.asList(new IntPoint("number", 3), new SortedNumericDocValuesField("number", 3)));
        }, avg -> {
            assertEquals(2.5, avg.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(avg));
        });
    }

    public void testQueryFiltersAll() throws IOException {
        testAggregation(IntPoint.newRangeQuery("number", -1, 0), iw -> {
            iw.addDocument(Arrays.asList(new IntPoint("number", 7), new SortedNumericDocValuesField("number", 7)));
            iw.addDocument(Arrays.asList(new IntPoint("number", 1), new SortedNumericDocValuesField("number", 2)));
            iw.addDocument(Arrays.asList(new IntPoint("number", 3), new SortedNumericDocValuesField("number", 7)));
        }, avg -> {
            assertEquals(Double.NaN, avg.getValue(), 0);
            assertFalse(AggregationInspectionHelper.hasValue(avg));
        });
    }

    public void testSummationAccuracy() throws IOException {
        // Summing up a normal array and expect an accurate value
        double[] values = new double[]{0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.9, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7};
        verifyAvgOfDoubles(values, 0.9, 0d);

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
        verifyAvgOfDoubles(values, sum / n, 1e-10);

        // Summing up some big double values and expect infinity result
        n = randomIntBetween(5, 10);
        double[] largeValues = new double[n];
        for (int i = 0; i < n; i++) {
            largeValues[i] = Double.MAX_VALUE;
        }
        verifyAvgOfDoubles(largeValues, Double.POSITIVE_INFINITY, 0d);

        for (int i = 0; i < n; i++) {
            largeValues[i] = -Double.MAX_VALUE;
        }
        verifyAvgOfDoubles(largeValues, Double.NEGATIVE_INFINITY, 0d);
    }

    public void testUnmappedField() throws IOException {
        AvgAggregationBuilder aggregationBuilder = new AvgAggregationBuilder("_name").field("number");
        testAggregation(aggregationBuilder, new DocValuesFieldExistsQuery("number"), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("number", 7)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 1)));
        }, avg -> {
            assertEquals(Double.NaN, avg.getValue(), 0);
            assertFalse(AggregationInspectionHelper.hasValue(avg));
        }, (MappedFieldType) null);
    }

    public void testUnmappedWithMissingField() throws IOException {
        AvgAggregationBuilder aggregationBuilder = new AvgAggregationBuilder("_name").field("number").missing(0L);
        testAggregation(aggregationBuilder, new DocValuesFieldExistsQuery("number"), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("number", 7)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 1)));
        }, avg -> {
            assertEquals(0.0, avg.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(avg));
        }, (MappedFieldType) null);
    }

    private void verifyAvgOfDoubles(double[] values, double expected, double delta) throws IOException {
        AvgAggregationBuilder aggregationBuilder = new AvgAggregationBuilder("_name").field("number");
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.DOUBLE);
        testAggregation(aggregationBuilder, new MatchAllDocsQuery(),
            iw -> {
                for (double value : values) {
                    iw.addDocument(singleton(new NumericDocValuesField("number", NumericUtils.doubleToSortableLong(value))));
                }
            },
            avg -> assertEquals(expected, avg.getValue(), delta),
            fieldType
        );
    }

    public void testSingleValuedFieldPartiallyUnmapped() throws IOException {
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        indexWriter.addDocument(singleton(new NumericDocValuesField("number", 7)));
        indexWriter.addDocument(singleton(new NumericDocValuesField("number", 2)));
        indexWriter.addDocument(singleton(new NumericDocValuesField("number", 3)));
        indexWriter.close();

        Directory unmappedDirectory = newDirectory();
        RandomIndexWriter unmappedIndexWriter = new RandomIndexWriter(random(), unmappedDirectory);
        unmappedIndexWriter.close();

        IndexReader indexReader = DirectoryReader.open(directory);
        IndexReader unamappedIndexReader = DirectoryReader.open(unmappedDirectory);
        MultiReader multiReader = new MultiReader(indexReader, unamappedIndexReader);
        IndexSearcher indexSearcher = newSearcher(multiReader, true, true);


        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);
        AvgAggregationBuilder aggregationBuilder = new AvgAggregationBuilder("_name").field("number");

        AvgAggregator aggregator = createAggregator(aggregationBuilder, indexSearcher, fieldType);
        aggregator.preCollection();
        indexSearcher.search(new MatchAllDocsQuery(), aggregator);
        aggregator.postCollection();

        InternalAvg avg = (InternalAvg) aggregator.buildAggregation(0L);

        assertEquals(4, avg.getValue(), 0);
        assertEquals(3, avg.getCount(), 0);
        assertTrue(AggregationInspectionHelper.hasValue(avg));

        multiReader.close();
        directory.close();
        unmappedDirectory.close();
    }

    public void testSingleValuedField() throws IOException {
        testAggregation(new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("number", 7)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 2)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 3)));
        }, avg -> {
            assertEquals(4, avg.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(avg));
            assertEquals(4.0, avg.getProperty("value"));
        });
    }

    public void testSingleValuedField_WithFormatter() throws IOException {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("value", NumberFieldMapper.NumberType.INTEGER);

        AvgAggregationBuilder aggregationBuilder = new AvgAggregationBuilder("_name")
            .format("#")
            .field("value")
            .script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, VALUE_SCRIPT, Collections.emptyMap()));

        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            final int numDocs = 10;
            for (int i = 0; i < numDocs; i++) {
                iw.addDocument(singleton(new NumericDocValuesField("value", i + 1)));
            }
        }, avg -> {
            assertEquals((double) (1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10) / 10, avg.getValue(),0);
            assertTrue(AggregationInspectionHelper.hasValue(avg));
            assertEquals("6", avg.getValueAsString());
        }, fieldType);
    }

    public void testSingleValuedFieldWithValueScript() throws IOException {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("value", NumberFieldMapper.NumberType.INTEGER);

        AvgAggregationBuilder aggregationBuilder = new AvgAggregationBuilder("_name")
            .field("value")
            .script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, VALUE_SCRIPT, Collections.emptyMap()));

        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            final int numDocs = 10;
            for (int i = 0; i < numDocs; i++) {
                iw.addDocument(singleton(new NumericDocValuesField("value", i + 1)));
            }
        }, avg -> {
            assertEquals((double) (1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10) / 10, avg.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(avg));
        }, fieldType);
    }

    public void testScriptSingleValued() throws IOException {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("value", NumberFieldMapper.NumberType.INTEGER);

        AvgAggregationBuilder aggregationBuilder = new AvgAggregationBuilder("_name")
            .script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, VALUE_FIELD_SCRIPT, Collections.emptyMap()));

        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            final int numDocs = 10;
            for (int i = 0; i < numDocs; i++) {
                iw.addDocument(singleton(new NumericDocValuesField("value", i + 1)));
            }
        }, avg -> {
            assertEquals((double) (1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10) / 10, avg.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(avg));
        }, fieldType);
    }

    public void testScriptSingleValuedWithParams() throws IOException {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("value", NumberFieldMapper.NumberType.INTEGER);

        Map<String, Object> params = new HashMap<>();
        params.put("inc", 1);
        params.put("field", "value");

        AvgAggregationBuilder aggregationBuilder = new AvgAggregationBuilder("_name")
            .script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, SUM_FIELD_PARAMS_SCRIPT, params));

        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            final int numDocs = 10;
            for (int i = 0; i < numDocs; i++) {
                iw.addDocument(singleton(new NumericDocValuesField("value", i + 1)));
            }
        }, avg -> {
            assertEquals((double) (2+3+4+5+6+7+8+9+10+11) / 10, avg.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(avg));
        }, fieldType);
    }

    public void testMultiValuedField() throws IOException {
        testAggregation(new MatchAllDocsQuery(), iw -> {
            final int numDocs = 10;
            for (int i = 0; i < numDocs; i++) {
                Document document = new Document();
                document.add(new SortedNumericDocValuesField("number", i + 2));
                document.add(new SortedNumericDocValuesField("number", i + 3));
                iw.addDocument(document);
            }
        }, avg -> {
            assertEquals((2+3+3+4+4+5+5+6+6+7+7+8+8+9+9+10+10+11+11+12) / 20, avg.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(avg));
        });
    }

    public void testScriptMultiValued() throws IOException {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("values", NumberFieldMapper.NumberType.INTEGER);

        AvgAggregationBuilder aggregationBuilder = new AvgAggregationBuilder("_name")
            .script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, SUM_VALUES_FIELD_SCRIPT, Collections.emptyMap()));

        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            final int numDocs = 10;
            for (int i = 0; i < numDocs; i++) {
                Document document = new Document();
                document.add(new SortedNumericDocValuesField("values", i + 2));
                document.add(new SortedNumericDocValuesField("values", i + 3));
                iw.addDocument(document);
            }
        }, avg -> {
            assertEquals((double) (2+3+3+4+4+5+5+6+6+7+7+8+8+9+9+10+10+11+11+12) / 20, avg.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(avg));
        }, fieldType);
    }

    public void testScriptMultiValuedWithParams() throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("inc", 1);
        params.put("field", "values");

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("values", NumberFieldMapper.NumberType.INTEGER);

        AvgAggregationBuilder aggregationBuilder = new AvgAggregationBuilder("_name")
            .script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, SUM_FIELD_PARAMS_SCRIPT, params));

        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            final int numDocs = 10;
            for (int i = 0; i < numDocs; i++) {
                Document document = new Document();
                document.add(new SortedNumericDocValuesField("values", i + 2));
                document.add(new SortedNumericDocValuesField("values", i + 3));
                iw.addDocument(document);
            }
        }, avg -> {
            assertEquals((double) (3+4+4+5+5+6+6+7+7+8+8+9+9+10+10+11+11+12+12+13) / 20, avg.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(avg));
        }, fieldType);
    }

    public void testSingleValuedFieldWithValueScriptWithParams() throws IOException {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("value", NumberFieldMapper.NumberType.INTEGER);

        Map<String, Object> params = Collections.singletonMap("inc", 1);
        AvgAggregationBuilder aggregationBuilder = new AvgAggregationBuilder("_name")
            .field("value")
            .script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, VALUE_SCRIPT, params));

        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            final int numDocs = 10;
            for (int i = 0; i < numDocs; i++) {
                iw.addDocument(singleton(new NumericDocValuesField("value", i + 1)));
            }
        }, avg -> {
            assertEquals((double) (2+3+4+5+6+7+8+9+10+11) / 10, avg.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(avg));
        }, fieldType);
    }

    public void testMultiValuedFieldWithValueScriptWithParams() throws IOException {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("values", NumberFieldMapper.NumberType.INTEGER);

        Map<String, Object> params = Collections.singletonMap("inc", 1);
        AvgAggregationBuilder aggregationBuilder = new AvgAggregationBuilder("_name")
            .field("values")
            .script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, VALUE_SCRIPT, params));

        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            final int numDocs = 10;
            for (int i = 0; i < numDocs; i++) {
                Document document = new Document();
                document.add(new SortedNumericDocValuesField("values", i + 2));
                document.add(new SortedNumericDocValuesField("values", i + 3));
                iw.addDocument(document);
            }
        }, avg -> {
            assertEquals((double) (3+4+4+5+5+6+6+7+7+8+8+9+9+10+10+11+11+12+12+13) / 20, avg.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(avg));
        }, fieldType);
    }

    public void testMultiValuedFieldWithValueScript() throws IOException {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("values", NumberFieldMapper.NumberType.INTEGER);

        AvgAggregationBuilder aggregationBuilder = new AvgAggregationBuilder("_name")
            .field("values")
            .script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, VALUE_SCRIPT, Collections.emptyMap()));

        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            final int numDocs = 10;
            for (int i = 0; i < numDocs; i++) {
                Document document = new Document();
                document.add(new SortedNumericDocValuesField("values", i + 2));
                document.add(new SortedNumericDocValuesField("values", i + 3));
                iw.addDocument(document);
            }
        }, avg -> {
            assertEquals((double) (2+3+3+4+4+5+5+6+6+7+7+8+8+9+9+10+10+11+11+12) / 20, avg.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(avg));
        }, fieldType);
    }

    public void testOrderByEmptyAggregation() throws IOException {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("value", NumberFieldMapper.NumberType.INTEGER);

        AggregationBuilder aggregationBuilder = new TermsAggregationBuilder("terms").userValueTypeHint(ValueType.NUMERIC)
            .field("value")
            .order(BucketOrder.compound(BucketOrder.aggregation("filter>avg", true)))
            .subAggregation(AggregationBuilders.filter("filter", termQuery("value", 100))
            .subAggregation(AggregationBuilders.avg("avg").field("value")));

        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        final int numDocs = 10;
        for (int i = 0; i < numDocs; i++) {
            indexWriter.addDocument(singleton(new NumericDocValuesField("value", i + 1)));
        }
        indexWriter.close();

        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

        TermsAggregator aggregator = createAggregator(aggregationBuilder, indexSearcher, fieldType);
        aggregator.preCollection();
        indexSearcher.search(new MatchAllDocsQuery(), aggregator);
        aggregator.postCollection();

        Terms terms = (Terms) aggregator.buildTopLevel();
        assertNotNull(terms);
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        assertNotNull(buckets);
        assertEquals(10, buckets.size());

        for (int i = 0; i < 10; i++) {
            Terms.Bucket bucket = buckets.get(i);
            assertNotNull(bucket);
            assertEquals((long) i + 1, bucket.getKeyAsNumber());
            assertEquals(1L, bucket.getDocCount());

            Filter filter = bucket.getAggregations().get("filter");
            assertNotNull(filter);
            assertEquals(0L, filter.getDocCount());

            Avg avg = filter.getAggregations().get("avg");
            assertNotNull(avg);
            assertEquals(Double.NaN, avg.getValue(), 0);
        }

        indexReader.close();
        directory.close();
    }

    private void testAggregation(Query query,
                          CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
                          Consumer<InternalAvg> verify) throws IOException {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);
        AvgAggregationBuilder aggregationBuilder = new AvgAggregationBuilder("_name").field("number");
        testAggregation(aggregationBuilder, query, buildIndex, verify, fieldType);
    }

    private void testAggregation(
        AggregationBuilder aggregationBuilder,
        Query query,
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
        Consumer<InternalAvg> verify,
        MappedFieldType fieldType)  throws IOException {
        testCase(aggregationBuilder, query, buildIndex, verify, fieldType);
    }

    /**
     * Make sure that an aggregation not using a script does get cached.
     */
    public void testCacheAggregation() throws IOException {
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        final int numDocs = 10;
        for (int i = 0; i < numDocs; i++) {
            indexWriter.addDocument(singleton(new NumericDocValuesField("value", i + 1)));
        }
        indexWriter.close();

        Directory unmappedDirectory = newDirectory();
        RandomIndexWriter unmappedIndexWriter = new RandomIndexWriter(random(), unmappedDirectory);
        unmappedIndexWriter.close();

        IndexReader indexReader = DirectoryReader.open(directory);
        IndexReader unamappedIndexReader = DirectoryReader.open(unmappedDirectory);
        MultiReader multiReader = new MultiReader(indexReader, unamappedIndexReader);
        IndexSearcher indexSearcher = newSearcher(multiReader, true, true);

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("value", NumberFieldMapper.NumberType.INTEGER);
        AvgAggregationBuilder aggregationBuilder = new AvgAggregationBuilder("avg")
            .field("value");

        AvgAggregator aggregator = createAggregator(aggregationBuilder, indexSearcher, fieldType);
        aggregator.preCollection();
        indexSearcher.search(new MatchAllDocsQuery(), aggregator);
        aggregator.postCollection();

        InternalAvg avg = (InternalAvg) aggregator.buildAggregation(0L);

        assertEquals(5.5, avg.getValue(), 0);
        assertEquals("avg", avg.getName());
        assertTrue(AggregationInspectionHelper.hasValue(avg));

        // Test that an aggregation not using a script does get cached
        assertTrue(aggregator.context().getQueryShardContext().isCacheable());

        multiReader.close();
        directory.close();
        unmappedDirectory.close();
    }

    /**
     * Make sure that an aggregation using a deterministic script does gets cached while
     * one using a nondeterministic script does not.
     */
    public void testScriptCaching() throws IOException {
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        final int numDocs = 10;
        for (int i = 0; i < numDocs; i++) {
            indexWriter.addDocument(singleton(new NumericDocValuesField("value", i + 1)));
        }
        indexWriter.close();

        Directory unmappedDirectory = newDirectory();
        RandomIndexWriter unmappedIndexWriter = new RandomIndexWriter(random(), unmappedDirectory);
        unmappedIndexWriter.close();

        IndexReader indexReader = DirectoryReader.open(directory);
        IndexReader unamappedIndexReader = DirectoryReader.open(unmappedDirectory);
        MultiReader multiReader = new MultiReader(indexReader, unamappedIndexReader);
        IndexSearcher indexSearcher = newSearcher(multiReader, true, true);

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("value", NumberFieldMapper.NumberType.INTEGER);
        AvgAggregationBuilder aggregationBuilder = new AvgAggregationBuilder("avg")
            .field("value")
            .script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, VALUE_SCRIPT, Collections.emptyMap()));

        AvgAggregator aggregator = createAggregator(aggregationBuilder, indexSearcher, fieldType);
        aggregator.preCollection();
        indexSearcher.search(new MatchAllDocsQuery(), aggregator);
        aggregator.postCollection();

        InternalAvg avg = (InternalAvg) aggregator.buildAggregation(0L);

        assertEquals(5.5, avg.getValue(), 0);
        assertEquals("avg", avg.getName());
        assertTrue(AggregationInspectionHelper.hasValue(avg));

        // Test that an aggregation using a deterministic script gets cached
        assertTrue(aggregator.context().getQueryShardContext().isCacheable());

        aggregationBuilder = new AvgAggregationBuilder("avg")
            .field("value")
            .script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, RANDOM_SCRIPT, Collections.emptyMap()));

        aggregator = createAggregator(aggregationBuilder, indexSearcher, fieldType);
        aggregator.preCollection();
        indexSearcher.search(new MatchAllDocsQuery(), aggregator);
        aggregator.postCollection();

        avg = (InternalAvg) aggregator.buildAggregation(0L);

        assertTrue(avg.getValue() >= 0.0);
        assertTrue(avg.getValue() <= 1.0);
        assertEquals("avg", avg.getName());
        assertTrue(AggregationInspectionHelper.hasValue(avg));

        // Test that an aggregation using a nondeterministic script does not get cached
        assertFalse(aggregator.context().getQueryShardContext().isCacheable());

        multiReader.close();
        directory.close();
        unmappedDirectory.close();
    }

    @Override
    protected List<ValuesSourceType> getSupportedValuesSourceTypes() {
        return List.of(
            CoreValuesSourceType.NUMERIC,
            CoreValuesSourceType.BOOLEAN,
            CoreValuesSourceType.DATE
        );
    }

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        return new AvgAggregationBuilder("foo").field(fieldName);
    }
}
