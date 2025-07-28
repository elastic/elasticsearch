/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.ttest;

import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.mapper.BooleanFieldMapper;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.bucket.global.GlobalAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.global.InternalGlobal;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceFieldConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.lookup.LeafDocLookup;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;

public class TTestAggregatorTests extends AggregatorTestCase {

    /**
     * Script to return the {@code _value} provided by aggs framework.
     */
    public static final String ADD_HALF_SCRIPT = "add_one";

    public static final String TERM_FILTERING = "term_filtering";

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        if (fieldType instanceof NumberFieldMapper.NumberFieldType) {
            return new TTestAggregationBuilder("foo").a(
                new MultiValuesSourceFieldConfig.Builder().setFieldName(fieldName)
                    .setFilter(QueryBuilders.rangeQuery(fieldName).lt(10))
                    .build()
            )
                .b(
                    new MultiValuesSourceFieldConfig.Builder().setFieldName(fieldName)
                        .setFilter(QueryBuilders.rangeQuery(fieldName).gte(10))
                        .build()
                );
        } else if (fieldType.typeName().equals(DateFieldMapper.CONTENT_TYPE)
            || fieldType.typeName().equals(DateFieldMapper.DATE_NANOS_CONTENT_TYPE)) {

                return new TTestAggregationBuilder("foo").a(
                    new MultiValuesSourceFieldConfig.Builder().setFieldName(fieldName)
                        .setFilter(QueryBuilders.rangeQuery(fieldName).lt(DateUtils.toInstant(10)))
                        .build()
                )
                    .b(
                        new MultiValuesSourceFieldConfig.Builder().setFieldName(fieldName)
                            .setFilter(QueryBuilders.rangeQuery(fieldName).gte(DateUtils.toInstant(10)))
                            .build()
                    );
            } else if (fieldType.typeName().equals(BooleanFieldMapper.CONTENT_TYPE)) {
                return new TTestAggregationBuilder("foo").a(
                    new MultiValuesSourceFieldConfig.Builder().setFieldName(fieldName)
                        .setFilter(QueryBuilders.rangeQuery(fieldName).lt("true"))
                        .build()
                )
                    .b(
                        new MultiValuesSourceFieldConfig.Builder().setFieldName(fieldName)
                            .setFilter(QueryBuilders.rangeQuery(fieldName).gte("false"))
                            .build()
                    );
            }
        // if it's "unsupported" just use matchall filters to avoid parsing issues
        return new TTestAggregationBuilder("foo").a(
            new MultiValuesSourceFieldConfig.Builder().setFieldName(fieldName).setFilter(QueryBuilders.matchAllQuery()).build()
        ).b(new MultiValuesSourceFieldConfig.Builder().setFieldName(fieldName).setFilter(QueryBuilders.matchAllQuery()).build());
    }

    @Override
    protected List<ValuesSourceType> getSupportedValuesSourceTypes() {
        return List.of(CoreValuesSourceType.NUMERIC, CoreValuesSourceType.BOOLEAN, CoreValuesSourceType.DATE);
    }

    @Override
    protected ScriptService getMockScriptService() {
        Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();

        scripts.put(ADD_HALF_SCRIPT, vars -> {
            LeafDocLookup leafDocLookup = (LeafDocLookup) vars.get("doc");
            String fieldname = (String) vars.get("fieldname");
            ScriptDocValues<?> scriptDocValues = leafDocLookup.get(fieldname);
            return ((Number) scriptDocValues.get(0)).doubleValue() + 0.5;
        });

        scripts.put(TERM_FILTERING, vars -> {
            LeafDocLookup leafDocLookup = (LeafDocLookup) vars.get("doc");
            int term = (Integer) vars.get("term");
            ScriptDocValues<?> termDocValues = leafDocLookup.get("term");
            int currentTerm = ((Number) termDocValues.get(0)).intValue();
            if (currentTerm == term) {
                return ((Number) leafDocLookup.get("field").get(0)).doubleValue();
            }
            return null;
        });

        MockScriptEngine scriptEngine = new MockScriptEngine(MockScriptEngine.NAME, scripts, Collections.emptyMap());
        Map<String, ScriptEngine> engines = Collections.singletonMap(scriptEngine.getType(), scriptEngine);

        return new ScriptService(Settings.EMPTY, engines, ScriptModule.CORE_CONTEXTS, () -> 1L);
    }

    public void testNoMatchingField() throws IOException {
        testCase(new MatchAllDocsQuery(), randomFrom(TTestType.values()), iw -> {
            iw.addDocument(asList(new NumericDocValuesField("wrong_a", 102), new NumericDocValuesField("wrong_b", 89)));
            iw.addDocument(asList(new NumericDocValuesField("wrong_a", 99), new NumericDocValuesField("wrong_b", 93)));
        }, tTest -> assertEquals(Double.NaN, tTest.getValue(), 0));
    }

    public void testNotEnoughRecords() throws IOException {
        testCase(new MatchAllDocsQuery(), randomFrom(TTestType.values()), iw -> {
            iw.addDocument(asList(new NumericDocValuesField("a", 102), new NumericDocValuesField("b", 89)));
        }, tTest -> assertEquals(Double.NaN, tTest.getValue(), 0));
    }

    public void testSameValues() throws IOException {
        TTestType tTestType = randomFrom(TTestType.values());
        testCase(new MatchAllDocsQuery(), tTestType, iw -> {
            iw.addDocument(asList(new NumericDocValuesField("a", 102), new NumericDocValuesField("b", 102)));
            iw.addDocument(asList(new NumericDocValuesField("a", 99), new NumericDocValuesField("b", 99)));
            iw.addDocument(asList(new NumericDocValuesField("a", 111), new NumericDocValuesField("b", 111)));
            iw.addDocument(asList(new NumericDocValuesField("a", 97), new NumericDocValuesField("b", 97)));
            iw.addDocument(asList(new NumericDocValuesField("a", 101), new NumericDocValuesField("b", 101)));
        }, tTest -> assertEquals(tTestType == TTestType.PAIRED ? Double.NaN : 1, tTest.getValue(), 0));
    }

    public void testMatchesSortedNumericDocValues() throws IOException {
        testCase(new MatchAllDocsQuery(), TTestType.PAIRED, iw -> {
            iw.addDocument(asList(new SortedNumericDocValuesField("a", 102), new SortedNumericDocValuesField("b", 89)));
            iw.addDocument(asList(new SortedNumericDocValuesField("a", 99), new SortedNumericDocValuesField("b", 93)));
            iw.addDocument(asList(new SortedNumericDocValuesField("a", 111), new SortedNumericDocValuesField("b", 72)));
            iw.addDocument(asList(new SortedNumericDocValuesField("a", 97), new SortedNumericDocValuesField("b", 98)));
            iw.addDocument(asList(new SortedNumericDocValuesField("a", 101), new SortedNumericDocValuesField("b", 102)));
            iw.addDocument(asList(new SortedNumericDocValuesField("a", 99), new SortedNumericDocValuesField("b", 98)));
        }, tTest -> assertEquals(0.09571844217 * 2, tTest.getValue(), 0.000001));
    }

    public void testMultiplePairedValues() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> testCase(new MatchAllDocsQuery(), TTestType.PAIRED, iw -> {
                iw.addDocument(
                    asList(
                        new SortedNumericDocValuesField("a", 102),
                        new SortedNumericDocValuesField("a", 103),
                        new SortedNumericDocValuesField("b", 89)
                    )
                );
                iw.addDocument(asList(new SortedNumericDocValuesField("a", 99), new SortedNumericDocValuesField("b", 93)));
            }, tTest -> fail("Should have thrown exception"))
        );
        assertEquals(
            "Encountered more than one value for a single document. Use a script to combine multiple values per doc into a single value.",
            ex.getMessage()
        );
    }

    public void testSameFieldAndNoFilters() {
        TTestType tTestType = randomFrom(TTestType.values());
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("field", NumberFieldMapper.NumberType.INTEGER);
        TTestAggregationBuilder aggregationBuilder = new TTestAggregationBuilder("t_test").a(
            new MultiValuesSourceFieldConfig.Builder().setFieldName("field").setMissing(100).build()
        ).b(new MultiValuesSourceFieldConfig.Builder().setFieldName("field").setMissing(100).build()).testType(tTestType);

        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> testCase(iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField("field", 102)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("field", 99)));
        }, tTest -> fail("Should have thrown exception"), new AggTestConfig(aggregationBuilder, fieldType)));
        assertEquals("The same field [field] is used for both population but no filters are specified.", ex.getMessage());
    }

    public void testMultipleUnpairedValues() throws IOException {
        TTestType tTestType = randomFrom(TTestType.HETEROSCEDASTIC, TTestType.HOMOSCEDASTIC);
        testCase(new MatchAllDocsQuery(), tTestType, iw -> {
            iw.addDocument(
                asList(
                    new SortedNumericDocValuesField("a", 102),
                    new SortedNumericDocValuesField("a", 103),
                    new SortedNumericDocValuesField("b", 89)
                )
            );
            iw.addDocument(asList(new SortedNumericDocValuesField("a", 99), new SortedNumericDocValuesField("b", 93)));
        }, tTest -> assertEquals(tTestType == TTestType.HETEROSCEDASTIC ? 0.0607303911 : 0.01718374671, tTest.getValue(), 0.000001));
    }

    public void testUnpairedValuesWithFilters() throws IOException {
        TTestType tTestType = randomFrom(TTestType.HETEROSCEDASTIC, TTestType.HOMOSCEDASTIC);
        testCase(new MatchAllDocsQuery(), tTestType, iw -> {
            iw.addDocument(
                asList(
                    new SortedNumericDocValuesField("a", 102),
                    new SortedNumericDocValuesField("a", 103),
                    new SortedNumericDocValuesField("b", 89)
                )
            );
            iw.addDocument(asList(new SortedNumericDocValuesField("a", 99), new SortedNumericDocValuesField("b", 93)));
        }, tTest -> assertEquals(tTestType == TTestType.HETEROSCEDASTIC ? 0.0607303911 : 0.01718374671, tTest.getValue(), 0.000001));
    }

    public void testMissingValues() throws IOException {
        TTestType tTestType = randomFrom(TTestType.values());
        testCase(new MatchAllDocsQuery(), tTestType, iw -> {
            iw.addDocument(asList(new SortedNumericDocValuesField("a", 102), new SortedNumericDocValuesField("b", 89)));
            iw.addDocument(asList(new SortedNumericDocValuesField("a1", 99), new SortedNumericDocValuesField("b", 93)));
            iw.addDocument(asList(new SortedNumericDocValuesField("a", 111), new SortedNumericDocValuesField("b1", 72)));
            iw.addDocument(asList(new SortedNumericDocValuesField("a", 97), new SortedNumericDocValuesField("b", 98)));
            iw.addDocument(asList(new SortedNumericDocValuesField("a", 101), new SortedNumericDocValuesField("b", 102)));
            iw.addDocument(asList(new SortedNumericDocValuesField("a", 99), new SortedNumericDocValuesField("b", 98)));
        }, tTest -> {
            switch (tTestType) {
                case PAIRED -> assertEquals(0.4385093524, tTest.getValue(), 0.000001);
                case HOMOSCEDASTIC -> assertEquals(0.1066843841, tTest.getValue(), 0.000001);
                case HETEROSCEDASTIC -> assertEquals(0.1068382282, tTest.getValue(), 0.000001);
                default -> fail("unknown t-test type " + tTestType);
            }
        });
    }

    public void testUnmappedWithMissingField() throws IOException {
        TTestType tTestType = randomFrom(TTestType.values());
        boolean missA = randomBoolean();
        boolean missB = missA == false || randomBoolean(); // at least one of the fields should be missing
        MappedFieldType fieldType1 = new NumberFieldMapper.NumberFieldType(missA ? "not_a" : "a", NumberFieldMapper.NumberType.INTEGER);
        MappedFieldType fieldType2 = new NumberFieldMapper.NumberFieldType(missB ? "not_b" : "b", NumberFieldMapper.NumberType.INTEGER);
        TTestAggregationBuilder aggregationBuilder = new TTestAggregationBuilder("t_test").a(
            new MultiValuesSourceFieldConfig.Builder().setFieldName("a").setMissing(100).build()
        ).b(new MultiValuesSourceFieldConfig.Builder().setFieldName("b").setMissing(100).build()).testType(tTestType);
        testCase(iw -> {
            iw.addDocument(asList(new NumericDocValuesField("a", 102), new NumericDocValuesField("b", 89)));
            iw.addDocument(asList(new NumericDocValuesField("a", 99), new NumericDocValuesField("b", 93)));
        }, (Consumer<InternalTTest>) tTest -> {
            if (missA && missB) {
                assertEquals(Double.NaN, tTest.getValue(), 0);
            } else {
                if (missA) {
                    switch (tTestType) {
                        case PAIRED -> assertEquals(0.1392089745, tTest.getValue(), 0.000001);
                        case HOMOSCEDASTIC -> assertEquals(0.04600190799, tTest.getValue(), 0.000001);
                        case HETEROSCEDASTIC -> assertEquals(0.1392089745, tTest.getValue(), 0.000001);
                        default -> fail("unknown t-test type " + tTestType);
                    }
                } else {
                    switch (tTestType) {
                        case PAIRED -> assertEquals(0.7951672353, tTest.getValue(), 0.000001);
                        case HOMOSCEDASTIC -> assertEquals(0.7705842661, tTest.getValue(), 0.000001);
                        case HETEROSCEDASTIC -> assertEquals(0.7951672353, tTest.getValue(), 0.000001);
                        default -> fail("unknown t-test type " + tTestType);
                    }
                }
            }
        }, new AggTestConfig(aggregationBuilder, fieldType1, fieldType2));
    }

    public void testUnsupportedType() {
        TTestType tTestType = randomFrom(TTestType.values());
        boolean wrongA = randomBoolean();
        boolean wrongB = wrongA == false || randomBoolean(); // at least one of the fields should have unsupported type
        MappedFieldType fieldType1;
        if (wrongA) {
            fieldType1 = new KeywordFieldMapper.KeywordFieldType("a");
        } else {
            fieldType1 = new NumberFieldMapper.NumberFieldType("a", NumberFieldMapper.NumberType.INTEGER);
        }
        MappedFieldType fieldType2;
        if (wrongB) {
            fieldType2 = new KeywordFieldMapper.KeywordFieldType("b");
        } else {
            fieldType2 = new NumberFieldMapper.NumberFieldType("b", NumberFieldMapper.NumberType.INTEGER);
        }
        TTestAggregationBuilder aggregationBuilder = new TTestAggregationBuilder("t_test").a(
            new MultiValuesSourceFieldConfig.Builder().setFieldName("a").build()
        ).b(new MultiValuesSourceFieldConfig.Builder().setFieldName("b").build()).testType(tTestType);

        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> testCase(iw -> {
            iw.addDocument(
                asList(
                    new SortedNumericDocValuesField("a", 102),
                    new SortedNumericDocValuesField("a", 103),
                    new SortedNumericDocValuesField("b", 89)
                )
            );
            iw.addDocument(asList(new SortedNumericDocValuesField("a", 99), new SortedNumericDocValuesField("b", 93)));
        }, tTest -> fail("Should have thrown exception"), new AggTestConfig(aggregationBuilder, fieldType1, fieldType2)));
        assertEquals("Expected numeric type on field [" + (wrongA ? "a" : "b") + "], but got [keyword]", ex.getMessage());
    }

    public void testBadMissingField() {
        TTestType tTestType = randomFrom(TTestType.values());
        boolean missA = randomBoolean();
        boolean missB = missA == false || randomBoolean(); // at least one of the fields should be have bad missing
        MappedFieldType fieldType1 = new NumberFieldMapper.NumberFieldType("a", NumberFieldMapper.NumberType.INTEGER);
        MultiValuesSourceFieldConfig.Builder a = new MultiValuesSourceFieldConfig.Builder().setFieldName("a");
        if (missA) {
            a.setMissing("bad_number");
        }
        MappedFieldType fieldType2 = new NumberFieldMapper.NumberFieldType("b", NumberFieldMapper.NumberType.INTEGER);
        MultiValuesSourceFieldConfig.Builder b = new MultiValuesSourceFieldConfig.Builder().setFieldName("b");
        if (missB) {
            b.setMissing("bad_number");
        }
        TTestAggregationBuilder aggregationBuilder = new TTestAggregationBuilder("t_test").a(a.build()).b(b.build()).testType(tTestType);

        NumberFormatException ex = expectThrows(NumberFormatException.class, () -> testCase(iw -> {
            iw.addDocument(asList(new SortedNumericDocValuesField("a", 102), new SortedNumericDocValuesField("b", 89)));
            iw.addDocument(asList(new SortedNumericDocValuesField("a", 99), new SortedNumericDocValuesField("b", 93)));
        }, tTest -> fail("Should have thrown exception"), new AggTestConfig(aggregationBuilder, fieldType1, fieldType2)));
        assertEquals("For input string: \"bad_number\"", ex.getMessage());
    }

    public void testUnmappedWithBadMissingField() {
        TTestType tTestType = randomFrom(TTestType.values());
        boolean missA = randomBoolean();
        boolean missB = missA == false || randomBoolean(); // at least one of the fields should be have bad missing
        MappedFieldType fieldType1 = new NumberFieldMapper.NumberFieldType("a", NumberFieldMapper.NumberType.INTEGER);
        MultiValuesSourceFieldConfig.Builder a = new MultiValuesSourceFieldConfig.Builder();
        if (missA) {
            a.setFieldName("not_a").setMissing("bad_number");
        } else {
            a.setFieldName("a");
        }
        MappedFieldType fieldType2 = new NumberFieldMapper.NumberFieldType(missB ? "not_b" : "b", NumberFieldMapper.NumberType.INTEGER);

        MultiValuesSourceFieldConfig.Builder b = new MultiValuesSourceFieldConfig.Builder();
        if (missB) {
            b.setFieldName("not_b").setMissing("bad_number");
        } else {
            b.setFieldName("b");
        }
        TTestAggregationBuilder aggregationBuilder = new TTestAggregationBuilder("t_test").a(a.build()).b(b.build()).testType(tTestType);

        NumberFormatException ex = expectThrows(NumberFormatException.class, () -> testCase(iw -> {
            iw.addDocument(asList(new SortedNumericDocValuesField("a", 102), new SortedNumericDocValuesField("b", 89)));
            iw.addDocument(asList(new SortedNumericDocValuesField("a", 99), new SortedNumericDocValuesField("b", 93)));
        }, tTest -> fail("Should have thrown exception"), new AggTestConfig(aggregationBuilder, fieldType1, fieldType2)));
        assertEquals("For input string: \"bad_number\"", ex.getMessage());
    }

    public void testEmptyBucket() throws IOException {
        TTestType tTestType = randomFrom(TTestType.values());
        MappedFieldType fieldType1 = new NumberFieldMapper.NumberFieldType("a", NumberFieldMapper.NumberType.INTEGER);
        MappedFieldType fieldType2 = new NumberFieldMapper.NumberFieldType("b", NumberFieldMapper.NumberType.INTEGER);
        MappedFieldType fieldTypePart = new NumberFieldMapper.NumberFieldType("part", NumberFieldMapper.NumberType.INTEGER);
        HistogramAggregationBuilder histogram = new HistogramAggregationBuilder("histo").field("part")
            .interval(10)
            .minDocCount(0)
            .subAggregation(
                new TTestAggregationBuilder("t_test").a(new MultiValuesSourceFieldConfig.Builder().setFieldName("a").build())
                    .b(new MultiValuesSourceFieldConfig.Builder().setFieldName("b").build())
                    .testType(tTestType)
            );

        testCase(iw -> {
            iw.addDocument(
                asList(new NumericDocValuesField("a", 102), new NumericDocValuesField("b", 89), new NumericDocValuesField("part", 1))
            );
            iw.addDocument(
                asList(new NumericDocValuesField("a", 99), new NumericDocValuesField("b", 93), new NumericDocValuesField("part", 1))
            );
            iw.addDocument(
                asList(new NumericDocValuesField("a", 111), new NumericDocValuesField("b", 72), new NumericDocValuesField("part", 1))
            );
            iw.addDocument(
                asList(new NumericDocValuesField("a", 97), new NumericDocValuesField("b", 98), new NumericDocValuesField("part", 21))
            );
            iw.addDocument(
                asList(new NumericDocValuesField("a", 101), new NumericDocValuesField("b", 102), new NumericDocValuesField("part", 21))
            );
            iw.addDocument(
                asList(new NumericDocValuesField("a", 99), new NumericDocValuesField("b", 98), new NumericDocValuesField("part", 21))
            );
        }, (Consumer<InternalHistogram>) histo -> {
            assertEquals(3, histo.getBuckets().size());
            assertNotNull(histo.getBuckets().get(0).getAggregations().get("t_test"));
            InternalTTest tTest = histo.getBuckets().get(0).getAggregations().get("t_test");
            assertEquals(
                tTestType == TTestType.PAIRED ? 0.1939778614 : tTestType == TTestType.HOMOSCEDASTIC ? 0.05878871029 : 0.07529006595,
                tTest.getValue(),
                0.000001
            );

            assertNotNull(histo.getBuckets().get(1).getAggregations().get("t_test"));
            tTest = histo.getBuckets().get(1).getAggregations().get("t_test");
            assertEquals(Double.NaN, tTest.getValue(), 0.000001);

            assertNotNull(histo.getBuckets().get(2).getAggregations().get("t_test"));
            tTest = histo.getBuckets().get(2).getAggregations().get("t_test");
            assertEquals(
                tTestType == TTestType.PAIRED ? 0.6666666667 : tTestType == TTestType.HOMOSCEDASTIC ? 0.8593081179 : 0.8594865044,
                tTest.getValue(),
                0.000001
            );

        }, new AggTestConfig(histogram, fieldType1, fieldType2, fieldTypePart));
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/54365")
    public void testFormatter() throws IOException {
        TTestType tTestType = randomFrom(TTestType.values());
        MappedFieldType fieldType1 = new NumberFieldMapper.NumberFieldType("a", NumberFieldMapper.NumberType.INTEGER);
        MappedFieldType fieldType2 = new NumberFieldMapper.NumberFieldType("b", NumberFieldMapper.NumberType.INTEGER);
        TTestAggregationBuilder aggregationBuilder = new TTestAggregationBuilder("t_test").a(
            new MultiValuesSourceFieldConfig.Builder().setFieldName("a").build()
        ).b(new MultiValuesSourceFieldConfig.Builder().setFieldName("b").build()).testType(tTestType).format("0.00%");

        testCase(iw -> {
            iw.addDocument(asList(new NumericDocValuesField("a", 102), new NumericDocValuesField("b", 89)));
            iw.addDocument(asList(new NumericDocValuesField("a", 99), new NumericDocValuesField("b", 93)));
            iw.addDocument(asList(new NumericDocValuesField("a", 111), new NumericDocValuesField("b", 72)));
        }, (Consumer<InternalTTest>) tTest -> {
            assertEquals(
                tTestType == TTestType.PAIRED ? 0.1939778614 : tTestType == TTestType.HOMOSCEDASTIC ? 0.05878871029 : 0.07529006595,
                tTest.getValue(),
                0.000001
            );
            assertEquals(
                tTestType == TTestType.PAIRED ? "19.40%" : tTestType == TTestType.HOMOSCEDASTIC ? "5.88%" : "7.53%",
                tTest.getValueAsString()
            );
        }, new AggTestConfig(aggregationBuilder, fieldType1, fieldType2));
    }

    public void testGetProperty() throws IOException {
        MappedFieldType fieldType1 = new NumberFieldMapper.NumberFieldType("a", NumberFieldMapper.NumberType.INTEGER);
        MappedFieldType fieldType2 = new NumberFieldMapper.NumberFieldType("b", NumberFieldMapper.NumberType.INTEGER);
        GlobalAggregationBuilder globalBuilder = new GlobalAggregationBuilder("global").subAggregation(
            new TTestAggregationBuilder("t_test").a(new MultiValuesSourceFieldConfig.Builder().setFieldName("a").build())
                .b(new MultiValuesSourceFieldConfig.Builder().setFieldName("b").build())
                .testType(TTestType.PAIRED)
        );

        testCase(iw -> {
            iw.addDocument(asList(new NumericDocValuesField("a", 102), new NumericDocValuesField("b", 89)));
            iw.addDocument(asList(new NumericDocValuesField("a", 99), new NumericDocValuesField("b", 93)));
            iw.addDocument(asList(new NumericDocValuesField("a", 111), new NumericDocValuesField("b", 72)));
        }, (Consumer<InternalGlobal>) global -> {
            assertEquals(3, global.getDocCount());
            assertTrue(AggregationInspectionHelper.hasValue(global));
            assertNotNull(global.getAggregations().get("t_test"));
            InternalTTest tTest = global.getAggregations().get("t_test");
            assertEquals(tTest, global.getProperty("t_test"));
            assertEquals(0.1939778614, (Double) global.getProperty("t_test.value"), 0.000001);
        }, new AggTestConfig(globalBuilder, fieldType1, fieldType2));
    }

    public void testScript() throws IOException {
        boolean fieldInA = randomBoolean();
        TTestType tTestType = randomFrom(TTestType.values());

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("field", NumberFieldMapper.NumberType.INTEGER);

        MultiValuesSourceFieldConfig a = new MultiValuesSourceFieldConfig.Builder().setFieldName("field").build();
        MultiValuesSourceFieldConfig b = new MultiValuesSourceFieldConfig.Builder().setScript(
            new Script(ScriptType.INLINE, MockScriptEngine.NAME, ADD_HALF_SCRIPT, Collections.singletonMap("fieldname", "field"))
        ).build();
        TTestAggregationBuilder aggregationBuilder = new TTestAggregationBuilder("t_test").a(fieldInA ? a : b)
            .b(fieldInA ? b : a)
            .testType(tTestType);

        testCase(iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("field", 1)));
            iw.addDocument(singleton(new NumericDocValuesField("field", 2)));
            iw.addDocument(singleton(new NumericDocValuesField("field", 3)));
        },
            (Consumer<InternalTTest>) tTest -> {
                assertEquals(tTestType == TTestType.PAIRED ? 0 : 0.5733922538, tTest.getValue(), 0.000001);
            },
            new AggTestConfig(aggregationBuilder, fieldType)
        );
    }

    public void testPaired() throws IOException {
        MappedFieldType fieldType1 = new NumberFieldMapper.NumberFieldType("a", NumberFieldMapper.NumberType.INTEGER);
        MappedFieldType fieldType2 = new NumberFieldMapper.NumberFieldType("b", NumberFieldMapper.NumberType.INTEGER);
        TTestAggregationBuilder aggregationBuilder = new TTestAggregationBuilder("t_test").a(
            new MultiValuesSourceFieldConfig.Builder().setFieldName("a").build()
        ).b(new MultiValuesSourceFieldConfig.Builder().setFieldName("b").build()).testType(TTestType.PAIRED);
        int tails = randomIntBetween(1, 2);
        if (tails == 1 || randomBoolean()) {
            aggregationBuilder.tails(tails);
        }
        testCase(iw -> {
            iw.addDocument(asList(new NumericDocValuesField("a", 102), new NumericDocValuesField("b", 89)));
            iw.addDocument(asList(new NumericDocValuesField("a", 99), new NumericDocValuesField("b", 93)));
            iw.addDocument(asList(new NumericDocValuesField("a", 111), new NumericDocValuesField("b", 72)));
            iw.addDocument(asList(new NumericDocValuesField("a", 97), new NumericDocValuesField("b", 98)));
            iw.addDocument(asList(new NumericDocValuesField("a", 101), new NumericDocValuesField("b", 102)));
            iw.addDocument(asList(new NumericDocValuesField("a", 99), new NumericDocValuesField("b", 98)));
        },
            (Consumer<InternalTTest>) ttest -> { assertEquals(0.09571844217 * tails, ttest.getValue(), 0.00001); },
            new AggTestConfig(aggregationBuilder, fieldType1, fieldType2)
        );
    }

    public void testHomoscedastic() throws IOException {
        MappedFieldType fieldType1 = new NumberFieldMapper.NumberFieldType("a", NumberFieldMapper.NumberType.INTEGER);
        MappedFieldType fieldType2 = new NumberFieldMapper.NumberFieldType("b", NumberFieldMapper.NumberType.INTEGER);
        TTestAggregationBuilder aggregationBuilder = new TTestAggregationBuilder("t_test").a(
            new MultiValuesSourceFieldConfig.Builder().setFieldName("a").build()
        ).b(new MultiValuesSourceFieldConfig.Builder().setFieldName("b").build()).testType(TTestType.HOMOSCEDASTIC);
        int tails = randomIntBetween(1, 2);
        if (tails == 1 || randomBoolean()) {
            aggregationBuilder.tails(tails);
        }
        testCase(iw -> {
            iw.addDocument(asList(new NumericDocValuesField("a", 102), new NumericDocValuesField("b", 89)));
            iw.addDocument(asList(new NumericDocValuesField("a", 99), new NumericDocValuesField("b", 93)));
            iw.addDocument(asList(new NumericDocValuesField("a", 111), new NumericDocValuesField("b", 72)));
            iw.addDocument(asList(new NumericDocValuesField("a", 97), new NumericDocValuesField("b", 98)));
            iw.addDocument(asList(new NumericDocValuesField("a", 101), new NumericDocValuesField("b", 102)));
            iw.addDocument(asList(new NumericDocValuesField("a", 99), new NumericDocValuesField("b", 98)));
        },
            (Consumer<InternalTTest>) ttest -> { assertEquals(0.03928288693 * tails, ttest.getValue(), 0.00001); },
            new AggTestConfig(aggregationBuilder, fieldType1, fieldType2)
        );
    }

    public void testHeteroscedastic() throws IOException {
        MappedFieldType fieldType1 = new NumberFieldMapper.NumberFieldType("a", NumberFieldMapper.NumberType.INTEGER);
        MappedFieldType fieldType2 = new NumberFieldMapper.NumberFieldType("b", NumberFieldMapper.NumberType.INTEGER);
        TTestAggregationBuilder aggregationBuilder = new TTestAggregationBuilder("t_test").a(
            new MultiValuesSourceFieldConfig.Builder().setFieldName("a").build()
        ).b(new MultiValuesSourceFieldConfig.Builder().setFieldName("b").build());
        if (randomBoolean()) {
            aggregationBuilder.testType(TTestType.HETEROSCEDASTIC);
        }
        int tails = randomIntBetween(1, 2);
        if (tails == 1 || randomBoolean()) {
            aggregationBuilder.tails(tails);
        }
        testCase(iw -> {
            iw.addDocument(asList(new NumericDocValuesField("a", 102), new NumericDocValuesField("b", 89)));
            iw.addDocument(asList(new NumericDocValuesField("a", 99), new NumericDocValuesField("b", 93)));
            iw.addDocument(asList(new NumericDocValuesField("a", 111), new NumericDocValuesField("b", 72)));
            iw.addDocument(asList(new NumericDocValuesField("a", 97), new NumericDocValuesField("b", 98)));
            iw.addDocument(asList(new NumericDocValuesField("a", 101), new NumericDocValuesField("b", 102)));
            iw.addDocument(asList(new NumericDocValuesField("a", 99), new NumericDocValuesField("b", 98)));
        },
            (Consumer<InternalTTest>) ttest -> { assertEquals(0.04538666214 * tails, ttest.getValue(), 0.00001); },
            new AggTestConfig(aggregationBuilder, fieldType1, fieldType2)
        );
    }

    public void testFiltered() throws IOException {
        TTestType tTestType = randomFrom(TTestType.values());
        MappedFieldType fieldType1 = new NumberFieldMapper.NumberFieldType("a", NumberFieldMapper.NumberType.INTEGER);
        MappedFieldType fieldType2 = new NumberFieldMapper.NumberFieldType("b", NumberFieldMapper.NumberType.INTEGER);
        TTestAggregationBuilder aggregationBuilder = new TTestAggregationBuilder("t_test").a(
            new MultiValuesSourceFieldConfig.Builder().setFieldName("a").setFilter(QueryBuilders.termQuery("b", 1)).build()
        )
            .b(new MultiValuesSourceFieldConfig.Builder().setFieldName("a").setFilter(QueryBuilders.termQuery("b", 2)).build())
            .testType(tTestType);
        int tails = randomIntBetween(1, 2);
        if (tails == 1 || randomBoolean()) {
            aggregationBuilder.tails(tails);
        }
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex = iw -> {
            iw.addDocument(asList(new NumericDocValuesField("a", 102), new IntPoint("b", 1)));
            iw.addDocument(asList(new NumericDocValuesField("a", 99), new IntPoint("b", 1)));
            iw.addDocument(asList(new NumericDocValuesField("a", 111), new IntPoint("b", 1)));
            iw.addDocument(asList(new NumericDocValuesField("a", 97), new IntPoint("b", 1)));
            iw.addDocument(asList(new NumericDocValuesField("a", 101), new IntPoint("b", 1)));
            iw.addDocument(asList(new NumericDocValuesField("a", 99), new IntPoint("b", 1)));

            iw.addDocument(asList(new NumericDocValuesField("a", 89), new IntPoint("b", 2)));
            iw.addDocument(asList(new NumericDocValuesField("a", 93), new IntPoint("b", 2)));
            iw.addDocument(asList(new NumericDocValuesField("a", 72), new IntPoint("b", 2)));
            iw.addDocument(asList(new NumericDocValuesField("a", 98), new IntPoint("b", 2)));
            iw.addDocument(asList(new NumericDocValuesField("a", 102), new IntPoint("b", 2)));
            iw.addDocument(asList(new NumericDocValuesField("a", 98), new IntPoint("b", 2)));

            iw.addDocument(asList(new NumericDocValuesField("a", 189), new IntPoint("b", 3)));
            iw.addDocument(asList(new NumericDocValuesField("a", 193), new IntPoint("b", 3)));
            iw.addDocument(asList(new NumericDocValuesField("a", 172), new IntPoint("b", 3)));
            iw.addDocument(asList(new NumericDocValuesField("a", 198), new IntPoint("b", 3)));
            iw.addDocument(asList(new NumericDocValuesField("a", 1102), new IntPoint("b", 3)));
            iw.addDocument(asList(new NumericDocValuesField("a", 198), new IntPoint("b", 3)));
        };
        if (tTestType == TTestType.PAIRED) {
            IllegalArgumentException ex = expectThrows(
                IllegalArgumentException.class,
                () -> testCase(
                    buildIndex,
                    tTest -> fail("Should have thrown exception"),
                    new AggTestConfig(aggregationBuilder, fieldType1, fieldType2)
                )
            );
            assertEquals("Paired t-test doesn't support filters", ex.getMessage());
        } else {
            testCase(buildIndex, (Consumer<InternalTTest>) ttest -> {
                if (tTestType == TTestType.HOMOSCEDASTIC) {
                    assertEquals(0.03928288693 * tails, ttest.getValue(), 0.00001);
                } else {
                    assertEquals(0.04538666214 * tails, ttest.getValue(), 0.00001);
                }
            }, new AggTestConfig(aggregationBuilder, fieldType1, fieldType2));
        }
    }

    public void testFilteredAsSubAgg() throws IOException {
        TTestType tTestType = randomFrom(TTestType.values());
        MappedFieldType fieldType1 = new NumberFieldMapper.NumberFieldType("h", NumberFieldMapper.NumberType.INTEGER);
        MappedFieldType fieldType2 = new NumberFieldMapper.NumberFieldType("a", NumberFieldMapper.NumberType.INTEGER);
        MappedFieldType fieldType3 = new NumberFieldMapper.NumberFieldType("b", NumberFieldMapper.NumberType.INTEGER);
        TTestAggregationBuilder ttestAggregationBuilder = new TTestAggregationBuilder("t_test").a(
            new MultiValuesSourceFieldConfig.Builder().setFieldName("a").setFilter(QueryBuilders.termQuery("b", 1)).build()
        )
            .b(new MultiValuesSourceFieldConfig.Builder().setFieldName("a").setFilter(QueryBuilders.termQuery("b", 2)).build())
            .testType(tTestType);
        int tails = randomIntBetween(1, 2);
        if (tails == 1 || randomBoolean()) {
            ttestAggregationBuilder.tails(tails);
        }
        HistogramAggregationBuilder aggregationBuilder = new HistogramAggregationBuilder("h").field("h")
            .interval(1)
            .subAggregation(ttestAggregationBuilder);
        int buckets = randomInt(100);
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex = iw -> {
            for (int i = 0; i < buckets; i++) {
                iw.addDocument(asList(new NumericDocValuesField("h", i), new NumericDocValuesField("a", 102), new IntPoint("b", 1)));
                iw.addDocument(asList(new NumericDocValuesField("h", i), new NumericDocValuesField("a", 99), new IntPoint("b", 1)));
                iw.addDocument(asList(new NumericDocValuesField("h", i), new NumericDocValuesField("a", 111), new IntPoint("b", 1)));
                iw.addDocument(asList(new NumericDocValuesField("h", i), new NumericDocValuesField("a", 97), new IntPoint("b", 1)));
                iw.addDocument(asList(new NumericDocValuesField("h", i), new NumericDocValuesField("a", 101), new IntPoint("b", 1)));
                iw.addDocument(asList(new NumericDocValuesField("h", i), new NumericDocValuesField("a", 99), new IntPoint("b", 1)));

                iw.addDocument(asList(new NumericDocValuesField("h", i), new NumericDocValuesField("a", 89), new IntPoint("b", 2)));
                iw.addDocument(asList(new NumericDocValuesField("h", i), new NumericDocValuesField("a", 93), new IntPoint("b", 2)));
                iw.addDocument(asList(new NumericDocValuesField("h", i), new NumericDocValuesField("a", 72), new IntPoint("b", 2)));
                iw.addDocument(asList(new NumericDocValuesField("h", i), new NumericDocValuesField("a", 98), new IntPoint("b", 2)));
                iw.addDocument(asList(new NumericDocValuesField("h", i), new NumericDocValuesField("a", 102), new IntPoint("b", 2)));
                iw.addDocument(asList(new NumericDocValuesField("h", i), new NumericDocValuesField("a", 98), new IntPoint("b", 2)));

                iw.addDocument(asList(new NumericDocValuesField("h", i), new NumericDocValuesField("a", 189), new IntPoint("b", 3)));
                iw.addDocument(asList(new NumericDocValuesField("h", i), new NumericDocValuesField("a", 193), new IntPoint("b", 3)));
                iw.addDocument(asList(new NumericDocValuesField("h", i), new NumericDocValuesField("a", 172), new IntPoint("b", 3)));
                iw.addDocument(asList(new NumericDocValuesField("h", i), new NumericDocValuesField("a", 198), new IntPoint("b", 3)));
                iw.addDocument(asList(new NumericDocValuesField("h", i), new NumericDocValuesField("a", 1102), new IntPoint("b", 3)));
                iw.addDocument(asList(new NumericDocValuesField("h", i), new NumericDocValuesField("a", 198), new IntPoint("b", 3)));
            }
        };
        if (tTestType == TTestType.PAIRED) {
            IllegalArgumentException ex = expectThrows(
                IllegalArgumentException.class,
                () -> testCase(
                    buildIndex,
                    tTest -> fail("Should have thrown exception"),
                    new AggTestConfig(aggregationBuilder, fieldType1, fieldType2, fieldType3)
                )
            );
            assertEquals("Paired t-test doesn't support filters", ex.getMessage());
        } else {
            testCase(buildIndex, (Consumer<InternalHistogram>) histogram -> {
                if (tTestType == TTestType.HOMOSCEDASTIC) {
                    assertEquals(buckets, histogram.getBuckets().size());
                    for (int i = 0; i < buckets; i++) {
                        InternalTTest ttest = histogram.getBuckets().get(i).getAggregations().get("t_test");
                        assertEquals(0.03928288693 * tails, ttest.getValue(), 0.00001);
                    }
                } else {
                    assertEquals(buckets, histogram.getBuckets().size());
                    for (int i = 0; i < buckets; i++) {
                        InternalTTest ttest = histogram.getBuckets().get(i).getAggregations().get("t_test");
                        assertEquals(0.04538666214 * tails, ttest.getValue(), 0.00001);
                    }
                }
            }, new AggTestConfig(aggregationBuilder, fieldType1, fieldType2, fieldType3));
        }
    }

    public void testFilterByFilterOrScript() throws IOException {
        boolean fieldInA = randomBoolean();
        TTestType tTestType = randomFrom(TTestType.HOMOSCEDASTIC, TTestType.HETEROSCEDASTIC);

        MappedFieldType fieldType1 = new NumberFieldMapper.NumberFieldType("field", NumberFieldMapper.NumberType.INTEGER);
        MappedFieldType fieldType2 = new NumberFieldMapper.NumberFieldType("term", NumberFieldMapper.NumberType.INTEGER);

        boolean filterTermOne = randomBoolean();

        MultiValuesSourceFieldConfig.Builder a = new MultiValuesSourceFieldConfig.Builder().setFieldName("field")
            .setFilter(QueryBuilders.termQuery("term", filterTermOne ? 1 : 2));
        MultiValuesSourceFieldConfig.Builder b = new MultiValuesSourceFieldConfig.Builder().setScript(
            new Script(ScriptType.INLINE, MockScriptEngine.NAME, TERM_FILTERING, Collections.singletonMap("term", filterTermOne ? 2 : 1))
        );

        TTestAggregationBuilder aggregationBuilder = new TTestAggregationBuilder("t_test").a(fieldInA ? a.build() : b.build())
            .b(fieldInA ? b.build() : a.build())
            .testType(tTestType);

        testCase(iw -> {
            iw.addDocument(asList(new NumericDocValuesField("field", 1), new IntPoint("term", 1), new NumericDocValuesField("term", 1)));
            iw.addDocument(asList(new NumericDocValuesField("field", 2), new IntPoint("term", 1), new NumericDocValuesField("term", 1)));
            iw.addDocument(asList(new NumericDocValuesField("field", 3), new IntPoint("term", 1), new NumericDocValuesField("term", 1)));

            iw.addDocument(asList(new NumericDocValuesField("field", 4), new IntPoint("term", 2), new NumericDocValuesField("term", 2)));
            iw.addDocument(asList(new NumericDocValuesField("field", 5), new IntPoint("term", 2), new NumericDocValuesField("term", 2)));
            iw.addDocument(asList(new NumericDocValuesField("field", 6), new IntPoint("term", 2), new NumericDocValuesField("term", 2)));
        },
            (Consumer<InternalTTest>) tTest -> { assertEquals(0.02131164113, tTest.getValue(), 0.000001); },
            new AggTestConfig(aggregationBuilder, fieldType1, fieldType2)
        );
    }

    private void testCase(
        Query query,
        TTestType type,
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
        Consumer<InternalTTest> verify
    ) throws IOException {
        MappedFieldType fieldType1 = new NumberFieldMapper.NumberFieldType("a", NumberFieldMapper.NumberType.INTEGER);
        MappedFieldType fieldType2 = new NumberFieldMapper.NumberFieldType("b", NumberFieldMapper.NumberType.INTEGER);

        TTestAggregationBuilder aggregationBuilder = new TTestAggregationBuilder("t_test").a(
            new MultiValuesSourceFieldConfig.Builder().setFieldName("a").build()
        ).b(new MultiValuesSourceFieldConfig.Builder().setFieldName("b").build());
        if (type != TTestType.HETEROSCEDASTIC || randomBoolean()) {
            aggregationBuilder.testType(type);
        }
        testCase(buildIndex, verify, new AggTestConfig(aggregationBuilder, fieldType1, fieldType2).withQuery(query));
    }

    @Override
    protected List<SearchPlugin> getSearchPlugins() {
        return Collections.singletonList(new AnalyticsPlugin());
    }
}
