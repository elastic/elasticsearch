/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.boxplot;

import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
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
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;
import org.elasticsearch.xpack.analytics.aggregations.support.AnalyticsValuesSourceType;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Collections.singleton;
import static org.hamcrest.Matchers.equalTo;

public class BoxplotAggregatorTests extends AggregatorTestCase {

    /** Script to return the {@code _value} provided by aggs framework. */
    public static final String VALUE_SCRIPT = "_value";

    @Override
    protected List<SearchPlugin> getSearchPlugins() {
        return List.of(new AnalyticsPlugin());
    }

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        return new BoxplotAggregationBuilder("foo").field(fieldName);
    }

    @Override
    protected List<ValuesSourceType> getSupportedValuesSourceTypes() {
        return List.of(CoreValuesSourceType.NUMERIC, AnalyticsValuesSourceType.HISTOGRAM);
    }

    @Override
    protected ScriptService getMockScriptService() {
        Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();

        scripts.put(VALUE_SCRIPT, vars -> ((Number) vars.get("_value")).doubleValue() + 1);

        MockScriptEngine scriptEngine = new MockScriptEngine(MockScriptEngine.NAME, scripts, Collections.emptyMap());
        Map<String, ScriptEngine> engines = Collections.singletonMap(scriptEngine.getType(), scriptEngine);

        return new ScriptService(Settings.EMPTY, engines, ScriptModule.CORE_CONTEXTS, () -> 1L);
    }

    public void testNoMatchingField() throws IOException {
        testCase(new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField("wrong_number", 7)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("wrong_number", 3)));
        }, boxplot -> {
            assertEquals(Double.POSITIVE_INFINITY, boxplot.getMin(), 0);
            assertEquals(Double.NEGATIVE_INFINITY, boxplot.getMax(), 0);
            assertEquals(Double.NaN, boxplot.getQ1(), 0);
            assertEquals(Double.NaN, boxplot.getQ2(), 0);
            assertEquals(Double.NaN, boxplot.getQ3(), 0);
        });
    }

    public void testMatchesSortedNumericDocValues() throws IOException {
        testCase(new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 2)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 2)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 3)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 4)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 5)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 10)));
        }, boxplot -> {
            assertEquals(2, boxplot.getMin(), 0);
            assertEquals(10, boxplot.getMax(), 0);
            assertEquals(2, boxplot.getQ1(), 0);
            assertEquals(3.5, boxplot.getQ2(), 0);
            assertEquals(5, boxplot.getQ3(), 0);
        });
    }

    public void testMatchesNumericDocValues() throws IOException {
        testCase(new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("number", 2)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 2)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 3)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 4)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 5)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 10)));
        }, boxplot -> {
            assertEquals(2, boxplot.getMin(), 0);
            assertEquals(10, boxplot.getMax(), 0);
            assertEquals(2, boxplot.getQ1(), 0);
            assertEquals(3.5, boxplot.getQ2(), 0);
            assertEquals(5, boxplot.getQ3(), 0);
        });
    }

    public void testSomeMatchesSortedNumericDocValues() throws IOException {
        testCase(new FieldExistsQuery("number"), iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 2)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 2)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number2", 2)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 3)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 4)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 5)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 10)));
        }, boxplot -> {
            assertEquals(2, boxplot.getMin(), 0);
            assertEquals(10, boxplot.getMax(), 0);
            assertEquals(2, boxplot.getQ1(), 0);
            assertEquals(3.5, boxplot.getQ2(), 0);
            assertEquals(5, boxplot.getQ3(), 0);
        });
    }

    public void testSomeMatchesNumericDocValues() throws IOException {
        testCase(new FieldExistsQuery("number"), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("number", 2)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 2)));
            iw.addDocument(singleton(new NumericDocValuesField("number2", 2)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 3)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 4)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 5)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 10)));
        }, boxplot -> {
            assertEquals(2, boxplot.getMin(), 0);
            assertEquals(10, boxplot.getMax(), 0);
            assertEquals(2, boxplot.getQ1(), 0);
            assertEquals(3.5, boxplot.getQ2(), 0);
            assertEquals(5, boxplot.getQ3(), 0);
        });
    }

    public void testMissingField() throws IOException {
        BoxplotAggregationBuilder aggregationBuilder = new BoxplotAggregationBuilder("boxplot").field("number").missing(10L);

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);

        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("other", 2)));
            iw.addDocument(singleton(new NumericDocValuesField("other", 2)));
            iw.addDocument(singleton(new NumericDocValuesField("other", 3)));
            iw.addDocument(singleton(new NumericDocValuesField("other", 4)));
            iw.addDocument(singleton(new NumericDocValuesField("other", 5)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 0)));
        }, (Consumer<InternalBoxplot>) boxplot -> {
            assertEquals(0, boxplot.getMin(), 0);
            assertEquals(10, boxplot.getMax(), 0);
            assertEquals(10, boxplot.getQ1(), 0);
            assertEquals(10, boxplot.getQ2(), 0);
            assertEquals(10, boxplot.getQ3(), 0);
        }, fieldType);
    }

    public void testUnmappedWithMissingField() throws IOException {
        BoxplotAggregationBuilder aggregationBuilder = new BoxplotAggregationBuilder("boxplot").field("does_not_exist").missing(0L);

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);

        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("number", 7)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 1)));
        }, (Consumer<InternalBoxplot>) boxplot -> {
            assertEquals(0, boxplot.getMin(), 0);
            assertEquals(0, boxplot.getMax(), 0);
            assertEquals(0, boxplot.getQ1(), 0);
            assertEquals(0, boxplot.getQ2(), 0);
            assertEquals(0, boxplot.getQ3(), 0);
        }, fieldType);
    }

    public void testUnsupportedType() {
        BoxplotAggregationBuilder aggregationBuilder = new BoxplotAggregationBuilder("boxplot").field("not_a_number");

        MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("not_a_number");

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> testCase(
                aggregationBuilder,
                new MatchAllDocsQuery(),
                iw -> { iw.addDocument(singleton(new SortedSetDocValuesField("string", new BytesRef("foo")))); },
                (Consumer<InternalBoxplot>) boxplot -> { fail("Should have thrown exception"); },
                fieldType
            )
        );
        assertEquals(e.getMessage(), "Field [not_a_number] of type [keyword] " + "is not supported for aggregation [boxplot]");
    }

    public void testBadMissingField() {
        BoxplotAggregationBuilder aggregationBuilder = new BoxplotAggregationBuilder("boxplot").field("number").missing("not_a_number");

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);

        expectThrows(NumberFormatException.class, () -> testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("number", 2)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 2)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 3)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 4)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 5)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 10)));
        }, (Consumer<InternalBoxplot>) boxplot -> { fail("Should have thrown exception"); }, fieldType));
    }

    public void testUnmappedWithBadMissingField() {
        BoxplotAggregationBuilder aggregationBuilder = new BoxplotAggregationBuilder("boxplot").field("does_not_exist")
            .missing("not_a_number");

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);

        expectThrows(NumberFormatException.class, () -> testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("number", 2)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 2)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 3)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 4)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 5)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 10)));
        }, (Consumer<InternalBoxplot>) boxplot -> { fail("Should have thrown exception"); }, fieldType));
    }

    public void testEmptyBucket() throws IOException {
        HistogramAggregationBuilder histogram = new HistogramAggregationBuilder("histo").field("number")
            .interval(10)
            .minDocCount(0)
            .subAggregation(new BoxplotAggregationBuilder("boxplot").field("number"));

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);

        testCase(histogram, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("number", 1)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 3)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 21)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 23)));
        }, (Consumer<InternalHistogram>) histo -> {
            assertThat(histo.getBuckets().size(), equalTo(3));

            assertNotNull(histo.getBuckets().get(0).getAggregations().asMap().get("boxplot"));
            InternalBoxplot boxplot = (InternalBoxplot) histo.getBuckets().get(0).getAggregations().asMap().get("boxplot");
            assertEquals(1, boxplot.getMin(), 0);
            assertEquals(3, boxplot.getMax(), 0);
            assertEquals(1, boxplot.getQ1(), 0);
            assertEquals(2, boxplot.getQ2(), 0);
            assertEquals(3, boxplot.getQ3(), 0);

            assertNotNull(histo.getBuckets().get(1).getAggregations().asMap().get("boxplot"));
            boxplot = (InternalBoxplot) histo.getBuckets().get(1).getAggregations().asMap().get("boxplot");
            assertEquals(Double.POSITIVE_INFINITY, boxplot.getMin(), 0);
            assertEquals(Double.NEGATIVE_INFINITY, boxplot.getMax(), 0);
            assertEquals(Double.NaN, boxplot.getQ1(), 0);
            assertEquals(Double.NaN, boxplot.getQ2(), 0);
            assertEquals(Double.NaN, boxplot.getQ3(), 0);

            assertNotNull(histo.getBuckets().get(2).getAggregations().asMap().get("boxplot"));
            boxplot = (InternalBoxplot) histo.getBuckets().get(2).getAggregations().asMap().get("boxplot");
            assertEquals(21, boxplot.getMin(), 0);
            assertEquals(23, boxplot.getMax(), 0);
            assertEquals(21, boxplot.getQ1(), 0);
            assertEquals(22, boxplot.getQ2(), 0);
            assertEquals(23, boxplot.getQ3(), 0);
        }, fieldType);
    }

    public void testFormatter() throws IOException {
        BoxplotAggregationBuilder aggregationBuilder = new BoxplotAggregationBuilder("boxplot").field("number").format("0000.0");

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);

        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("number", 1)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 2)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 3)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 4)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 5)));
        }, (Consumer<InternalBoxplot>) boxplot -> {
            assertEquals(1, boxplot.getMin(), 0);
            assertEquals(5, boxplot.getMax(), 0);
            assertEquals(1.75, boxplot.getQ1(), 0);
            assertEquals(3, boxplot.getQ2(), 0);
            assertEquals(4.25, boxplot.getQ3(), 0);
            assertEquals("0001.0", boxplot.getMinAsString());
            assertEquals("0005.0", boxplot.getMaxAsString());
            assertEquals("0001.8", boxplot.getQ1AsString());
            assertEquals("0003.0", boxplot.getQ2AsString());
            assertEquals("0004.2", boxplot.getQ3AsString());
        }, fieldType);
    }

    public void testGetProperty() throws IOException {
        GlobalAggregationBuilder globalBuilder = new GlobalAggregationBuilder("global").subAggregation(
            new BoxplotAggregationBuilder("boxplot").field("number")
        );

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);

        testCase(globalBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("number", 1)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 2)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 3)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 4)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 5)));
        }, (Consumer<InternalGlobal>) global -> {
            assertEquals(5, global.getDocCount());
            assertTrue(AggregationInspectionHelper.hasValue(global));
            assertNotNull(global.getAggregations().asMap().get("boxplot"));
            InternalBoxplot boxplot = (InternalBoxplot) global.getAggregations().asMap().get("boxplot");
            assertThat(global.getProperty("boxplot"), equalTo(boxplot));
            assertThat(global.getProperty("boxplot.min"), equalTo(1.0));
            assertThat(global.getProperty("boxplot.max"), equalTo(5.0));
            assertThat(boxplot.getProperty("min"), equalTo(1.0));
            assertThat(boxplot.getProperty("max"), equalTo(5.0));
        }, fieldType);
    }

    public void testValueScript() throws IOException {
        BoxplotAggregationBuilder aggregationBuilder = new BoxplotAggregationBuilder("boxplot").field("number")
            .script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, VALUE_SCRIPT, Collections.emptyMap()));

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);

        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("number", 7)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 1)));
        }, (Consumer<InternalBoxplot>) boxplot -> {
            assertEquals(2, boxplot.getMin(), 0);
            assertEquals(8, boxplot.getMax(), 0);
            assertEquals(2, boxplot.getQ1(), 0);
            assertEquals(5, boxplot.getQ2(), 0);
            assertEquals(8, boxplot.getQ3(), 0);
        }, fieldType);
    }

    public void testValueScriptUnmapped() throws IOException {
        BoxplotAggregationBuilder aggregationBuilder = new BoxplotAggregationBuilder("boxplot").field("does_not_exist")
            .script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, VALUE_SCRIPT, Collections.emptyMap()));

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);

        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("number", 7)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 1)));
        }, (Consumer<InternalBoxplot>) boxplot -> {
            assertEquals(Double.POSITIVE_INFINITY, boxplot.getMin(), 0);
            assertEquals(Double.NEGATIVE_INFINITY, boxplot.getMax(), 0);
            assertEquals(Double.NaN, boxplot.getQ1(), 0);
            assertEquals(Double.NaN, boxplot.getQ2(), 0);
            assertEquals(Double.NaN, boxplot.getQ3(), 0);
        }, fieldType);
    }

    public void testValueScriptUnmappedMissing() throws IOException {
        BoxplotAggregationBuilder aggregationBuilder = new BoxplotAggregationBuilder("boxplot").field("does_not_exist")
            .script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, VALUE_SCRIPT, Collections.emptyMap()))
            .missing(1.0);

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);

        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("number", 7)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 1)));
        }, (Consumer<InternalBoxplot>) boxplot -> {
            // Note: the way scripts, missing and unmapped interact, these will be the missing value and the script is not invoked
            assertEquals(1.0, boxplot.getMin(), 0);
            assertEquals(1.0, boxplot.getMax(), 0);
            assertEquals(1.0, boxplot.getQ1(), 0);
            assertEquals(1.0, boxplot.getQ2(), 0);
            assertEquals(1.0, boxplot.getQ3(), 0);
        }, fieldType);
    }

    private void testCase(Query query, CheckedConsumer<RandomIndexWriter, IOException> buildIndex, Consumer<InternalBoxplot> verify)
        throws IOException {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);
        BoxplotAggregationBuilder aggregationBuilder = new BoxplotAggregationBuilder("boxplot").field("number");
        testCase(aggregationBuilder, query, buildIndex, verify, fieldType);
    }

}
