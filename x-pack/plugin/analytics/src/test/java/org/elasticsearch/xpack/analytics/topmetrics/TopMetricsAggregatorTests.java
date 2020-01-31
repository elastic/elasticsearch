/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.topmetrics;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.mapper.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceFieldConfig;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.GeoDistanceSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.ScriptSortBuilder;
import org.elasticsearch.search.sort.ScriptSortBuilder.ScriptSortType;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.sort.SortValue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notANumber;
import static org.hamcrest.Matchers.nullValue;

public class TopMetricsAggregatorTests extends AggregatorTestCase {
    public void testNoDocs() throws IOException {
        InternalTopMetrics result = collect(simpleBuilder(), new MatchAllDocsQuery(), writer -> {},
                doubleFields());
        assertThat(result.getSortFormat(), equalTo(DocValueFormat.RAW));
        assertThat(result.getSortOrder(), equalTo(SortOrder.ASC));
        assertThat(result.getSortValue(), nullValue());
        assertThat(result.getMetricValue(), notANumber());
    }

    public void testUnmappedMetric() throws IOException {
        InternalTopMetrics result = collect(simpleBuilder(), new MatchAllDocsQuery(), writer -> {
                    writer.addDocument(singletonList(doubleField("s", 1.0)));
                },
                numberFieldType(NumberType.DOUBLE, "s"));
        assertThat(result.getSortOrder(), equalTo(SortOrder.ASC));
        assertThat(result.getSortValue(), nullValue());
        assertThat(result.getMetricValue(), notANumber());
    }

    public void testMissingValueForMetric() throws IOException {
        InternalTopMetrics result = collect(simpleBuilder(), new MatchAllDocsQuery(), writer -> {
                    writer.addDocument(singletonList(doubleField("s", 1.0)));
                },
                doubleFields());
        assertThat(result.getSortOrder(), equalTo(SortOrder.ASC));
        assertThat(result.getSortValue(), equalTo(SortValue.from(1.0)));
        assertThat(result.getMetricValue(), notANumber());
    }

    public void testActualValueForMetric() throws IOException {
        InternalTopMetrics result = collect(simpleBuilder(), new MatchAllDocsQuery(), writer -> {
                    writer.addDocument(Arrays.asList(doubleField("s", 1.0), doubleField("m", 2.0)));
                },
                doubleFields());
        assertThat(result.getSortOrder(), equalTo(SortOrder.ASC));
        assertThat(result.getSortValue(), equalTo(SortValue.from(1.0)));
        assertThat(result.getMetricValue(), equalTo(2.0d));
    }
    
    private InternalTopMetrics collectFromDoubles(TopMetricsAggregationBuilder builder) throws IOException {
        return collect(builder, new MatchAllDocsQuery(), writer -> {
                    writer.addDocument(Arrays.asList(doubleField("s", 1.0), doubleField("m", 2.0)));
                    writer.addDocument(Arrays.asList(doubleField("s", 2.0), doubleField("m", 3.0)));
                },
                doubleFields());
    }

    public void testSortByDoubleAscending() throws IOException {
        InternalTopMetrics result = collectFromDoubles(simpleBuilder(new FieldSortBuilder("s").order(SortOrder.ASC)));
        assertThat(result.getSortOrder(), equalTo(SortOrder.ASC));
        assertThat(result.getSortValue(), equalTo(SortValue.from(1.0)));
        assertThat(result.getMetricValue(), equalTo(2.0d));
    }

    public void testSortByDoubleDescending() throws IOException {
        InternalTopMetrics result = collectFromDoubles(simpleBuilder(new FieldSortBuilder("s").order(SortOrder.DESC)));
        assertThat(result.getSortOrder(), equalTo(SortOrder.DESC));
        assertThat(result.getSortValue(), equalTo(SortValue.from(2.0)));
        assertThat(result.getMetricValue(), equalTo(3.0d));
    }

    public void testSortByDoubleCastToLong() throws IOException {
        InternalTopMetrics result = collectFromDoubles(simpleBuilder(new FieldSortBuilder("s").setNumericType("long")));
        assertThat(result.getSortOrder(), equalTo(SortOrder.ASC));
        assertThat(result.getSortValue(), equalTo(SortValue.from(1)));
        assertThat(result.getMetricValue(), equalTo(2.0d));
    }


    public void testSortByFloatAscending() throws IOException {
        TopMetricsAggregationBuilder builder = simpleBuilder(new FieldSortBuilder("s").order(SortOrder.ASC));
        InternalTopMetrics result = collect(builder, new MatchAllDocsQuery(), writer -> {
                    writer.addDocument(Arrays.asList(floatField("s", 1.0F), doubleField("m", 2.0)));
                    writer.addDocument(Arrays.asList(floatField("s", 2.0F), doubleField("m", 3.0)));
                },
                floatAndDoubleField());
        assertThat(result.getSortOrder(), equalTo(SortOrder.ASC));
        assertThat(result.getSortValue(), equalTo(SortValue.from(1.0)));
        assertThat(result.getMetricValue(), equalTo(2.0d));
    }

    public void testSortByFloatDescending() throws IOException {
        TopMetricsAggregationBuilder builder = simpleBuilder(new FieldSortBuilder("s").order(SortOrder.DESC));
        InternalTopMetrics result = collect(builder, new MatchAllDocsQuery(), writer -> {
                    writer.addDocument(Arrays.asList(floatField("s", 1.0F), doubleField("m", 2.0)));
                    writer.addDocument(Arrays.asList(floatField("s", 2.0F), doubleField("m", 3.0)));
                },
                floatAndDoubleField());
        assertThat(result.getSortOrder(), equalTo(SortOrder.DESC));
        assertThat(result.getSortValue(), equalTo(SortValue.from(2.0)));
        assertThat(result.getMetricValue(), equalTo(3.0d));
    }

    public void testSortByLongAscending() throws IOException {
        TopMetricsAggregationBuilder builder = simpleBuilder(new FieldSortBuilder("s").order(SortOrder.ASC));
        InternalTopMetrics result = collect(builder, new MatchAllDocsQuery(), writer -> {
                    writer.addDocument(Arrays.asList(longField("s", 10), doubleField("m", 2.0)));
                    writer.addDocument(Arrays.asList(longField("s", 20), doubleField("m", 3.0)));
                },
                longAndDoubleField());
        assertThat(result.getSortOrder(), equalTo(SortOrder.ASC));
        assertThat(result.getSortValue(), equalTo(SortValue.from(10)));
        assertThat(result.getMetricValue(), equalTo(2.0d));
    }

    public void testSortByLongDescending() throws IOException {
        TopMetricsAggregationBuilder builder = simpleBuilder(new FieldSortBuilder("s").order(SortOrder.DESC));
        InternalTopMetrics result = collect(builder, new MatchAllDocsQuery(), writer -> {
                    writer.addDocument(Arrays.asList(longField("s", 10), doubleField("m", 2.0)));
                    writer.addDocument(Arrays.asList(longField("s", 20), doubleField("m", 3.0)));
                },
                longAndDoubleField());
        assertThat(result.getSortOrder(), equalTo(SortOrder.DESC));
        assertThat(result.getSortValue(), equalTo(SortValue.from(20)));
        assertThat(result.getMetricValue(), equalTo(3.0d));
    }

    public void testSortByScoreDescending() throws IOException {
        TopMetricsAggregationBuilder builder = simpleBuilder(new ScoreSortBuilder().order(SortOrder.DESC));
        InternalTopMetrics result = collect(builder, boostFoo(), writer -> {
                    writer.addDocument(Arrays.asList(textField("s", "foo"), doubleField("m", 2.0)));
                    writer.addDocument(Arrays.asList(textField("s", "bar"), doubleField("m", 3.0)));
                },
                textAndDoubleField());
        assertThat(result.getSortOrder(), equalTo(SortOrder.DESC));
        assertThat(result.getSortValue(), equalTo(SortValue.from(2.0)));
        assertThat(result.getMetricValue(), equalTo(2.0d));
    }

    public void testSortByScoreAscending() throws IOException {
        TopMetricsAggregationBuilder builder = simpleBuilder(new ScoreSortBuilder().order(SortOrder.ASC));
        InternalTopMetrics result = collect(builder, boostFoo(), writer -> {
                    writer.addDocument(Arrays.asList(textField("s", "foo"), doubleField("m", 2.0)));
                    writer.addDocument(Arrays.asList(textField("s", "bar"), doubleField("m", 3.0)));
                },
                textAndDoubleField());
        assertThat(result.getSortOrder(), equalTo(SortOrder.ASC));
        assertThat(result.getSortValue(), equalTo(SortValue.from(1.0)));
        assertThat(result.getMetricValue(), equalTo(3.0d));
    }

    public void testSortByScriptDescending() throws IOException {
        TopMetricsAggregationBuilder builder = simpleBuilder(scriptSortOnS().order(SortOrder.DESC));
        InternalTopMetrics result = collect(builder, new MatchAllDocsQuery(), writer -> {
                    writer.addDocument(Arrays.asList(doubleField("s", 2), doubleField("m", 2.0)));
                    writer.addDocument(Arrays.asList(doubleField("s", 1), doubleField("m", 3.0)));
                },
                doubleFields());
        assertThat(result.getSortOrder(), equalTo(SortOrder.DESC));
        assertThat(result.getSortValue(), equalTo(SortValue.from(2.0)));
        assertThat(result.getMetricValue(), equalTo(2.0d));
    }

    public void testSortByScriptAscending() throws IOException {
        TopMetricsAggregationBuilder builder = simpleBuilder(scriptSortOnS().order(SortOrder.ASC));
        InternalTopMetrics result = collect(builder, new MatchAllDocsQuery(), writer -> {
                    writer.addDocument(Arrays.asList(doubleField("s", 2), doubleField("m", 2.0)));
                    writer.addDocument(Arrays.asList(doubleField("s", 1), doubleField("m", 3.0)));
                },
                doubleFields());
        assertThat(result.getSortOrder(), equalTo(SortOrder.ASC));
        assertThat(result.getSortValue(), equalTo(SortValue.from(1.0)));
        assertThat(result.getMetricValue(), equalTo(3.0d));
    }

    public void testSortByStringScriptFails() throws IOException {
        Script script = new Script(ScriptType.INLINE, MockScriptEngine.NAME, "s", emptyMap());
        TopMetricsAggregationBuilder builder = simpleBuilder(new ScriptSortBuilder(script, ScriptSortType.STRING));
        Exception e = expectThrows(IllegalArgumentException.class, () -> collect(builder, boostFoo(), writer -> {
                    writer.addDocument(Arrays.asList(textField("s", "foo"), doubleField("m", 2.0)));
                    writer.addDocument(Arrays.asList(textField("s", "bar"), doubleField("m", 3.0)));
                },
                textAndDoubleField()));
        assertThat(e.getMessage(), equalTo("unsupported sort: only supported on numeric values"));
    }

    private InternalTopMetrics collectFromNewYorkAndLA(TopMetricsAggregationBuilder builder) throws IOException {
        return collect(builder, new MatchAllDocsQuery(), writer -> {
            writer.addDocument(Arrays.asList(geoPointField("s", 40.7128, -74.0060), doubleField("m", 2.0)));
            writer.addDocument(Arrays.asList(geoPointField("s", 34.0522, -118.2437), doubleField("m", 3.0)));
        },
        geoPointAndDoubleField());
    }

    public void testSortByGeoDistancDescending() throws IOException {
        TopMetricsAggregationBuilder builder = simpleBuilder(new GeoDistanceSortBuilder("s", 35.7796, 78.6382).order(SortOrder.DESC));
        InternalTopMetrics result = collectFromNewYorkAndLA(builder); 
        assertThat(result.getSortOrder(), equalTo(SortOrder.DESC));
        assertThat(result.getSortValue(), equalTo(SortValue.from(1.2054632268631617E7)));
        assertThat(result.getMetricValue(), equalTo(3.0d));
    }

    public void testSortByGeoDistanceAscending() throws IOException {
        TopMetricsAggregationBuilder builder = simpleBuilder(new GeoDistanceSortBuilder("s", 35.7796, 78.6382).order(SortOrder.ASC));
        InternalTopMetrics result = collectFromNewYorkAndLA(builder); 
        assertThat(result.getSortOrder(), equalTo(SortOrder.ASC));
        assertThat(result.getSortValue(), equalTo(SortValue.from(1.1062351376961706E7)));
        assertThat(result.getMetricValue(), equalTo(2.0d));
    }


    public void testBuckets() throws IOException {
        TopMetricsAggregationBuilder builder = simpleBuilder(new FieldSortBuilder("s").order(SortOrder.ASC));
        TermsAggregationBuilder terms = new TermsAggregationBuilder("terms", ValueType.DOUBLE).field("c").subAggregation(builder);
        Terms result = (Terms) collect(terms, new MatchAllDocsQuery(), writer -> {
                    writer.addDocument(Arrays.asList(doubleField("c", 1.0), doubleField("s", 1.0), doubleField("m", 2.0)));
                    writer.addDocument(Arrays.asList(doubleField("c", 1.0), doubleField("s", 2.0), doubleField("m", 3.0)));
                    writer.addDocument(Arrays.asList(doubleField("c", 2.0), doubleField("s", 4.0), doubleField("m", 9.0)));
                },
                numberFieldType(NumberType.DOUBLE, "c"), numberFieldType(NumberType.DOUBLE, "s"), numberFieldType(NumberType.DOUBLE, "m"));
        Terms.Bucket bucket1 = result.getBuckets().get(0);
        assertThat(bucket1.getKey(), equalTo(1.0));
        InternalTopMetrics top1 = bucket1.getAggregations().get("test");
        assertThat(top1.getSortOrder(), equalTo(SortOrder.ASC));
        assertThat(top1.getSortValue(), equalTo(SortValue.from(1.0)));
        assertThat(top1.getMetricValue(), equalTo(2.0d));
        Terms.Bucket bucket2 = result.getBuckets().get(1);
        assertThat(bucket2.getKey(), equalTo(2.0));
        InternalTopMetrics top2 = bucket2.getAggregations().get("test");
        assertThat(top2.getSortOrder(), equalTo(SortOrder.ASC));
        assertThat(top2.getSortValue(), equalTo(SortValue.from(4.0)));
        assertThat(top2.getMetricValue(), equalTo(9.0d));
    }

    private TopMetricsAggregationBuilder simpleBuilder(SortBuilder<?> sort) {
        return new TopMetricsAggregationBuilder("test", singletonList(sort),
                new MultiValuesSourceFieldConfig.Builder().setFieldName("m").build());
    }

    private TopMetricsAggregationBuilder simpleBuilder() {
        return simpleBuilder(new FieldSortBuilder("s"));
    }

    /**
     * Build a query that matches all documents but adds 1 to the score of
     * all docs that contain "foo". We use this instead of a term query
     * directly because the score that can come from the term query can
     * very quite a bit but this is super predictable.
     */
    private Query boostFoo() {
        return new BooleanQuery.Builder()
                .add(new BooleanClause(new MatchAllDocsQuery(), Occur.MUST))
                .add(new BooleanClause(new BoostQuery(new ConstantScoreQuery(new TermQuery(new Term("s", "foo"))), 1.0f), Occur.SHOULD))
                .build();
    }

    private MappedFieldType[] doubleFields() {
        return new MappedFieldType[] {numberFieldType(NumberType.DOUBLE, "s"), numberFieldType(NumberType.DOUBLE, "m")};
    }

    private MappedFieldType[] floatAndDoubleField() {
        return new MappedFieldType[] {numberFieldType(NumberType.FLOAT, "s"), numberFieldType(NumberType.DOUBLE, "m")};
    }

    private MappedFieldType[] longAndDoubleField() {
        return new MappedFieldType[] {numberFieldType(NumberType.LONG, "s"), numberFieldType(NumberType.DOUBLE, "m")};
    }

    private MappedFieldType[] textAndDoubleField() {
        return new MappedFieldType[] {textFieldType("s"), numberFieldType(NumberType.DOUBLE, "m")};
    }

    private MappedFieldType[] geoPointAndDoubleField() {
        return new MappedFieldType[] {geoPointFieldType("s"), numberFieldType(NumberType.DOUBLE, "m")};
    }

    private MappedFieldType numberFieldType(NumberType numberType, String name) {
        NumberFieldMapper.NumberFieldType type = new NumberFieldMapper.NumberFieldType(numberType);
        type.setName(name);
        return type;
    }

    private MappedFieldType textFieldType(String name) {
        TextFieldMapper.TextFieldType type = new TextFieldMapper.TextFieldType();
        type.setName(name);
        return type;
    }

    private MappedFieldType geoPointFieldType(String name) {
        GeoPointFieldMapper.GeoPointFieldType type = new GeoPointFieldMapper.GeoPointFieldType();
        type.setName(name);
        type.setHasDocValues(true);
        return type;
    }

    private IndexableField doubleField(String name, double value) {
        return new SortedNumericDocValuesField(name, NumericUtils.doubleToSortableLong(value));
    }

    private IndexableField floatField(String name, float value) {
        return new SortedNumericDocValuesField(name, NumericUtils.floatToSortableInt(value));
    }

    private IndexableField longField(String name, long value) {
        return new SortedNumericDocValuesField(name, value);
    }

    private IndexableField textField(String name, String value) {
        return new Field(name, value, textFieldType(name));
    }

    private IndexableField geoPointField(String name, double lat, double lon) {
        return new LatLonDocValuesField(name, lat, lon);
    }

    private InternalTopMetrics collect(TopMetricsAggregationBuilder builder, Query query,
            CheckedConsumer<RandomIndexWriter, IOException> buildIndex, MappedFieldType... fields) throws IOException {
        InternalTopMetrics result = (InternalTopMetrics) collect((AggregationBuilder) builder, query, buildIndex, fields);
        assertThat(result.getSortFormat(), equalTo(DocValueFormat.RAW));
        assertThat(result.getMetricName(), equalTo(builder.getMetricField().getFieldName()));
        return result;
    }

    private InternalAggregation collect(AggregationBuilder builder, Query query,
            CheckedConsumer<RandomIndexWriter, IOException> buildIndex, MappedFieldType... fields) throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                buildIndex.accept(indexWriter);
            }

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newSearcher(indexReader, true, true);
                return search(indexSearcher, query, builder, fields);
            }
        }
    }

    /**
     * Builds a simple script that reads the "s" field.
     */
    private ScriptSortBuilder scriptSortOnS() {
        return new ScriptSortBuilder(new Script(ScriptType.INLINE, MockScriptEngine.NAME, "s", emptyMap()), ScriptSortType.NUMBER);
    }

    @Override
    protected ScriptService getMockScriptService() {
        MockScriptEngine scriptEngine = new MockScriptEngine(MockScriptEngine.NAME,
                singletonMap("s", args -> {
                    @SuppressWarnings("unchecked")
                    Map<String, ScriptDocValues<?>> fields = (Map<String, ScriptDocValues<?>>) args.get("doc");
                    ScriptDocValues.Doubles field = (ScriptDocValues.Doubles) fields.get("s");
                    return field.getValue();
                }),
                emptyMap());
        Map<String, ScriptEngine> engines = singletonMap(scriptEngine.getType(), scriptEngine);
        return new ScriptService(Settings.EMPTY, engines, ScriptModule.CORE_CONTEXTS);
    }
}
