/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.topmetrics;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
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
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.mapper.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.MultiBucketConsumerService;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceFieldConfig;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.GeoDistanceSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.ScriptSortBuilder;
import org.elasticsearch.search.sort.ScriptSortBuilder.ScriptSortType;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.sort.SortValue;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TopMetricsAggregatorTests extends AggregatorTestCase {
    public void testNoDocs() throws IOException {
        InternalTopMetrics result = collect(simpleBuilder(), new MatchAllDocsQuery(), writer -> {}, doubleFields());
        assertThat(result.getSortOrder(), equalTo(SortOrder.ASC));
        assertThat(result.getTopMetrics(), equalTo(emptyList()));
    }

    public void testUnmappedMetric() throws IOException {
        InternalTopMetrics result = collect(
            simpleBuilder(),
            new MatchAllDocsQuery(),
            writer -> { writer.addDocument(singletonList(doubleField("s", 1.0))); },
            numberFieldType(NumberType.DOUBLE, "s")
        );
        assertThat(result.getSortOrder(), equalTo(SortOrder.ASC));
        assertThat(result.getTopMetrics(), hasSize(1));
        assertThat(result.getTopMetrics().get(0).getSortValue(), equalTo(SortValue.from(1.0)));
        assertThat(result.getTopMetrics().get(0).getMetricValues(), equalTo(singletonList(null)));
    }

    public void testMissingValueForDoubleMetric() throws IOException {
        InternalTopMetrics result = collect(
            simpleBuilder(),
            new MatchAllDocsQuery(),
            writer -> { writer.addDocument(singletonList(doubleField("s", 1.0))); },
            doubleFields()
        );
        assertThat(result.getSortOrder(), equalTo(SortOrder.ASC));
        assertThat(result.getTopMetrics(), hasSize(1));
        assertThat(result.getTopMetrics().get(0).getSortValue(), equalTo(SortValue.from(1.0)));
        assertThat(result.getTopMetrics().get(0).getMetricValues(), equalTo(singletonList(null)));
    }

    public void testMissingValueForLongMetric() throws IOException {
        InternalTopMetrics result = collect(
            simpleBuilder(),
            new MatchAllDocsQuery(),
            writer -> { writer.addDocument(singletonList(longField("s", 1))); },
            longFields()
        );
        assertThat(result.getSortOrder(), equalTo(SortOrder.ASC));
        assertThat(result.getTopMetrics(), hasSize(1));
        assertThat(result.getTopMetrics().get(0).getSortValue(), equalTo(SortValue.from(1)));
        assertThat(result.getTopMetrics().get(0).getMetricValues(), equalTo(singletonList(null)));
    }

    public void testActualValueForDoubleMetric() throws IOException {
        InternalTopMetrics result = collect(
            simpleBuilder(),
            new MatchAllDocsQuery(),
            writer -> { writer.addDocument(Arrays.asList(doubleField("s", 1.0), doubleField("m", 2.0))); },
            doubleFields()
        );
        assertThat(result.getSortOrder(), equalTo(SortOrder.ASC));
        assertThat(result.getTopMetrics(), equalTo(singletonList(top(1.0, 2.0))));
    }

    public void testActualValueForLongMetric() throws IOException {
        InternalTopMetrics result = collect(
            simpleBuilder(),
            new MatchAllDocsQuery(),
            writer -> { writer.addDocument(Arrays.asList(longField("s", 1), longField("m", 2))); },
            longFields()
        );
        assertThat(result.getSortOrder(), equalTo(SortOrder.ASC));
        assertThat(result.getTopMetrics(), equalTo(singletonList(top(1, 2))));
    }

    private InternalTopMetrics collectFromDoubles(TopMetricsAggregationBuilder builder) throws IOException {
        return collect(builder, new MatchAllDocsQuery(), writer -> {
            writer.addDocument(Arrays.asList(doubleField("s", 1.0), doubleField("m", 2.0)));
            writer.addDocument(Arrays.asList(doubleField("s", 2.0), doubleField("m", 3.0)));
        }, doubleFields());
    }

    public void testSortByDoubleAscending() throws IOException {
        InternalTopMetrics result = collectFromDoubles(simpleBuilder(new FieldSortBuilder("s").order(SortOrder.ASC)));
        assertThat(result.getSortOrder(), equalTo(SortOrder.ASC));
        assertThat(result.getTopMetrics(), equalTo(singletonList(top(1.0, 2.0))));
    }

    public void testSortByDoubleDescending() throws IOException {
        InternalTopMetrics result = collectFromDoubles(simpleBuilder(new FieldSortBuilder("s").order(SortOrder.DESC)));
        assertThat(result.getSortOrder(), equalTo(SortOrder.DESC));
        assertThat(result.getTopMetrics(), equalTo(singletonList(top(2.0, 3.0))));
    }

    public void testSortByDoubleCastToLong() throws IOException {
        InternalTopMetrics result = collectFromDoubles(simpleBuilder(new FieldSortBuilder("s").setNumericType("long")));
        assertThat(result.getSortOrder(), equalTo(SortOrder.ASC));
        assertThat(result.getTopMetrics(), equalTo(singletonList(top(1, 2.0))));
    }

    public void testSortByDoubleTwoHits() throws IOException {
        InternalTopMetrics result = collectFromDoubles(simpleBuilder(new FieldSortBuilder("s").order(SortOrder.ASC), 2));
        assertThat(result.getSortOrder(), equalTo(SortOrder.ASC));
        assertThat(result.getTopMetrics(), equalTo(List.of(top(1.0, 2.0), top(2.0, 3.0))));
    }

    public void testSortByFloatAscending() throws IOException {
        TopMetricsAggregationBuilder builder = simpleBuilder(new FieldSortBuilder("s").order(SortOrder.ASC));
        InternalTopMetrics result = collect(builder, new MatchAllDocsQuery(), writer -> {
            writer.addDocument(Arrays.asList(floatField("s", 1.0F), doubleField("m", 2.0)));
            writer.addDocument(Arrays.asList(floatField("s", 2.0F), doubleField("m", 3.0)));
        }, floatAndDoubleField());
        assertThat(result.getSortOrder(), equalTo(SortOrder.ASC));
        assertThat(result.getTopMetrics(), equalTo(singletonList(top(1.0, 2.0d))));
    }

    public void testSortByFloatDescending() throws IOException {
        TopMetricsAggregationBuilder builder = simpleBuilder(new FieldSortBuilder("s").order(SortOrder.DESC));
        InternalTopMetrics result = collect(builder, new MatchAllDocsQuery(), writer -> {
            writer.addDocument(Arrays.asList(floatField("s", 1.0F), doubleField("m", 2.0)));
            writer.addDocument(Arrays.asList(floatField("s", 2.0F), doubleField("m", 3.0)));
        }, floatAndDoubleField());
        assertThat(result.getSortOrder(), equalTo(SortOrder.DESC));
        assertThat(result.getTopMetrics(), equalTo(singletonList(top(2.0, 3.0))));
    }

    public void testSortByLongAscending() throws IOException {
        TopMetricsAggregationBuilder builder = simpleBuilder(new FieldSortBuilder("s").order(SortOrder.ASC));
        InternalTopMetrics result = collect(builder, new MatchAllDocsQuery(), writer -> {
            writer.addDocument(Arrays.asList(longField("s", 10), doubleField("m", 2.0)));
            writer.addDocument(Arrays.asList(longField("s", 20), doubleField("m", 3.0)));
        }, longAndDoubleField());
        assertThat(result.getSortOrder(), equalTo(SortOrder.ASC));
        assertThat(result.getTopMetrics(), equalTo(singletonList(top(10, 2.0))));
    }

    public void testSortByLongDescending() throws IOException {
        TopMetricsAggregationBuilder builder = simpleBuilder(new FieldSortBuilder("s").order(SortOrder.DESC));
        InternalTopMetrics result = collect(builder, new MatchAllDocsQuery(), writer -> {
            writer.addDocument(Arrays.asList(longField("s", 10), doubleField("m", 2.0)));
            writer.addDocument(Arrays.asList(longField("s", 20), doubleField("m", 3.0)));
        }, longAndDoubleField());
        assertThat(result.getSortOrder(), equalTo(SortOrder.DESC));
        assertThat(result.getTopMetrics(), equalTo(singletonList(top(20, 3.0))));
    }

    public void testSortByScoreDescending() throws IOException {
        TopMetricsAggregationBuilder builder = simpleBuilder(new ScoreSortBuilder().order(SortOrder.DESC));
        InternalTopMetrics result = collect(builder, boostFoo(), writer -> {
            writer.addDocument(Arrays.asList(textField("s", "foo"), doubleField("m", 2.0)));
            writer.addDocument(Arrays.asList(textField("s", "bar"), doubleField("m", 3.0)));
        }, textAndDoubleField());
        assertThat(result.getSortOrder(), equalTo(SortOrder.DESC));
        assertThat(result.getTopMetrics(), equalTo(singletonList(top(2.0, 2.0))));
    }

    public void testSortByScoreAscending() throws IOException {
        TopMetricsAggregationBuilder builder = simpleBuilder(new ScoreSortBuilder().order(SortOrder.ASC));
        InternalTopMetrics result = collect(builder, boostFoo(), writer -> {
            writer.addDocument(Arrays.asList(textField("s", "foo"), doubleField("m", 2.0)));
            writer.addDocument(Arrays.asList(textField("s", "bar"), doubleField("m", 3.0)));
        }, textAndDoubleField());
        assertThat(result.getSortOrder(), equalTo(SortOrder.ASC));
        assertThat(result.getTopMetrics(), equalTo(singletonList(top(1.0, 3.0))));
    }

    public void testSortByScriptDescending() throws IOException {
        TopMetricsAggregationBuilder builder = simpleBuilder(scriptSortOnS().order(SortOrder.DESC));
        InternalTopMetrics result = collect(builder, new MatchAllDocsQuery(), writer -> {
            writer.addDocument(Arrays.asList(doubleField("s", 2), doubleField("m", 2.0)));
            writer.addDocument(Arrays.asList(doubleField("s", 1), doubleField("m", 3.0)));
        }, doubleFields());
        assertThat(result.getSortOrder(), equalTo(SortOrder.DESC));
        assertThat(result.getTopMetrics(), equalTo(singletonList(top(2.0, 2.0))));
    }

    public void testSortByScriptAscending() throws IOException {
        TopMetricsAggregationBuilder builder = simpleBuilder(scriptSortOnS().order(SortOrder.ASC));
        InternalTopMetrics result = collect(builder, new MatchAllDocsQuery(), writer -> {
            writer.addDocument(Arrays.asList(doubleField("s", 2), doubleField("m", 2.0)));
            writer.addDocument(Arrays.asList(doubleField("s", 1), doubleField("m", 3.0)));
        }, doubleFields());
        assertThat(result.getSortOrder(), equalTo(SortOrder.ASC));
        assertThat(result.getTopMetrics(), equalTo(singletonList(top(1.0, 3.0))));
    }

    public void testSortByStringScriptFails() throws IOException {
        Script script = new Script(ScriptType.INLINE, MockScriptEngine.NAME, "s", emptyMap());
        TopMetricsAggregationBuilder builder = simpleBuilder(new ScriptSortBuilder(script, ScriptSortType.STRING));
        Exception e = expectThrows(IllegalArgumentException.class, () -> collect(builder, boostFoo(), writer -> {
            writer.addDocument(Arrays.asList(textField("s", "foo"), doubleField("m", 2.0)));
            writer.addDocument(Arrays.asList(textField("s", "bar"), doubleField("m", 3.0)));
        }, textAndDoubleField()));
        assertThat(
            e.getMessage(),
            equalTo("error building sort for [_script]: script sorting only supported on [numeric] scripts but was [string]")
        );
    }

    private InternalTopMetrics collectFromNewYorkAndLA(TopMetricsAggregationBuilder builder) throws IOException {
        return collect(builder, new MatchAllDocsQuery(), writer -> {
            writer.addDocument(Arrays.asList(geoPointField("s", 40.7128, -74.0060), doubleField("m", 2.0)));
            writer.addDocument(Arrays.asList(geoPointField("s", 34.0522, -118.2437), doubleField("m", 3.0)));
        }, geoPointAndDoubleField());
    }

    public void testSortByGeoDistancDescending() throws IOException {
        TopMetricsAggregationBuilder builder = simpleBuilder(new GeoDistanceSortBuilder("s", 35.7796, 78.6382).order(SortOrder.DESC));
        InternalTopMetrics result = collectFromNewYorkAndLA(builder);
        assertThat(result.getSortOrder(), equalTo(SortOrder.DESC));
        assertThat(result.getTopMetrics(), equalTo(singletonList(top(1.2054632268631617E7, 3.0))));
    }

    public void testSortByGeoDistanceAscending() throws IOException {
        TopMetricsAggregationBuilder builder = simpleBuilder(new GeoDistanceSortBuilder("s", 35.7796, 78.6382).order(SortOrder.ASC));
        InternalTopMetrics result = collectFromNewYorkAndLA(builder);
        assertThat(result.getSortOrder(), equalTo(SortOrder.ASC));
        assertThat(result.getTopMetrics(), equalTo(singletonList(top(1.1062351376961706E7, 2.0))));
    }

    public void testSortByGeoDistanceTwoHits() throws IOException {
        TopMetricsAggregationBuilder builder = simpleBuilder(new GeoDistanceSortBuilder("s", 35.7796, 78.6382).order(SortOrder.DESC), 2);
        InternalTopMetrics result = collectFromNewYorkAndLA(builder);
        assertThat(result.getSize(), equalTo(2));
        assertThat(result.getSortOrder(), equalTo(SortOrder.DESC));
        assertThat(result.getTopMetrics(), equalTo(List.of(top(1.2054632268631617E7, 3.0), top(1.1062351376961706E7, 2.0))));
    }

    public void testInsideTerms() throws IOException {
        TopMetricsAggregationBuilder builder = simpleBuilder(new FieldSortBuilder("s").order(SortOrder.ASC));
        TermsAggregationBuilder terms = new TermsAggregationBuilder("terms").field("c").subAggregation(builder);
        Terms result = (Terms) collect(terms, new MatchAllDocsQuery(), writer -> {
            writer.addDocument(Arrays.asList(doubleField("c", 1.0), doubleField("s", 1.0), doubleField("m", 2.0)));
            writer.addDocument(Arrays.asList(doubleField("c", 1.0), doubleField("s", 2.0), doubleField("m", 3.0)));
            writer.addDocument(Arrays.asList(doubleField("c", 2.0), doubleField("s", 4.0), doubleField("m", 9.0)));
        }, numberFieldType(NumberType.DOUBLE, "c"), numberFieldType(NumberType.DOUBLE, "s"), numberFieldType(NumberType.DOUBLE, "m"));
        Terms.Bucket bucket1 = result.getBuckets().get(0);
        assertThat(bucket1.getKey(), equalTo(1.0));
        InternalTopMetrics top1 = bucket1.getAggregations().get("test");
        assertThat(top1.getSortOrder(), equalTo(SortOrder.ASC));
        assertThat(top1.getTopMetrics(), equalTo(singletonList(top(1.0, 2.0))));
        Terms.Bucket bucket2 = result.getBuckets().get(1);
        assertThat(bucket2.getKey(), equalTo(2.0));
        InternalTopMetrics top2 = bucket2.getAggregations().get("test");
        assertThat(top2.getSortOrder(), equalTo(SortOrder.ASC));
        assertThat(top2.getTopMetrics(), equalTo(singletonList(top(4.0, 9.0))));
    }

    public void testTonsOfBucketsTriggersBreaker() throws IOException {
        // Build a "simple" circuit breaker that trips at 20k
        CircuitBreakerService breaker = mock(CircuitBreakerService.class);
        ByteSizeValue max = new ByteSizeValue(20, ByteSizeUnit.KB);
        when(breaker.getBreaker(CircuitBreaker.REQUEST)).thenReturn(new MockBigArrays.LimitedBreaker(CircuitBreaker.REQUEST, max));

        // Collect some buckets with it
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), directory)) {
                writer.addDocument(Arrays.asList(doubleField("s", 1.0), doubleField("m", 2.0)));
            }

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newSearcher(indexReader, false, false);
                TopMetricsAggregationBuilder builder = simpleBuilder(new FieldSortBuilder("s").order(SortOrder.ASC));
                AggregationContext context = createAggregationContext(
                    indexSearcher,
                    createIndexSettings(),
                    new MatchAllDocsQuery(),
                    breaker,
                    builder.bytesToPreallocate(),
                    MultiBucketConsumerService.DEFAULT_MAX_BUCKETS,
                    false,
                    doubleFields()
                );
                Aggregator aggregator = builder.build(context, null).create(null, CardinalityUpperBound.ONE);
                aggregator.preCollection();
                assertThat(indexReader.leaves(), hasSize(1));
                LeafBucketCollector leaf = aggregator.getLeafCollector(indexReader.leaves().get(0));

                /*
                 * Collect some number of buckets that we *know* fit in the
                 * breaker. The number of buckets feels fairly arbitrary but
                 * it comes from:
                 * budget = 15k = 20k - 5k for the "default weight" of ever agg
                 * The 646th bucket causes a resize which requests puts the total
                 * just over 15k. This works out to more like 190 bits per bucket
                 * when we're fairly sure this should take about 129 bits per
                 * bucket. The difference is because, for arrays in of this size,
                 * BigArrays allocates the new array before freeing the old one.
                 * That causes us to trip when we're about 2/3 of the way to the
                 * limit. And 2/3 of 190 is 126. Which is pretty much what we
                 * expect. Sort of.
                 */
                int bucketThatBreaks = 646;
                for (int b = 0; b < bucketThatBreaks; b++) {
                    try {
                        leaf.collect(0, b);
                    } catch (CircuitBreakingException e) {
                        throw new AssertionError("Unexpected circuit break at [" + b + "]. Expected at [" + bucketThatBreaks + "]", e);
                    }
                }
                CircuitBreakingException e = expectThrows(CircuitBreakingException.class, () -> leaf.collect(0, bucketThatBreaks));
                assertThat(e.getMessage(), equalTo("test error"));
                assertThat(e.getByteLimit(), equalTo(max.getBytes()));
                assertThat(e.getBytesWanted(), equalTo(5872L));
            }
        }
    }

    public void testManyMetrics() throws IOException {
        List<SortBuilder<?>> sorts = singletonList(new FieldSortBuilder("s").order(SortOrder.ASC));
        TopMetricsAggregationBuilder builder = new TopMetricsAggregationBuilder(
            "test",
            sorts,
            1,
            List.of(
                new MultiValuesSourceFieldConfig.Builder().setFieldName("m1").build(),
                new MultiValuesSourceFieldConfig.Builder().setFieldName("m2").build(),
                new MultiValuesSourceFieldConfig.Builder().setFieldName("m3").build()
            )
        );
        InternalTopMetrics result = collect(builder, new MatchAllDocsQuery(), writer -> {
            writer.addDocument(Arrays.asList(doubleField("s", 1.0), doubleField("m1", 12.0), longField("m2", 22), doubleField("m3", 32.0)));
            writer.addDocument(Arrays.asList(doubleField("s", 2.0), doubleField("m1", 13.0), longField("m2", 23), doubleField("m3", 33.0)));
        }, manyMetricsFields());
        assertThat(result.getSortOrder(), equalTo(SortOrder.ASC));
        assertThat(
            result.getTopMetrics(),
            equalTo(
                singletonList(
                    new InternalTopMetrics.TopMetric(
                        DocValueFormat.RAW,
                        SortValue.from(1.0),
                        metricValues(SortValue.from(12.0), SortValue.from(22), SortValue.from(32.0))
                    )
                )
            )
        );
    }

    private TopMetricsAggregationBuilder simpleBuilder() {
        return simpleBuilder(new FieldSortBuilder("s"));
    }

    private TopMetricsAggregationBuilder simpleBuilder(SortBuilder<?> sort) {
        return simpleBuilder(sort, 1);
    }

    private TopMetricsAggregationBuilder simpleBuilder(SortBuilder<?> sort, int size) {
        return new TopMetricsAggregationBuilder(
            "test",
            singletonList(sort),
            size,
            singletonList(new MultiValuesSourceFieldConfig.Builder().setFieldName("m").build())
        );
    }

    /**
     * Build a query that matches all documents but adds 1 to the score of
     * all docs that contain "foo". We use this instead of a term query
     * directly because the score that can come from the term query can
     * very quite a bit but this is super predictable.
     */
    private Query boostFoo() {
        return new BooleanQuery.Builder().add(new BooleanClause(new MatchAllDocsQuery(), Occur.MUST))
            .add(new BooleanClause(new BoostQuery(new ConstantScoreQuery(new TermQuery(new Term("s", "foo"))), 1.0f), Occur.SHOULD))
            .build();
    }

    private MappedFieldType[] doubleFields() {
        return new MappedFieldType[] { numberFieldType(NumberType.DOUBLE, "s"), numberFieldType(NumberType.DOUBLE, "m") };
    }

    private MappedFieldType[] longFields() {
        return new MappedFieldType[] { numberFieldType(NumberType.LONG, "s"), numberFieldType(NumberType.LONG, "m") };
    }

    private MappedFieldType[] manyMetricsFields() {
        return new MappedFieldType[] {
            numberFieldType(NumberType.DOUBLE, "s"),
            numberFieldType(NumberType.DOUBLE, "m1"),
            numberFieldType(NumberType.LONG, "m2"),
            numberFieldType(NumberType.DOUBLE, "m3"), };
    }

    private MappedFieldType[] floatAndDoubleField() {
        return new MappedFieldType[] { numberFieldType(NumberType.FLOAT, "s"), numberFieldType(NumberType.DOUBLE, "m") };
    }

    private MappedFieldType[] longAndDoubleField() {
        return new MappedFieldType[] { numberFieldType(NumberType.LONG, "s"), numberFieldType(NumberType.DOUBLE, "m") };
    }

    private MappedFieldType[] textAndDoubleField() {
        return new MappedFieldType[] { textFieldType("s"), numberFieldType(NumberType.DOUBLE, "m") };
    }

    private MappedFieldType[] geoPointAndDoubleField() {
        return new MappedFieldType[] { geoPointFieldType("s"), numberFieldType(NumberType.DOUBLE, "m") };
    }

    private MappedFieldType numberFieldType(NumberType numberType, String name) {
        return new NumberFieldMapper.NumberFieldType(name, numberType);
    }

    private MappedFieldType textFieldType(String name) {
        return new TextFieldMapper.TextFieldType(name);
    }

    private MappedFieldType geoPointFieldType(String name) {
        return new GeoPointFieldMapper.GeoPointFieldType(name);
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
        return new TextField(name, value, Field.Store.NO);
    }

    private IndexableField geoPointField(String name, double lat, double lon) {
        return new LatLonDocValuesField(name, lat, lon);
    }

    private InternalTopMetrics collect(
        TopMetricsAggregationBuilder builder,
        Query query,
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
        MappedFieldType... fields
    ) throws IOException {
        InternalTopMetrics result = (InternalTopMetrics) collect((AggregationBuilder) builder, query, buildIndex, fields);
        List<String> expectedFieldNames = builder.getMetricFields()
            .stream()
            .map(MultiValuesSourceFieldConfig::getFieldName)
            .collect(toList());
        assertThat(result.getMetricNames(), equalTo(expectedFieldNames));
        return result;
    }

    private InternalAggregation collect(
        AggregationBuilder builder,
        Query query,
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
        MappedFieldType... fields
    ) throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                buildIndex.accept(indexWriter);
            }

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newSearcher(indexReader, true, true);
                InternalAggregation agg = searchAndReduce(indexSearcher, query, builder, fields);
                verifyOutputFieldNames(builder, agg);
                return agg;
            }
        }
    }

    private InternalTopMetrics.TopMetric top(long sortValue, double... metricValues) {
        return new InternalTopMetrics.TopMetric(DocValueFormat.RAW, SortValue.from(sortValue), metricValues(metricValues));
    }

    private InternalTopMetrics.TopMetric top(long sortValue, long... metricValues) {
        return new InternalTopMetrics.TopMetric(DocValueFormat.RAW, SortValue.from(sortValue), metricValues(metricValues));
    }

    private InternalTopMetrics.TopMetric top(double sortValue, double... metricValues) {
        return new InternalTopMetrics.TopMetric(DocValueFormat.RAW, SortValue.from(sortValue), metricValues(metricValues));
    }

    private List<InternalTopMetrics.MetricValue> metricValues(double... metricValues) {
        return metricValues(Arrays.stream(metricValues).mapToObj(SortValue::from).toArray(SortValue[]::new));
    }

    private List<InternalTopMetrics.MetricValue> metricValues(long... metricValues) {
        return metricValues(Arrays.stream(metricValues).mapToObj(SortValue::from).toArray(SortValue[]::new));
    }

    private List<InternalTopMetrics.MetricValue> metricValues(SortValue... metricValues) {
        return Arrays.stream(metricValues).map(v -> new InternalTopMetrics.MetricValue(DocValueFormat.RAW, v)).collect(toList());
    }

    /**
     * Builds a simple script that reads the "s" field.
     */
    private ScriptSortBuilder scriptSortOnS() {
        return new ScriptSortBuilder(new Script(ScriptType.INLINE, MockScriptEngine.NAME, "s", emptyMap()), ScriptSortType.NUMBER);
    }

    @Override
    protected ScriptService getMockScriptService() {
        MockScriptEngine scriptEngine = new MockScriptEngine(MockScriptEngine.NAME, singletonMap("s", args -> {
            @SuppressWarnings("unchecked")
            Map<String, ScriptDocValues<?>> fields = (Map<String, ScriptDocValues<?>>) args.get("doc");
            ScriptDocValues.Doubles field = (ScriptDocValues.Doubles) fields.get("s");
            return field.getValue();
        }), emptyMap());
        Map<String, ScriptEngine> engines = singletonMap(scriptEngine.getType(), scriptEngine);
        return new ScriptService(Settings.EMPTY, engines, ScriptModule.CORE_CONTEXTS, () -> 1L);
    }

    @Override
    protected List<SearchPlugin> getSearchPlugins() {
        return Collections.singletonList(new AnalyticsPlugin());
    }
}
