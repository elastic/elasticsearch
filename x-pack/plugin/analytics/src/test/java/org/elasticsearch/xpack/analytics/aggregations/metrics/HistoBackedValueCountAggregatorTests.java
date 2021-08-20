/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.analytics.aggregations.metrics;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.metrics.InternalValueCount;
import org.elasticsearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;
import org.elasticsearch.xpack.analytics.aggregations.support.AnalyticsValuesSourceType;
import org.elasticsearch.xpack.analytics.mapper.HistogramFieldMapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static java.util.Collections.singleton;
import static org.elasticsearch.search.aggregations.AggregationBuilders.count;
import static org.elasticsearch.xpack.analytics.AnalyticsTestsUtils.histogramFieldDocValues;

public class HistoBackedValueCountAggregatorTests extends AggregatorTestCase {

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
            iw.addDocument(singleton(histogramFieldDocValues("wrong_field", new double[] { 3, 1.2, 10 })));
            iw.addDocument(singleton(histogramFieldDocValues("wrong_field", new double[] { 5.3, 6, 20 })));
        }, count -> {
            assertEquals(0L, count.getValue());
            assertFalse(AggregationInspectionHelper.hasValue(count));
        });
    }

    public void testSimpleHistogram() throws IOException {
        testCase(new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(histogramFieldDocValues(FIELD_NAME, new double[] { 3, 1.2, 10 })));
            iw.addDocument(singleton(histogramFieldDocValues(FIELD_NAME, new double[] { 5.3, 6, 6, 20 })));
            iw.addDocument(singleton(histogramFieldDocValues(FIELD_NAME, new double[] { -10, 0.01, 1, 90 })));
        }, count -> {
            assertEquals(11, count.getValue());
            assertTrue(AggregationInspectionHelper.hasValue(count));
        });
    }

    public void testQueryFiltering() throws IOException {
        testCase(new TermQuery(new Term("match", "yes")), iw -> {
            iw.addDocument(
                Arrays.asList(
                    new StringField("match", "yes", Field.Store.NO),
                    histogramFieldDocValues(FIELD_NAME, new double[] { 3, 1.2, 10 })
                )
            );
            iw.addDocument(
                Arrays.asList(
                    new StringField("match", "yes", Field.Store.NO),
                    histogramFieldDocValues(FIELD_NAME, new double[] { 5.3, 6, 20 })
                )
            );
            iw.addDocument(
                Arrays.asList(
                    new StringField("match", "no", Field.Store.NO),
                    histogramFieldDocValues(FIELD_NAME, new double[] { 3, 1.2, 10 })
                )
            );
            iw.addDocument(
                Arrays.asList(
                    new StringField("match", "no", Field.Store.NO),
                    histogramFieldDocValues(FIELD_NAME, new double[] { 3, 1.2, 10 })
                )
            );
            iw.addDocument(
                Arrays.asList(
                    new StringField("match", "yes", Field.Store.NO),
                    histogramFieldDocValues(FIELD_NAME, new double[] { -10, 0.01, 1, 90 })
                )
            );
        }, count -> {
            assertEquals(10, count.getValue());
            assertTrue(AggregationInspectionHelper.hasValue(count));
        });
    }

    private void testCase(Query query, CheckedConsumer<RandomIndexWriter, IOException> indexer, Consumer<InternalValueCount> verify)
        throws IOException {
        testCase(count("_name").field(FIELD_NAME), query, indexer, verify, defaultFieldType());
    }

    @Override
    protected List<SearchPlugin> getSearchPlugins() {
        return org.elasticsearch.core.List.of(new AnalyticsPlugin(Settings.EMPTY));
    }

    @Override
    protected List<ValuesSourceType> getSupportedValuesSourceTypes() {
        // Note: this is the same list as Core, plus Analytics
        return org.elasticsearch.core.List.of(
            CoreValuesSourceType.NUMERIC,
            CoreValuesSourceType.DATE,
            CoreValuesSourceType.BOOLEAN,
            CoreValuesSourceType.KEYWORD,
            CoreValuesSourceType.IP,
            CoreValuesSourceType.GEOPOINT,
            CoreValuesSourceType.RANGE,
            CoreValuesSourceType.BOOLEAN,
            CoreValuesSourceType.DATE,
            CoreValuesSourceType.IP,
            AnalyticsValuesSourceType.HISTOGRAM
        );
    }

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        return new ValueCountAggregationBuilder("_name").field(fieldName);
    }

    private MappedFieldType defaultFieldType() {
        return new HistogramFieldMapper.HistogramFieldType("field", Collections.emptyMap());
    }
}
