/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.exponentialhistogram.aggregations.metrics;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Min;
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xpack.exponentialhistogram.ExponentialHistogramFieldMapper;
import org.elasticsearch.xpack.exponentialhistogram.aggregations.ExponentialHistogramAggregatorTestCase;
import org.elasticsearch.xpack.exponentialhistogram.aggregations.support.ExponentialHistogramValuesSourceType;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;

public class ExponentialHistogramMinAggregatorTests extends ExponentialHistogramAggregatorTestCase {

    private static final String FIELD_NAME = "my_histogram";

    public void testMatchesNumericDocValues() throws IOException {

        List<ExponentialHistogram> histograms = createRandomHistograms(randomIntBetween(1, 1000));
        boolean anyNonEmpty = histograms.stream().anyMatch(histo -> histo.valueCount() > 0);

        double expectedMin = histograms.stream()
            .mapToDouble(ExponentialHistogram::min)
            .filter(val -> Double.isNaN(val) == false)
            .min()
            .orElse(Double.POSITIVE_INFINITY);

        testCase(Queries.ALL_DOCS_INSTANCE, iw -> histograms.forEach(histo -> addHistogramDoc(iw, FIELD_NAME, histo)), min -> {
            assertThat(min.value(), equalTo(expectedMin));
            assertThat(AggregationInspectionHelper.hasValue(min), equalTo(anyNonEmpty));
        });
    }

    public void testNoDocs() throws IOException {
        testCase(Queries.ALL_DOCS_INSTANCE, iw -> {
            // Intentionally not writing any docs
        }, min -> {
            assertThat(min.value(), equalTo(Double.POSITIVE_INFINITY));
            assertThat(AggregationInspectionHelper.hasValue(min), equalTo(false));
        });
    }

    public void testNoMatchingField() throws IOException {
        List<ExponentialHistogram> histograms = createRandomHistograms(10);
        testCase(Queries.ALL_DOCS_INSTANCE, iw -> histograms.forEach(histo -> addHistogramDoc(iw, "wrong_field", histo)), min -> {
            assertThat(min.value(), equalTo(Double.POSITIVE_INFINITY));
            assertThat(AggregationInspectionHelper.hasValue(min), equalTo(false));
        });
    }

    public void testQueryFiltering() throws IOException {
        List<Map.Entry<ExponentialHistogram, Boolean>> histogramsWithFilter = createRandomHistograms(10).stream()
            .map(histo -> Map.entry(histo, randomBoolean()))
            .toList();

        boolean anyMatch = histogramsWithFilter.stream().filter(entry -> entry.getKey().valueCount() > 0).anyMatch(Map.Entry::getValue);
        double filteredMin = histogramsWithFilter.stream()
            .filter(Map.Entry::getValue)
            .filter(entry -> entry.getKey().valueCount() > 0)
            .mapToDouble(entry -> entry.getKey().min())
            .min()
            .orElse(Double.POSITIVE_INFINITY);

        testCase(
            new TermQuery(new Term("match", "yes")),
            iw -> histogramsWithFilter.forEach(
                entry -> addHistogramDoc(
                    iw,
                    FIELD_NAME,
                    entry.getKey(),
                    new StringField("match", entry.getValue() ? "yes" : "no", Field.Store.NO)
                )
            ),
            min -> {
                assertThat(min.value(), equalTo(filteredMin));
                assertThat(AggregationInspectionHelper.hasValue(min), equalTo(anyMatch));
            }
        );
    }

    private void testCase(Query query, CheckedConsumer<RandomIndexWriter, IOException> buildIndex, Consumer<Min> verify)
        throws IOException {
        var fieldType = new ExponentialHistogramFieldMapper.ExponentialHistogramFieldType(FIELD_NAME, Collections.emptyMap(), null);
        AggregationBuilder aggregationBuilder = createAggBuilderForTypeTest(fieldType, FIELD_NAME);
        testCase(buildIndex, verify, new AggTestConfig(aggregationBuilder, fieldType).withQuery(query));
    }

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        return new MinAggregationBuilder("min_agg").field(fieldName);
    }

    @Override
    protected List<ValuesSourceType> getSupportedValuesSourceTypes() {
        return List.of(
            CoreValuesSourceType.NUMERIC,
            CoreValuesSourceType.DATE,
            CoreValuesSourceType.BOOLEAN,
            ExponentialHistogramValuesSourceType.EXPONENTIAL_HISTOGRAM
        );
    }
}
