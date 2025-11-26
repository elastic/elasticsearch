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
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.InternalValueCount;
import org.elasticsearch.search.aggregations.metrics.ValueCountAggregationBuilder;
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

public class ExponentialHistogramValueCountAggregatorTests extends ExponentialHistogramAggregatorTestCase {

    private static final String FIELD_NAME = "my_histogram";

    public void testMatchesNumericDocValues() throws IOException {

        List<ExponentialHistogram> histograms = createRandomHistograms(randomIntBetween(1, 1000));

        long expectedCount = histograms.stream().mapToLong(ExponentialHistogram::valueCount).sum();

        testCase(new MatchAllDocsQuery(), iw -> histograms.forEach(histo -> addHistogramDoc(iw, FIELD_NAME, histo)), valueCount -> {
            assertThat(valueCount.getValue(), equalTo(expectedCount));
            assertThat(AggregationInspectionHelper.hasValue(valueCount), equalTo(expectedCount > 0));
        });
    }

    public void testNoDocs() throws IOException {
        testCase(new MatchAllDocsQuery(), iw -> {
            // Intentionally not writing any docs
        }, valueCount -> {
            assertThat(valueCount.getValue(), equalTo(0L));
            assertThat(AggregationInspectionHelper.hasValue(valueCount), equalTo(false));
        });
    }

    public void testNoMatchingField() throws IOException {
        List<ExponentialHistogram> histograms = createRandomHistograms(10);
        testCase(new MatchAllDocsQuery(), iw -> histograms.forEach(histo -> addHistogramDoc(iw, "wrong_field", histo)), sum -> {
            assertThat(sum.getValue(), equalTo(0L));
            assertThat(AggregationInspectionHelper.hasValue(sum), equalTo(false));
        });
    }

    public void testQueryFiltering() throws IOException {
        List<Map.Entry<ExponentialHistogram, Boolean>> histogramsWithFilter = createRandomHistograms(10).stream()
            .map(histo -> Map.entry(histo, randomBoolean()))
            .toList();

        long filteredCount = histogramsWithFilter.stream()
            .filter(Map.Entry::getValue)
            .mapToLong(entry -> entry.getKey().valueCount())
            .sum();

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
            sum -> {
                assertThat(sum.getValue(), equalTo(filteredCount));
                assertThat(AggregationInspectionHelper.hasValue(sum), equalTo(filteredCount > 0));
            }
        );
    }

    private void testCase(Query query, CheckedConsumer<RandomIndexWriter, IOException> buildIndex, Consumer<InternalValueCount> verify)
        throws IOException {
        var fieldType = new ExponentialHistogramFieldMapper.ExponentialHistogramFieldType(FIELD_NAME, Collections.emptyMap(), null);
        AggregationBuilder aggregationBuilder = createAggBuilderForTypeTest(fieldType, FIELD_NAME);
        testCase(buildIndex, verify, new AggTestConfig(aggregationBuilder, fieldType).withQuery(query));
    }

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        return new ValueCountAggregationBuilder("value_count_agg").field(fieldName);
    }

    @Override
    protected List<ValuesSourceType> getSupportedValuesSourceTypes() {
        return List.of(
            CoreValuesSourceType.NUMERIC,
            CoreValuesSourceType.KEYWORD,
            CoreValuesSourceType.GEOPOINT,
            CoreValuesSourceType.RANGE,
            CoreValuesSourceType.BOOLEAN,
            CoreValuesSourceType.DATE,
            CoreValuesSourceType.IP,
            ExponentialHistogramValuesSourceType.EXPONENTIAL_HISTOGRAM
        );
    }
}
