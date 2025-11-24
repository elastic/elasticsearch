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
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.InternalAvg;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xpack.exponentialhistogram.ExponentialHistogramFieldMapper;
import org.elasticsearch.xpack.exponentialhistogram.aggregations.ExponentialHistogramAggregatorTestCase;
import org.elasticsearch.xpack.exponentialhistogram.aggregations.support.ExponentialHistogramValuesSourceType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class ExponentialHistogramAvgAggregatorTests extends ExponentialHistogramAggregatorTestCase {

    private static final String FIELD_NAME = "my_histogram";

    public void testMatchesNumericDocValues() throws IOException {

        List<ExponentialHistogram> histograms = createRandomHistograms(randomIntBetween(1, 1000));

        double expectedSum = histograms.stream().mapToDouble(ExponentialHistogram::sum).sum();
        long expectedCount = histograms.stream().mapToLong(ExponentialHistogram::valueCount).sum();
        double expectedAvg = expectedSum / expectedCount;

        testCase(new MatchAllDocsQuery(), iw -> histograms.forEach(histo -> addHistogramDoc(iw, FIELD_NAME, histo)), avg -> {
            if (expectedCount > 0) {
                assertThat(avg.value(), closeTo(expectedAvg, 0.0001d));
                assertThat(AggregationInspectionHelper.hasValue(avg), equalTo(true));
            } else {
                assertThat(AggregationInspectionHelper.hasValue(avg), equalTo(false));
            }
        });
    }

    public void testNoDocs() throws IOException {
        testCase(new MatchAllDocsQuery(), iw -> {
            // Intentionally not writing any docs
        }, avg -> {
            assertThat(avg.value(), equalTo(Double.NaN));
            assertThat(AggregationInspectionHelper.hasValue(avg), equalTo(false));
        });
    }

    public void testNoMatchingField() throws IOException {
        List<ExponentialHistogram> histograms = createRandomHistograms(10);
        testCase(new MatchAllDocsQuery(), iw -> histograms.forEach(histo -> addHistogramDoc(iw, "wrong_field", histo)), avg -> {
            assertThat(avg.value(), equalTo(Double.NaN));
            assertThat(AggregationInspectionHelper.hasValue(avg), equalTo(false));
        });
    }

    public void testQueryFiltering() throws IOException {
        List<Map.Entry<ExponentialHistogram, Boolean>> histogramsWithFilter = new ArrayList<>();
        do {
            histogramsWithFilter.clear();
            createRandomHistograms(10).stream().map(histo -> Map.entry(histo, randomBoolean())).forEach(histogramsWithFilter::add);
        } while (histogramsWithFilter.stream().noneMatch(Map.Entry::getValue)); // ensure at least one matches

        double filteredSum = histogramsWithFilter.stream().filter(Map.Entry::getValue).mapToDouble(entry -> entry.getKey().sum()).sum();
        long filteredCnt = histogramsWithFilter.stream().filter(Map.Entry::getValue).mapToLong(entry -> entry.getKey().valueCount()).sum();
        double filteredAvg = filteredSum / filteredCnt;

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
            avg -> {
                if (filteredCnt > 0) {
                    assertThat(avg.value(), closeTo(filteredAvg, 0.0001d));
                    assertThat(AggregationInspectionHelper.hasValue(avg), equalTo(true));
                } else {
                    assertThat(AggregationInspectionHelper.hasValue(avg), equalTo(false));
                }
            }
        );
    }

    private void testCase(Query query, CheckedConsumer<RandomIndexWriter, IOException> buildIndex, Consumer<InternalAvg> verify)
        throws IOException {
        var fieldType = new ExponentialHistogramFieldMapper.ExponentialHistogramFieldType(FIELD_NAME, Collections.emptyMap(), null);
        AggregationBuilder aggregationBuilder = createAggBuilderForTypeTest(fieldType, FIELD_NAME);
        testCase(buildIndex, verify, new AggTestConfig(aggregationBuilder, fieldType).withQuery(query));
    }

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        return new AvgAggregationBuilder("avg_agg").field(fieldName);
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
