/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.analytics.aggregations.metrics;

import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.metrics.InternalTDigestPercentiles;
import org.elasticsearch.search.aggregations.metrics.PercentilesAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.PercentilesConfig;
import org.elasticsearch.search.aggregations.metrics.PercentilesMethod;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;
import org.elasticsearch.xpack.analytics.aggregations.support.AnalyticsValuesSourceType;
import org.elasticsearch.xpack.analytics.mapper.HistogramFieldMapper;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static java.util.Collections.singleton;
import static org.elasticsearch.xpack.analytics.AnalyticsTestsUtils.histogramFieldDocValues;

public class TDigestPreAggregatedPercentilesAggregatorTests extends AggregatorTestCase {

    @Override
    protected List<SearchPlugin> getSearchPlugins() {
        return List.of(new AnalyticsPlugin());
    }

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        var tdigestConfig = new PercentilesConfig.TDigest();
        if (randomBoolean()) {
            tdigestConfig.setCompression(randomDoubleBetween(50, 200, true));
        }
        if (randomBoolean()) {
            tdigestConfig.setOptimizeForAccuracy(randomBoolean());
        }
        return new PercentilesAggregationBuilder("tdigest_percentiles").field(fieldName).percentilesConfig(tdigestConfig);
    }

    @Override
    protected List<ValuesSourceType> getSupportedValuesSourceTypes() {
        // Note: this is the same list as Core, plus Analytics
        return List.of(
            CoreValuesSourceType.NUMERIC,
            CoreValuesSourceType.DATE,
            CoreValuesSourceType.BOOLEAN,
            AnalyticsValuesSourceType.HISTOGRAM
        );
    }

    public void testNoMatchingField() throws IOException {
        testCase(
            new MatchAllDocsQuery(),
            iw -> { iw.addDocument(singleton(histogramFieldDocValues("wrong_number", new double[] { 7, 1 }))); },
            hdr -> {
                // assertEquals(0L, hdr.state.getTotalCount());
                assertFalse(AggregationInspectionHelper.hasValue(hdr));
            }
        );
    }

    public void testEmptyField() throws IOException {
        testCase(new MatchAllDocsQuery(), iw -> { iw.addDocument(singleton(histogramFieldDocValues("number", new double[0]))); }, hdr -> {
            assertFalse(AggregationInspectionHelper.hasValue(hdr));
        });
    }

    public void testSomeMatchesBinaryDocValues() throws IOException {
        testCase(new FieldExistsQuery("number"), iw -> {
            iw.addDocument(singleton(histogramFieldDocValues("number", new double[] { 60, 40, 20, 10 })));
        }, hdr -> {
            // assertEquals(4L, hdr.state.getTotalCount());
            double approximation = 0.05d;
            assertEquals(17.5d, hdr.percentile(25), approximation);
            assertEquals(30.0d, hdr.percentile(50), approximation);
            assertEquals(45.0d, hdr.percentile(75), approximation);
            assertEquals(59.4d, hdr.percentile(99), approximation);
            assertTrue(AggregationInspectionHelper.hasValue(hdr));
        });
    }

    public void testSomeMatchesMultiBinaryDocValues() throws IOException {
        testCase(new FieldExistsQuery("number"), iw -> {
            iw.addDocument(singleton(histogramFieldDocValues("number", new double[] { 60, 40, 20, 10 })));
            iw.addDocument(singleton(histogramFieldDocValues("number", new double[] { 60, 40, 20, 10 })));
            iw.addDocument(singleton(histogramFieldDocValues("number", new double[] { 60, 40, 20, 10 })));
            iw.addDocument(singleton(histogramFieldDocValues("number", new double[] { 60, 40, 20, 10 })));
        }, hdr -> {
            // assertEquals(16L, hdr.state.getTotalCount());
            double approximation = 0.05d;
            assertEquals(17.5d, hdr.percentile(25), approximation);
            assertEquals(30.0d, hdr.percentile(50), approximation);
            assertEquals(45.0d, hdr.percentile(75), approximation);
            assertEquals(60.0d, hdr.percentile(99), approximation);
            assertTrue(AggregationInspectionHelper.hasValue(hdr));
        });
    }

    private void testCase(
        Query query,
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
        Consumer<InternalTDigestPercentiles> verify
    ) throws IOException {
        PercentilesAggregationBuilder builder = new PercentilesAggregationBuilder("test").field("number").method(PercentilesMethod.TDIGEST);

        MappedFieldType fieldType = new HistogramFieldMapper.HistogramFieldType("number", Collections.emptyMap());
        testCase(buildIndex, verify, new AggTestConfig(builder, fieldType).withQuery(query));
    }
}
