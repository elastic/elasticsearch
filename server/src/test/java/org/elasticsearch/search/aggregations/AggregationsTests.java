/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations;

import org.elasticsearch.search.aggregations.bucket.composite.InternalCompositeTests;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilterTests;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFiltersTests;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGridTests;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileGridTests;
import org.elasticsearch.search.aggregations.bucket.global.InternalGlobalTests;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalDateHistogramTests;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogramTests;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalVariableWidthHistogramTests;
import org.elasticsearch.search.aggregations.bucket.missing.InternalMissingTests;
import org.elasticsearch.search.aggregations.bucket.nested.InternalNestedTests;
import org.elasticsearch.search.aggregations.bucket.nested.InternalReverseNestedTests;
import org.elasticsearch.search.aggregations.bucket.range.InternalBinaryRangeTests;
import org.elasticsearch.search.aggregations.bucket.range.InternalDateRangeTests;
import org.elasticsearch.search.aggregations.bucket.range.InternalGeoDistanceTests;
import org.elasticsearch.search.aggregations.bucket.range.InternalRangeTests;
import org.elasticsearch.search.aggregations.bucket.sampler.InternalSamplerTests;
import org.elasticsearch.search.aggregations.bucket.terms.DoubleTermsTests;
import org.elasticsearch.search.aggregations.bucket.terms.LongRareTermsTests;
import org.elasticsearch.search.aggregations.bucket.terms.LongTermsTests;
import org.elasticsearch.search.aggregations.bucket.terms.SignificantLongTermsTests;
import org.elasticsearch.search.aggregations.bucket.terms.SignificantStringTermsTests;
import org.elasticsearch.search.aggregations.bucket.terms.StringRareTermsTests;
import org.elasticsearch.search.aggregations.bucket.terms.StringTermsTests;
import org.elasticsearch.search.aggregations.metrics.InternalAvgTests;
import org.elasticsearch.search.aggregations.metrics.InternalCardinalityTests;
import org.elasticsearch.search.aggregations.metrics.InternalExtendedStatsTests;
import org.elasticsearch.search.aggregations.metrics.InternalGeoBoundsTests;
import org.elasticsearch.search.aggregations.metrics.InternalGeoCentroidTests;
import org.elasticsearch.search.aggregations.metrics.InternalHDRPercentilesRanksTests;
import org.elasticsearch.search.aggregations.metrics.InternalHDRPercentilesTests;
import org.elasticsearch.search.aggregations.metrics.InternalMedianAbsoluteDeviationTests;
import org.elasticsearch.search.aggregations.metrics.InternalScriptedMetricTests;
import org.elasticsearch.search.aggregations.metrics.InternalStatsBucketTests;
import org.elasticsearch.search.aggregations.metrics.InternalStatsTests;
import org.elasticsearch.search.aggregations.metrics.InternalTDigestPercentilesRanksTests;
import org.elasticsearch.search.aggregations.metrics.InternalTDigestPercentilesTests;
import org.elasticsearch.search.aggregations.metrics.InternalTopHitsTests;
import org.elasticsearch.search.aggregations.metrics.InternalValueCountTests;
import org.elasticsearch.search.aggregations.metrics.InternalWeightedAvgTests;
import org.elasticsearch.search.aggregations.metrics.MaxTests;
import org.elasticsearch.search.aggregations.metrics.MinTests;
import org.elasticsearch.search.aggregations.metrics.SumTests;
import org.elasticsearch.search.aggregations.pipeline.InternalBucketMetricValueTests;
import org.elasticsearch.search.aggregations.pipeline.InternalExtendedStatsBucketTests;
import org.elasticsearch.search.aggregations.pipeline.InternalPercentilesBucketTests;
import org.elasticsearch.search.aggregations.pipeline.InternalSimpleValueTests;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.elasticsearch.test.InternalMultiBucketAggregationTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;

/**
 * This class tests that aggregations parsing works properly. It checks that we can parse
 * different aggregations and adds sub-aggregations where applicable.
 *
 */
public class AggregationsTests extends ESTestCase {

    private static final List<InternalAggregationTestCase<?>> aggsTests = List.of(
        new InternalCardinalityTests(),
        new InternalTDigestPercentilesTests(),
        new InternalTDigestPercentilesRanksTests(),
        new InternalHDRPercentilesTests(),
        new InternalHDRPercentilesRanksTests(),
        new InternalPercentilesBucketTests(),
        new MinTests(),
        new MaxTests(),
        new InternalAvgTests(),
        new InternalWeightedAvgTests(),
        new SumTests(),
        new InternalValueCountTests(),
        new InternalSimpleValueTests(),
        new InternalBucketMetricValueTests(),
        new InternalStatsTests(),
        new InternalStatsBucketTests(),
        new InternalExtendedStatsTests(),
        new InternalExtendedStatsBucketTests(),
        new InternalGeoBoundsTests(),
        new InternalGeoCentroidTests(),
        new InternalHistogramTests(),
        new InternalDateHistogramTests(),
        new InternalVariableWidthHistogramTests(),
        new LongTermsTests(),
        new DoubleTermsTests(),
        new StringTermsTests(),
        new LongRareTermsTests(),
        new StringRareTermsTests(),
        new InternalMissingTests(),
        new InternalNestedTests(),
        new InternalReverseNestedTests(),
        new InternalGlobalTests(),
        new InternalFilterTests(),
        new InternalSamplerTests(),
        new GeoHashGridTests(),
        new GeoTileGridTests(),
        new InternalRangeTests(),
        new InternalDateRangeTests(),
        new InternalGeoDistanceTests(),
        new InternalFiltersTests(),
        new SignificantLongTermsTests(),
        new SignificantStringTermsTests(),
        new InternalScriptedMetricTests(),
        new InternalBinaryRangeTests(),
        new InternalTopHitsTests(),
        new InternalCompositeTests(),
        new InternalMedianAbsoluteDeviationTests()
    );

    @Before
    public void init() throws Exception {
        for (InternalAggregationTestCase<?> aggsTest : aggsTests) {
            if (aggsTest instanceof InternalMultiBucketAggregationTestCase) {
                // Lower down the number of buckets generated by multi bucket aggregation tests in
                // order to avoid too many aggregations to be created.
                ((InternalMultiBucketAggregationTestCase<?>) aggsTest).setMaxNumberOfBuckets(3);
            }
            aggsTest.setUp();
        }
    }

    @After
    public void cleanUp() throws Exception {
        for (InternalAggregationTestCase<?> aggsTest : aggsTests) {
            aggsTest.tearDown();
        }
    }

    public final InternalAggregations createTestInstance() {
        return createTestInstance(1, 0, 5);
    }

    private static InternalAggregations createTestInstance(final int minNumAggs, final int currentDepth, final int maxDepth) {
        int numAggs = randomIntBetween(minNumAggs, 4);
        List<InternalAggregation> aggs = new ArrayList<>(numAggs);
        for (int i = 0; i < numAggs; i++) {
            InternalAggregationTestCase<?> testCase = randomFrom(aggsTests);
            if (testCase instanceof InternalMultiBucketAggregationTestCase<?> multiBucketAggTestCase) {
                if (currentDepth < maxDepth) {
                    multiBucketAggTestCase.setSubAggregationsSupplier(() -> createTestInstance(0, currentDepth + 1, maxDepth));
                } else {
                    multiBucketAggTestCase.setSubAggregationsSupplier(() -> InternalAggregations.EMPTY);
                }
            } else if (testCase instanceof InternalSingleBucketAggregationTestCase<?> singleBucketAggTestCase) {
                if (currentDepth < maxDepth) {
                    singleBucketAggTestCase.subAggregationsSupplier = () -> createTestInstance(0, currentDepth + 1, maxDepth);
                } else {
                    singleBucketAggTestCase.subAggregationsSupplier = () -> InternalAggregations.EMPTY;
                }
            }
            aggs.add(testCase.createTestInstanceForXContent());
        }
        return InternalAggregations.from(aggs);
    }
}
