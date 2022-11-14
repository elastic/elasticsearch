/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.support;

import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.InternalComposite;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilters;
import org.elasticsearch.search.aggregations.bucket.geogrid.InternalGeoGrid;
import org.elasticsearch.search.aggregations.bucket.global.InternalGlobal;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalVariableWidthHistogram;
import org.elasticsearch.search.aggregations.bucket.missing.InternalMissing;
import org.elasticsearch.search.aggregations.bucket.nested.InternalNested;
import org.elasticsearch.search.aggregations.bucket.nested.InternalReverseNested;
import org.elasticsearch.search.aggregations.bucket.range.InternalRange;
import org.elasticsearch.search.aggregations.bucket.sampler.InternalSampler;
import org.elasticsearch.search.aggregations.bucket.sampler.UnmappedSampler;
import org.elasticsearch.search.aggregations.bucket.terms.InternalSignificantTerms;
import org.elasticsearch.search.aggregations.bucket.terms.InternalTerms;
import org.elasticsearch.search.aggregations.bucket.terms.UnmappedSignificantTerms;
import org.elasticsearch.search.aggregations.bucket.terms.UnmappedTerms;
import org.elasticsearch.search.aggregations.metrics.InternalAvg;
import org.elasticsearch.search.aggregations.metrics.InternalBounds;
import org.elasticsearch.search.aggregations.metrics.InternalCardinality;
import org.elasticsearch.search.aggregations.metrics.InternalCentroid;
import org.elasticsearch.search.aggregations.metrics.InternalExtendedStats;
import org.elasticsearch.search.aggregations.metrics.InternalHDRPercentileRanks;
import org.elasticsearch.search.aggregations.metrics.InternalHDRPercentiles;
import org.elasticsearch.search.aggregations.metrics.InternalMedianAbsoluteDeviation;
import org.elasticsearch.search.aggregations.metrics.InternalScriptedMetric;
import org.elasticsearch.search.aggregations.metrics.InternalStats;
import org.elasticsearch.search.aggregations.metrics.InternalTDigestPercentileRanks;
import org.elasticsearch.search.aggregations.metrics.InternalTDigestPercentiles;
import org.elasticsearch.search.aggregations.metrics.InternalTopHits;
import org.elasticsearch.search.aggregations.metrics.InternalValueCount;
import org.elasticsearch.search.aggregations.metrics.InternalWeightedAvg;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.MetricInspectionHelper;
import org.elasticsearch.search.aggregations.metrics.Min;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.search.aggregations.pipeline.InternalBucketMetricValue;
import org.elasticsearch.search.aggregations.pipeline.InternalPercentilesBucket;
import org.elasticsearch.search.aggregations.pipeline.InternalSimpleValue;

import java.util.stream.StreamSupport;

/**
 * Provides a set of static helpers to determine if a particular type of InternalAggregation "has a value"
 * or not.  This can be difficult to determine from an external perspective because each agg uses
 * different internal bookkeeping to determine if it is empty or not (NaN, +/-Inf, 0.0, etc).
 *
 * This set of helpers aim to ease that task by codifying what "empty" is for each agg.
 *
 * It is not entirely accurate for all aggs, since some do not expose or track the needed state
 * (e.g. sum doesn't record count, so it's not clear if the sum is 0.0 because it is empty
 * or because of summing to zero).  Pipeline aggs in particular are not well supported
 * by these helpers since most share InternalSimpleValue and it's not clear which pipeline
 * generated the value.
 */
public class AggregationInspectionHelper {
    public static <A extends InternalTerms<A, B>, B extends InternalTerms.Bucket<B>> boolean hasValue(InternalTerms<A, B> agg) {
        return agg.getBuckets().stream().anyMatch(bucket -> bucket.getDocCount() > 0);
    }

    public static boolean hasValue(UnmappedTerms agg) {
        return false;
    }

    public static boolean hasValue(UnmappedSignificantTerms agg) {
        return false;
    }

    public static boolean hasValue(UnmappedSampler agg) {
        return false;
    }

    public static boolean hasValue(MultiBucketsAggregation agg) {
        return agg.getBuckets().stream().anyMatch(bucket -> bucket.getDocCount() > 0);
    }

    public static boolean hasValue(InternalFilters agg) {
        return agg.getBuckets().stream().anyMatch(bucket -> bucket.getDocCount() > 0);
    }

    public static boolean hasValue(InternalFilter agg) {
        return agg.getDocCount() > 0;
    }

    public static boolean hasValue(InternalGeoGrid<?> agg) {
        return agg.getBuckets().stream().anyMatch(bucket -> bucket.getDocCount() > 0);
    }

    public static boolean hasValue(InternalGlobal agg) {
        return agg.getDocCount() > 0;
    }

    public static boolean hasValue(InternalHistogram agg) {
        return agg.getBuckets().stream().anyMatch(bucket -> bucket.getDocCount() > 0);
    }

    public static boolean hasValue(InternalDateHistogram agg) {
        return agg.getBuckets().stream().anyMatch(bucket -> bucket.getDocCount() > 0);
    }

    public static boolean hasValue(InternalVariableWidthHistogram agg) {
        return agg.getBuckets().stream().anyMatch(bucket -> bucket.getDocCount() > 0);
    }

    public static boolean hasValue(InternalComposite agg) {
        return agg.getBuckets().stream().anyMatch(bucket -> bucket.getDocCount() > 0);
    }

    public static boolean hasValue(InternalMissing agg) {
        return agg.getDocCount() > 0;
    }

    public static boolean hasValue(InternalNested agg) {
        return agg.getDocCount() > 0;
    }

    public static boolean hasValue(InternalReverseNested agg) {
        return agg.getDocCount() > 0;
    }

    public static boolean hasValue(InternalRange<?, ?> agg) {
        return agg.getBuckets().stream().anyMatch(bucket -> bucket.getDocCount() > 0);
    }

    public static boolean hasValue(InternalSampler agg) {
        return agg.getDocCount() > 0;
    }

    public static boolean hasValue(InternalSignificantTerms<?, ?> agg) {
        return agg.getBuckets().stream().anyMatch(bucket -> bucket.getDocCount() > 0);
    }

    public static boolean hasValue(InternalAvg agg) {
        return MetricInspectionHelper.hasValue(agg);
    }

    public static boolean hasValue(Sum agg) {
        // TODO this could be incorrect... e.g. +1 + -1
        return agg.value() != 0.0;
    }

    public static boolean hasValue(InternalCardinality agg) {
        return MetricInspectionHelper.hasValue(agg);
    }

    public static boolean hasValue(InternalExtendedStats agg) {
        return agg.getCount() > 0;
    }

    public static boolean hasValue(InternalBounds<?> agg) {
        return (agg.topLeft() == null && agg.bottomRight() == null) == false;
    }

    public static boolean hasValue(InternalCentroid agg) {
        return agg.centroid() != null && agg.count() > 0;
    }

    public static boolean hasValue(InternalHDRPercentileRanks agg) {
        return MetricInspectionHelper.hasValue(agg);
    }

    public static boolean hasValue(InternalHDRPercentiles agg) {
        return MetricInspectionHelper.hasValue(agg);
    }

    public static boolean hasValue(Max agg) {
        return agg.value() != Double.NEGATIVE_INFINITY;
    }

    public static boolean hasValue(InternalMedianAbsoluteDeviation agg) {
        return MetricInspectionHelper.hasValue(agg);
    }

    public static boolean hasValue(Min agg) {
        return agg.value() != Double.POSITIVE_INFINITY;
    }

    public static boolean hasValue(InternalScriptedMetric agg) {
        return MetricInspectionHelper.hasValue(agg);
    }

    public static boolean hasValue(InternalStats agg) {
        return agg.getCount() > 0;
    }

    public static boolean hasValue(InternalTDigestPercentileRanks agg) {
        return MetricInspectionHelper.hasValue(agg);
    }

    public static boolean hasValue(InternalTDigestPercentiles agg) {
        return MetricInspectionHelper.hasValue(agg);
    }

    public static boolean hasValue(InternalTopHits agg) {
        return MetricInspectionHelper.hasValue(agg);
    }

    public static boolean hasValue(InternalValueCount agg) {
        return agg.getValue() > 0;
    }

    public static boolean hasValue(InternalWeightedAvg agg) {
        return MetricInspectionHelper.hasValue(agg);
    }

    public static boolean hasValue(InternalSimpleValue agg) {
        // This is a coarse approximation, since some aggs use positive/negative infinity or NaN
        return (Double.isInfinite(agg.getValue()) || Double.isNaN(agg.getValue())) == false;
    }

    public static boolean hasValue(InternalBucketMetricValue agg) {
        return Double.isInfinite(agg.value()) == false;
    }

    public static boolean hasValue(InternalPercentilesBucket agg) {
        return StreamSupport.stream(agg.spliterator(), false).allMatch(p -> Double.isNaN(p.getValue())) == false;
    }

}
