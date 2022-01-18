/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation;

import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.filter.Filters;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.Cardinality;
import org.elasticsearch.search.aggregations.metrics.ExtendedStats;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;

import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class MockAggregations {

    public static Terms mockTerms(String name) {
        return mockTerms(name, Collections.emptyList(), 0);
    }

    public static Terms mockTerms(String name, List<Terms.Bucket> buckets, long sumOfOtherDocCounts) {
        Terms agg = mock(Terms.class);
        when(agg.getName()).thenReturn(name);
        doReturn(buckets).when(agg).getBuckets();
        when(agg.getSumOfOtherDocCounts()).thenReturn(sumOfOtherDocCounts);
        return agg;
    }

    public static Terms.Bucket mockTermsBucket(String key, Aggregations subAggs) {
        Terms.Bucket bucket = mock(Terms.Bucket.class);
        when(bucket.getKeyAsString()).thenReturn(key);
        when(bucket.getAggregations()).thenReturn(subAggs);
        return bucket;
    }

    public static Filters mockFilters(String name) {
        return mockFilters(name, Collections.emptyList());
    }

    public static Filters mockFilters(String name, List<Filters.Bucket> buckets) {
        Filters agg = mock(Filters.class);
        when(agg.getName()).thenReturn(name);
        doReturn(buckets).when(agg).getBuckets();
        return agg;
    }

    public static Filters.Bucket mockFiltersBucket(String key, long docCount, Aggregations subAggs) {
        Filters.Bucket bucket = mockFiltersBucket(key, docCount);
        when(bucket.getAggregations()).thenReturn(subAggs);
        return bucket;
    }

    public static Filters.Bucket mockFiltersBucket(String key, long docCount) {
        Filters.Bucket bucket = mock(Filters.Bucket.class);
        when(bucket.getKeyAsString()).thenReturn(key);
        when(bucket.getDocCount()).thenReturn(docCount);
        return bucket;
    }

    public static Filter mockFilter(String name, long docCount) {
        Filter agg = mock(Filter.class);
        when(agg.getName()).thenReturn(name);
        when(agg.getDocCount()).thenReturn(docCount);
        return agg;
    }

    public static NumericMetricsAggregation.SingleValue mockSingleValue(String name, double value) {
        NumericMetricsAggregation.SingleValue agg = mock(NumericMetricsAggregation.SingleValue.class);
        when(agg.getName()).thenReturn(name);
        when(agg.value()).thenReturn(value);
        return agg;
    }

    public static Cardinality mockCardinality(String name, long value) {
        Cardinality agg = mock(Cardinality.class);
        when(agg.getName()).thenReturn(name);
        when(agg.getValue()).thenReturn(value);
        return agg;
    }

    public static ExtendedStats mockExtendedStats(String name, double variance, long count) {
        ExtendedStats agg = mock(ExtendedStats.class);
        when(agg.getName()).thenReturn(name);
        when(agg.getVariance()).thenReturn(variance);
        when(agg.getCount()).thenReturn(count);
        return agg;
    }
}
