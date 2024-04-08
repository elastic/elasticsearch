/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.metrics;

/**
 * Counterpart to {@link org.elasticsearch.search.aggregations.support.AggregationInspectionHelper}, providing
 * helpers for some aggs that have package-private getters.  AggregationInspectionHelper delegates to these
 * helpers when needed, and consumers should prefer to use AggregationInspectionHelper instead of these
 * helpers.
 */
public class MetricInspectionHelper {

    public static boolean hasValue(InternalAvg agg) {
        return agg.getCount() > 0;
    }

    public static boolean hasValue(InternalCardinality agg) {
        return agg.getCounts() != null;
    }

    public static boolean hasValue(InternalHDRPercentileRanks agg) {
        return agg.getState().getTotalCount() > 0;
    }

    public static boolean hasValue(InternalHDRPercentiles agg) {
        return agg.getState().getTotalCount() > 0;
    }

    public static boolean hasValue(InternalMedianAbsoluteDeviation agg) {
        return agg.getValuesSketch().size() > 0;
    }

    public static boolean hasValue(InternalScriptedMetric agg) {
        // TODO better way to know if the scripted metric received documents?
        // Could check for null too, but a script might return null on purpose...
        return agg.aggregationsList().size() > 0;
    }

    public static boolean hasValue(InternalTDigestPercentileRanks agg) {
        return agg.getState().size() > 0;
    }

    public static boolean hasValue(InternalTDigestPercentiles agg) {
        return agg.getState().size() > 0;
    }

    public static boolean hasValue(InternalTopHits agg) {
        return (agg.getHits().getTotalHits().value == 0
            && Double.isNaN(agg.getHits().getMaxScore())
            && Double.isNaN(agg.getTopDocs().maxScore)) == false;
    }

    public static boolean hasValue(InternalWeightedAvg agg) {
        return (agg.getSum() == 0.0 && agg.getWeight() == 0L) == false;
    }
}
