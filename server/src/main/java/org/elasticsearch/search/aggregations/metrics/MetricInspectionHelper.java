/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.search.aggregations.pipeline.InternalDerivative;

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
        return agg.getAggregation().size() > 0 ;
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

    public static boolean hasValue(InternalDerivative agg) {
        return true;
    }
}
