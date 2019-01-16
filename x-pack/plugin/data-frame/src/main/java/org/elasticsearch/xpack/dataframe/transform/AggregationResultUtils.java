/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.transform;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation.SingleValue;
import org.elasticsearch.xpack.core.dataframe.transform.DataFrameIndexerTransformStats;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

final class AggregationResultUtils {
    private static final Logger logger = LogManager.getLogger(AggregationResultUtils.class);

    /**
     * Extracts aggregation results from a composite aggregation and puts it into a map.
     *
     * @param agg The aggregation result
     * @param sources The original sources used for querying
     * @param aggregationBuilders the aggregation used for querying
     * @param dataFrameIndexerTransformStats stats collector
     * @return a map containing the results of the aggregation in a consumable way
     */
    public static Stream<Map<String, Object>> extractCompositeAggregationResults(CompositeAggregation agg,
            List<CompositeValuesSourceBuilder<?>> sources, Collection<AggregationBuilder> aggregationBuilders,
            DataFrameIndexerTransformStats dataFrameIndexerTransformStats) {
        return agg.getBuckets().stream().map(bucket -> {
            dataFrameIndexerTransformStats.incrementNumDocuments(bucket.getDocCount());

            Map<String, Object> document = new HashMap<>();
            for (CompositeValuesSourceBuilder<?> source : sources) {
                String destinationFieldName = source.name();
                document.put(destinationFieldName, bucket.getKey().get(destinationFieldName));
            }
            for (AggregationBuilder aggregationBuilder : aggregationBuilders) {
                String aggName = aggregationBuilder.getName();

                // TODO: support other aggregation types
                Aggregation aggResult = bucket.getAggregations().get(aggName);

                if (aggResult instanceof NumericMetricsAggregation.SingleValue) {
                    NumericMetricsAggregation.SingleValue aggResultSingleValue = (SingleValue) aggResult;
                    document.put(aggName, aggResultSingleValue.value());
                } else {
                    // Execution should never reach this point!
                    // Creating transforms with unsupported aggregations shall not be possible
                    logger.error("Dataframe Internal Error: unsupported aggregation ["+ aggResult.getName() +"], ignoring");
                    assert false;
                }
            }
            return document;
        });
    }

}
