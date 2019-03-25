/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.transforms.pivot;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation.SingleValue;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameIndexerTransformStats;
import org.elasticsearch.xpack.core.dataframe.transforms.pivot.GroupConfig;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.dataframe.transforms.pivot.SchemaUtil.isNumericType;

final class AggregationResultUtils {
    private static final Logger logger = LogManager.getLogger(AggregationResultUtils.class);

    /**
     * Extracts aggregation results from a composite aggregation and puts it into a map.
     *
     * @param agg The aggregation result
     * @param groups The original groupings used for querying
     * @param aggregationBuilders the aggregation used for querying
     * @param fieldTypeMap A Map containing "field-name": "type" entries to determine the appropriate type for the aggregation results.
     * @param stats stats collector
     * @return a map containing the results of the aggregation in a consumable way
     */
    public static Stream<Map<String, Object>> extractCompositeAggregationResults(CompositeAggregation agg,
                                                                                 GroupConfig groups,
                                                                                 Collection<AggregationBuilder> aggregationBuilders,
                                                                                 Map<String, String> fieldTypeMap,
                                                                                 DataFrameIndexerTransformStats stats) {
        return agg.getBuckets().stream().map(bucket -> {
            stats.incrementNumDocuments(bucket.getDocCount());

            Map<String, Object> document = new HashMap<>();
            groups.getGroups().keySet().forEach(destinationFieldName ->
                document.put(destinationFieldName, bucket.getKey().get(destinationFieldName)));

            for (AggregationBuilder aggregationBuilder : aggregationBuilders) {
                String aggName = aggregationBuilder.getName();
                final String fieldType = fieldTypeMap.get(aggName);

                // TODO: support other aggregation types
                Aggregation aggResult = bucket.getAggregations().get(aggName);

                if (aggResult instanceof NumericMetricsAggregation.SingleValue) {
                    NumericMetricsAggregation.SingleValue aggResultSingleValue = (SingleValue) aggResult;
                    // If the type is numeric, simply gather the `value` type, otherwise utilize `getValueAsString` so we don't lose
                    // formatted outputs.
                    if (isNumericType(fieldType)) {
                        document.put(aggName, aggResultSingleValue.value());
                    } else {
                        document.put(aggName, aggResultSingleValue.getValueAsString());
                    }
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
