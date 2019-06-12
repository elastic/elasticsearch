/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.transforms.pivot;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.metrics.GeoCentroid;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation.SingleValue;
import org.elasticsearch.search.aggregations.metrics.ScriptedMetric;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameIndexerTransformStats;
import org.elasticsearch.xpack.core.dataframe.transforms.pivot.GroupConfig;
import org.elasticsearch.xpack.dataframe.transforms.IDGenerator;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.dataframe.transforms.pivot.SchemaUtil.isNumericType;

public final class AggregationResultUtils {

    private static final Map<String, AggValueExtractor> TYPE_VALUE_EXTRACTOR_MAP;
    static {
        Map<String, AggValueExtractor> tempMap = new HashMap<>();
        tempMap.put(SingleValue.class.getName(), new SingleValueAggExtractor());
        tempMap.put(ScriptedMetric.class.getName(), new ScriptedMetricAggExtractor());
        tempMap.put(GeoCentroid.class.getName(), new GeoCentroidAggExtractor());
        TYPE_VALUE_EXTRACTOR_MAP = Collections.unmodifiableMap(tempMap);
    }

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
                                                                                 Collection<PipelineAggregationBuilder> pipelineAggs,
                                                                                 Map<String, String> fieldTypeMap,
                                                                                 DataFrameIndexerTransformStats stats) {
        return agg.getBuckets().stream().map(bucket -> {
            stats.incrementNumDocuments(bucket.getDocCount());
            Map<String, Object> document = new HashMap<>();
            // generator to create unique but deterministic document ids, so we
            // - do not create duplicates if we re-run after failure
            // - update documents
            IDGenerator idGen = new IDGenerator();

            groups.getGroups().keySet().forEach(destinationFieldName -> {
                Object value = bucket.getKey().get(destinationFieldName);
                idGen.add(destinationFieldName, value);
                updateDocument(document, destinationFieldName, value);
            });

            List<String> aggNames = aggregationBuilders.stream().map(AggregationBuilder::getName).collect(Collectors.toList());
            aggNames.addAll(pipelineAggs.stream().map(PipelineAggregationBuilder::getName).collect(Collectors.toList()));

            for (String aggName: aggNames) {
                final String fieldType = fieldTypeMap.get(aggName);

                // TODO: support other aggregation types
                Aggregation aggResult = bucket.getAggregations().get(aggName);

                AggValueExtractor extractor = getExtractor(aggResult);
                updateDocument(document, aggName, extractor.value(aggResult, fieldType));
            }

            document.put(DataFrameField.DOCUMENT_ID_FIELD, idGen.getID());

            return document;
        });
    }

    static AggValueExtractor getExtractor(Aggregation aggregation) {
        if (aggregation instanceof SingleValue) {
            return TYPE_VALUE_EXTRACTOR_MAP.get(SingleValue.class.getName());
        } else if (aggregation instanceof ScriptedMetric) {
            return TYPE_VALUE_EXTRACTOR_MAP.get(ScriptedMetric.class.getName());
        } else if (aggregation instanceof GeoCentroid) {
            return TYPE_VALUE_EXTRACTOR_MAP.get(GeoCentroid.class.getName());
        } else {
            // Execution should never reach this point!
            // Creating transforms with unsupported aggregations shall not be possible
            throw new AggregationExtractionException("unsupported aggregation [{}] with name [{}]",
                aggregation.getType(),
                aggregation.getName());
        }
    }


    @SuppressWarnings("unchecked")
    static void updateDocument(Map<String, Object> document, String fieldName, Object value) {
        String[] fieldTokens = fieldName.split("\\.");
        if (fieldTokens.length == 1) {
            document.put(fieldName, value);
            return;
        }
        Map<String, Object> internalMap = document;
        for (int i = 0; i < fieldTokens.length; i++) {
            String token = fieldTokens[i];
            if (i == fieldTokens.length - 1) {
                if (internalMap.containsKey(token)) {
                    if (internalMap.get(token) instanceof Map) {
                        throw new AggregationExtractionException("mixed object types of nested and non-nested fields [{}]",
                            fieldName);
                    } else {
                        throw new AggregationExtractionException("duplicate key value pairs key [{}] old value [{}] duplicate value [{}]",
                            fieldName,
                            internalMap.get(token),
                            value);
                    }
                }
                internalMap.put(token, value);
            } else {
                if (internalMap.containsKey(token)) {
                    if (internalMap.get(token) instanceof Map) {
                        internalMap = (Map<String, Object>)internalMap.get(token);
                    } else {
                        throw new AggregationExtractionException("mixed object types of nested and non-nested fields [{}]",
                            fieldName);
                    }
                } else {
                    Map<String, Object> newMap = new HashMap<>();
                    internalMap.put(token, newMap);
                    internalMap = newMap;
                }
            }
        }
    }

    public static class AggregationExtractionException extends ElasticsearchException {
        AggregationExtractionException(String msg, Object... args) {
            super(msg, args);
        }
    }

    private interface AggValueExtractor {
        Object value(Aggregation aggregation, String fieldType);
    }

    private static class SingleValueAggExtractor implements AggValueExtractor {
        @Override
        public Object value(Aggregation agg, String fieldType) {
            SingleValue aggregation = (SingleValue)agg;
            // If the double is invalid, this indicates sparse data
            if (Numbers.isValidDouble(aggregation.value()) == false) {
                return null;
            }
            // If the type is numeric or if the formatted string is the same as simply making the value a string,
            //    gather the `value` type, otherwise utilize `getValueAsString` so we don't lose formatted outputs.
            if (isNumericType(fieldType) ||
                aggregation.getValueAsString().equals(String.valueOf(aggregation.value()))){
                return aggregation.value();
            } else {
                return aggregation.getValueAsString();
            }
        }
    }

    private static class ScriptedMetricAggExtractor implements AggValueExtractor {
        @Override
        public Object value(Aggregation agg, String fieldType) {
            ScriptedMetric aggregation = (ScriptedMetric)agg;
            return aggregation.aggregation();
        }
    }

    private static class GeoCentroidAggExtractor implements AggValueExtractor {
        @Override
        public Object value(Aggregation agg, String fieldType) {
            GeoCentroid aggregation = (GeoCentroid)agg;
            // if the account is `0` iff there is no contained centroid
            return aggregation.count() > 0 ? aggregation.centroid().toString() : null;
        }
    }
}
