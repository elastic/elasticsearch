/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.transforms.pivot;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse.FieldMappingMetaData;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ScriptedMetricAggregationBuilder;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.dataframe.transforms.pivot.PivotConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class SchemaUtil {
    private static final Logger logger = LogManager.getLogger(SchemaUtil.class);

    // Full collection of numeric field type strings
    private static final Set<String> NUMERIC_FIELD_MAPPER_TYPES;
    static {
        Set<String> types = Stream.of(NumberFieldMapper.NumberType.values())
            .map(NumberFieldMapper.NumberType::typeName)
            .collect(Collectors.toSet());
        types.add("scaled_float"); // have to add manually since scaled_float is in a module
        NUMERIC_FIELD_MAPPER_TYPES = types;
    }

    private SchemaUtil() {
    }

    public static boolean isNumericType(String type) {
        return type != null && NUMERIC_FIELD_MAPPER_TYPES.contains(type);
    }

    /**
     * Deduce the mappings for the destination index given the source index
     *
     * The Listener is alerted with a {@code Map<String, String>} that is a "field-name":"type" mapping
     *
     * @param client Client from which to make requests against the cluster
     * @param config The PivotConfig for which to deduce destination mapping
     * @param source Source index that contains the data to pivot
     * @param listener Listener to alert on success or failure.
     */
    public static void deduceMappings(final Client client,
                                      final PivotConfig config,
                                      final String[] source,
                                      final ActionListener<Map<String, String>> listener) {
        // collects the fieldnames used as source for aggregations
        Map<String, String> aggregationSourceFieldNames = new HashMap<>();
        // collects the aggregation types by source name
        Map<String, String> aggregationTypes = new HashMap<>();
        // collects the fieldnames and target fieldnames used for grouping
        Map<String, String> fieldNamesForGrouping = new HashMap<>();

        config.getGroupConfig().getGroups().forEach((destinationFieldName, group) -> {
            fieldNamesForGrouping.put(destinationFieldName, group.getField());
        });

        for (AggregationBuilder agg : config.getAggregationConfig().getAggregatorFactories()) {
            if (agg instanceof ValuesSourceAggregationBuilder) {
                ValuesSourceAggregationBuilder<?, ?> valueSourceAggregation = (ValuesSourceAggregationBuilder<?, ?>) agg;
                aggregationSourceFieldNames.put(valueSourceAggregation.getName(), valueSourceAggregation.field());
                aggregationTypes.put(valueSourceAggregation.getName(), valueSourceAggregation.getType());
            } else if(agg instanceof ScriptedMetricAggregationBuilder || agg instanceof MultiValuesSourceAggregationBuilder) {
                aggregationTypes.put(agg.getName(), agg.getType());
            } else {
                // execution should not reach this point
                listener.onFailure(new RuntimeException("Unsupported aggregation type [" + agg.getType() + "]"));
                return;
            }
        }

        // For pipeline aggs, since they are referencing other aggregations in the payload, they do not have any
        // sourcefieldnames to put into the payload. Though, certain ones, i.e. avg_bucket, do have determinant value types
        for (PipelineAggregationBuilder agg : config.getAggregationConfig().getPipelineAggregatorFactories()) {
            aggregationTypes.put(agg.getName(), agg.getType());
        }

        Map<String, String> allFieldNames = new HashMap<>();
        allFieldNames.putAll(aggregationSourceFieldNames);
        allFieldNames.putAll(fieldNamesForGrouping);

        getSourceFieldMappings(client, source, allFieldNames.values().toArray(new String[0]),
                ActionListener.wrap(
                    sourceMappings -> listener.onResponse(resolveMappings(aggregationSourceFieldNames,
                        aggregationTypes,
                        fieldNamesForGrouping,
                        sourceMappings)),
                    listener::onFailure));
    }

    /**
     * Gathers the field mappings for the "destination" index. Listener will receive an error, or a {@code Map<String, String>} of
     * "field-name":"type".
     *
     * @param client Client used to execute the request
     * @param index The index, or index pattern, from which to gather all the field mappings
     * @param listener The listener to be alerted on success or failure.
     */
    public static void getDestinationFieldMappings(final Client client,
                                                   final String index,
                                                   final ActionListener<Map<String, String>> listener) {
        GetFieldMappingsRequest fieldMappingRequest = new GetFieldMappingsRequest();
        fieldMappingRequest.indices(index);
        fieldMappingRequest.fields("*");
        ClientHelper.executeAsyncWithOrigin(client,
            ClientHelper.DATA_FRAME_ORIGIN,
            GetFieldMappingsAction.INSTANCE,
            fieldMappingRequest,
            ActionListener.wrap(
                r -> listener.onResponse(extractFieldMappings(r.mappings())),
                listener::onFailure
            ));
    }

    private static Map<String, String> resolveMappings(Map<String, String> aggregationSourceFieldNames,
                                                       Map<String, String> aggregationTypes,
                                                       Map<String, String> fieldNamesForGrouping,
                                                       Map<String, String> sourceMappings) {
        Map<String, String> targetMapping = new HashMap<>();

        aggregationTypes.forEach((targetFieldName, aggregationName) -> {
            String sourceFieldName = aggregationSourceFieldNames.get(targetFieldName);
            String sourceMapping = sourceFieldName == null ? null : sourceMappings.get(sourceFieldName);
            String destinationMapping = Aggregations.resolveTargetMapping(aggregationName, sourceMapping);

            logger.debug("Deduced mapping for: [{}], agg type [{}] to [{}]",
                    targetFieldName, aggregationName, destinationMapping);

            if (Aggregations.isDynamicMapping(destinationMapping)) {
                logger.debug("Dynamic target mapping set for field [{}] and aggregation [{}]", targetFieldName, aggregationName);
            } else if (destinationMapping != null) {
                targetMapping.put(targetFieldName, destinationMapping);
            } else {
                logger.warn("Failed to deduce mapping for [" + targetFieldName + "], fall back to dynamic mapping.");
            }
        });

        fieldNamesForGrouping.forEach((targetFieldName, sourceFieldName) -> {
            String destinationMapping = sourceMappings.get(sourceFieldName);
            logger.debug("Deduced mapping for: [{}] to [{}]", targetFieldName, destinationMapping);
            if (destinationMapping != null) {
                targetMapping.put(targetFieldName, destinationMapping);
            } else {
                logger.warn("Failed to deduce mapping for [" + targetFieldName + "], fall back to keyword.");
                targetMapping.put(targetFieldName, "keyword");
            }
        });
        return targetMapping;
    }

    /*
     * Very "magic" helper method to extract the source mappings
     */
    private static void getSourceFieldMappings(Client client, String[] index, String[] fields,
            ActionListener<Map<String, String>> listener) {
        GetFieldMappingsRequest fieldMappingRequest = new GetFieldMappingsRequest();
        fieldMappingRequest.indices(index);
        fieldMappingRequest.fields(fields);
        fieldMappingRequest.indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);

        client.execute(GetFieldMappingsAction.INSTANCE, fieldMappingRequest, ActionListener.wrap(
            response -> listener.onResponse(extractFieldMappings(response.mappings())),
            listener::onFailure));
    }

    private static Map<String, String> extractFieldMappings(Map<String, Map<String, Map<String, FieldMappingMetaData>>> mappings) {
        Map<String, String> extractedTypes = new HashMap<>();

        mappings.forEach((indexName, docTypeToMapping) -> {
            // "_doc" ->
            docTypeToMapping.forEach((docType, fieldNameToMapping) -> {
                // "my_field" ->
                fieldNameToMapping.forEach((fieldName, fieldMapping) -> {
                    // "mapping" -> "my_field" ->
                    fieldMapping.sourceAsMap().forEach((name, typeMap) -> {
                        // expected object: { "type": type }
                        if (typeMap instanceof Map) {
                            final Map<?, ?> map = (Map<?, ?>) typeMap;
                            if (map.containsKey("type")) {
                                String type = map.get("type").toString();
                                if (logger.isTraceEnabled()) {
                                    logger.trace("Extracted type for [" + fieldName + "] : [" + type + "] from index [" + indexName + "]");
                                }
                                // TODO: overwrites types, requires resolve if
                                // types are mixed
                                extractedTypes.put(fieldName, type);
                            }
                        }
                    });
                });
            });
        });
        return extractedTypes;
    }
}
