/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.transforms.pivot;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesAction;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.transform.transforms.pivot.PivotConfig;

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

    private SchemaUtil() {}

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
    public static void deduceMappings(
        final Client client,
        final PivotConfig config,
        final String[] source,
        final ActionListener<Map<String, String>> listener
    ) {
        // collects the fieldnames used as source for aggregations
        Map<String, String> aggregationSourceFieldNames = new HashMap<>();
        // collects the aggregation types by output field name
        Map<String, String> aggregationTypes = new HashMap<>();
        // collects the fieldnames and target fieldnames used for grouping
        Map<String, String> fieldNamesForGrouping = new HashMap<>();
        // collects the target mapping types used for grouping
        Map<String, String> fieldTypesForGrouping = new HashMap<>();

        config.getGroupConfig()
            .getGroups()
            .forEach((destinationFieldName, group) -> {
                // We will always need the field name for the grouping to create the mapping
                fieldNamesForGrouping.put(destinationFieldName, group.getField());
                // Sometimes the group config will supply a desired mapping as well
                if (group.getMappingType() != null) {
                    fieldTypesForGrouping.put(destinationFieldName, group.getMappingType());
                }
            });


        for (AggregationBuilder agg : config.getAggregationConfig().getAggregatorFactories()) {
            Tuple<Map<String, String>, Map<String, String>> inputAndOutputTypes = Aggregations.getAggregationInputAndOutputTypes(agg);
            aggregationSourceFieldNames.putAll(inputAndOutputTypes.v1());
            aggregationTypes.putAll(inputAndOutputTypes.v2());
        }

        // For pipeline aggs, since they are referencing other aggregations in the payload, they do not have any
        // sourcefieldnames to put into the payload. Though, certain ones, i.e. avg_bucket, do have determinant value types
        for (PipelineAggregationBuilder agg : config.getAggregationConfig().getPipelineAggregatorFactories()) {
            aggregationTypes.put(agg.getName(), agg.getType());
        }

        Map<String, String> allFieldNames = new HashMap<>();
        allFieldNames.putAll(aggregationSourceFieldNames);
        allFieldNames.putAll(fieldNamesForGrouping);

        getSourceFieldMappings(
            client,
            source,
            allFieldNames.values().toArray(new String[0]),
            ActionListener.wrap(
                sourceMappings -> listener.onResponse(
                    resolveMappings(
                        aggregationSourceFieldNames,
                        aggregationTypes,
                        fieldNamesForGrouping,
                        fieldTypesForGrouping,
                        sourceMappings
                    )
                ),
                listener::onFailure
            )
        );
    }

    /**
     * Gathers the field mappings for the "destination" index. Listener will receive an error, or a {@code Map<String, String>} of
     * "field-name":"type".
     *
     * @param client Client used to execute the request
     * @param index The index, or index pattern, from which to gather all the field mappings
     * @param listener The listener to be alerted on success or failure.
     */
    public static void getDestinationFieldMappings(
        final Client client,
        final String index,
        final ActionListener<Map<String, String>> listener
    ) {
        FieldCapabilitiesRequest fieldCapabilitiesRequest = new FieldCapabilitiesRequest().indices(index)
            .fields("*")
            .indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);
        ClientHelper.executeAsyncWithOrigin(
            client,
            ClientHelper.TRANSFORM_ORIGIN,
            FieldCapabilitiesAction.INSTANCE,
            fieldCapabilitiesRequest,
            ActionListener.wrap(r -> listener.onResponse(extractFieldMappings(r)), listener::onFailure)
        );
    }

    private static Map<String, String> resolveMappings(
        Map<String, String> aggregationSourceFieldNames,
        Map<String, String> aggregationTypes,
        Map<String, String> fieldNamesForGrouping,
        Map<String, String> fieldTypesForGrouping,
        Map<String, String> sourceMappings
    ) {
        Map<String, String> targetMapping = new HashMap<>();

        aggregationTypes.forEach((targetFieldName, aggregationName) -> {
            String sourceFieldName = aggregationSourceFieldNames.get(targetFieldName);
            String sourceMapping = sourceFieldName == null ? null : sourceMappings.get(sourceFieldName);
            String destinationMapping = Aggregations.resolveTargetMapping(aggregationName, sourceMapping);

            logger.debug(() -> new ParameterizedMessage(
                "Deduced mapping for: [{}], agg type [{}] to [{}]",
                targetFieldName,
                aggregationName,
                destinationMapping
            ));

            if (Aggregations.isDynamicMapping(destinationMapping)) {
                logger.debug(() -> new ParameterizedMessage(
                    "Dynamic target mapping set for field [{}] and aggregation [{}]",
                    targetFieldName,
                    aggregationName
                ));
            } else if (destinationMapping != null) {
                targetMapping.put(targetFieldName, destinationMapping);
            } else {
                logger.warn("Failed to deduce mapping for [{}], fall back to dynamic mapping.", targetFieldName);
            }
        });

        fieldNamesForGrouping.forEach((targetFieldName, sourceFieldName) -> {
            String destinationMapping = fieldTypesForGrouping.computeIfAbsent(targetFieldName, (s) -> sourceMappings.get(sourceFieldName));
            logger.debug(() -> new ParameterizedMessage("Deduced mapping for: [{}] to [{}]", targetFieldName, destinationMapping));
            if (destinationMapping != null) {
                targetMapping.put(targetFieldName, destinationMapping);
            } else {
                logger.warn("Failed to deduce mapping for [{}], fall back to keyword.", targetFieldName);
                targetMapping.put(targetFieldName, KeywordFieldMapper.CONTENT_TYPE);
            }
        });

        // insert object mappings for nested fields
        insertNestedObjectMappings(targetMapping);

        return targetMapping;
    }

    /*
     * Very "magic" helper method to extract the source mappings
     */
    private static void getSourceFieldMappings(
        Client client,
        String[] index,
        String[] fields,
        ActionListener<Map<String, String>> listener
    ) {
        FieldCapabilitiesRequest fieldCapabilitiesRequest = new FieldCapabilitiesRequest().indices(index)
            .fields(fields)
            .indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);
        client.execute(
            FieldCapabilitiesAction.INSTANCE,
            fieldCapabilitiesRequest,
            ActionListener.wrap(response -> listener.onResponse(extractFieldMappings(response)), listener::onFailure)
        );
    }

    private static Map<String, String> extractFieldMappings(FieldCapabilitiesResponse response) {
        Map<String, String> extractedTypes = new HashMap<>();

        response.get()
            .forEach(
                (fieldName, capabilitiesMap) -> {
                    // TODO: overwrites types, requires resolve if
                    // types are mixed
                    capabilitiesMap.forEach((name, capability) -> {
                        logger.trace(() -> new ParameterizedMessage("Extracted type for [{}] : [{}]", fieldName, capability.getType()));
                        extractedTypes.put(fieldName, capability.getType());
                    });
                }
            );
        return extractedTypes;
    }

    /**
     * Insert object mappings for fields like:
     *
     * a.b.c : some_type
     *
     * in which case it creates additional mappings:
     *
     * a.b : object
     * a : object
     *
     * avoids snafu with index templates injecting incompatible mappings
     *
     * @param fieldMappings field mappings to inject to
     */
    static void insertNestedObjectMappings(Map<String, String> fieldMappings) {
        Map<String, String> additionalMappings = new HashMap<>();
        fieldMappings.keySet().stream().filter(key -> key.contains(".")).forEach(key -> {
            int pos;
            String objectKey = key;
            // lastIndexOf returns -1 on mismatch, but to disallow empty strings check for > 0
            while ((pos = objectKey.lastIndexOf(".")) > 0) {
                objectKey = objectKey.substring(0, pos);
                additionalMappings.putIfAbsent(objectKey, "object");
            }
        });

        additionalMappings.forEach(fieldMappings::putIfAbsent);
    }
}
