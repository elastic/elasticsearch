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
import org.elasticsearch.client.Client;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.xpack.core.dataframe.transforms.pivot.PivotConfig;

import java.util.HashMap;
import java.util.Map;

public class SchemaUtil {
    private static final Logger logger = LogManager.getLogger(SchemaUtil.class);

    private SchemaUtil() {
    }

    public static void deduceMappings(final Client client, final PivotConfig config, final String source,
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
            } else {
                // execution should not reach this point
                listener.onFailure(new RuntimeException("Unsupported aggregation type [" + agg.getType() + "]"));
                return;
            }
        }

        Map<String, String> allFieldNames = new HashMap<>();
        allFieldNames.putAll(aggregationSourceFieldNames);
        allFieldNames.putAll(fieldNamesForGrouping);

        getSourceFieldMappings(client, source, allFieldNames.values().toArray(new String[0]),
                ActionListener.wrap(sourceMappings -> {
                    Map<String, String> targetMapping = resolveMappings(aggregationSourceFieldNames, aggregationTypes,
                            fieldNamesForGrouping, sourceMappings);

                    listener.onResponse(targetMapping);
                }, e -> {
                    listener.onFailure(e);
                }));
    }

    private static Map<String, String> resolveMappings(Map<String, String> aggregationSourceFieldNames,
            Map<String, String> aggregationTypes, Map<String, String> fieldNamesForGrouping, Map<String, String> sourceMappings) {
        Map<String, String> targetMapping = new HashMap<>();

        aggregationTypes.forEach((targetFieldName, aggregationName) -> {
            String sourceFieldName = aggregationSourceFieldNames.get(targetFieldName);
            String destinationMapping = Aggregations.resolveTargetMapping(aggregationName, sourceMappings.get(sourceFieldName));

            logger.debug(
                    "Deduced mapping for: [" + targetFieldName + "], agg type [" + aggregationName + "] to [" + destinationMapping + "]");
            if (destinationMapping != null) {
                targetMapping.put(targetFieldName, destinationMapping);
            } else {
                logger.warn("Failed to deduce mapping for [" + targetFieldName + "], fall back to double.");
                targetMapping.put(targetFieldName, "double");
            }
        });

        fieldNamesForGrouping.forEach((targetFieldName, sourceFieldName) -> {
            String destinationMapping = sourceMappings.get(sourceFieldName);
            logger.debug(
                    "Deduced mapping for: [" + targetFieldName + "] to [" + destinationMapping + "]");
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
    private static void getSourceFieldMappings(Client client, String index, String[] fields,
            ActionListener<Map<String, String>> listener) {
        GetFieldMappingsRequest fieldMappingRequest = new GetFieldMappingsRequest();
        fieldMappingRequest.indices(index);
        fieldMappingRequest.fields(fields);

        client.execute(GetFieldMappingsAction.INSTANCE, fieldMappingRequest, ActionListener.wrap(response -> {
            listener.onResponse(extractSourceFieldMappings(response.mappings()));
        }, e -> {
            listener.onFailure(e);
        }));
    }

    private static Map<String, String> extractSourceFieldMappings(Map<String, Map<String, Map<String, FieldMappingMetaData>>> mappings) {
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
                                logger.debug("Extracted type for [" + fieldName + "] : [" + type + "]");
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
