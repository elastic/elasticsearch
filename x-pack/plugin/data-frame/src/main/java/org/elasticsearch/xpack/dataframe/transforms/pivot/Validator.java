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
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.dataframe.transforms.DataFrameTransformConfig;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class Validator {
    private static final String COMPOSITE_AGGREGATION_NAME = "_data_frame";

    private static final Logger logger = LogManager.getLogger(Validator.class);

    private final Client client;
    private final DataFrameTransformConfig config;

    public Validator(DataFrameTransformConfig config, Client client) {
        this.client = Objects.requireNonNull(client);
        this.config = Objects.requireNonNull(config);
    }

    public void validate(final ActionListener<Boolean> listener) {
        // step 1: check if used aggregations are supported
        for (AggregationBuilder agg : config.getPivotConfig().getAggregationConfig().getAggregatorFactories()) {
            if (Aggregations.isSupportedByDataframe(agg.getType()) == false) {
                listener.onFailure(new RuntimeException("Unsupported aggregation type [" + agg.getType() + "]"));
                return;
            }
        }

        // step 2: run a query to validate that config is valid
        runTestQuery(listener);
    }

    public void deduceMappings(final ActionListener<Map<String, String>> listener) {
        // collects the fieldnames used as source for aggregations
        Map<String, String> aggregationSourceFieldNames = new HashMap<>();
        // collects the aggregation types by source name
        Map<String, String> aggregationTypes = new HashMap<>();
        // collects the fieldnames and target fieldnames used for grouping
        Map<String, String> fieldNamesForGrouping = new HashMap<>();

        final PivotConfig pivotConfig = config.getPivotConfig();

        pivotConfig.getGroups().forEach(group -> {
            fieldNamesForGrouping.put(group.getDestinationFieldName(), group.getGroupSource().getField());
        });

        for (AggregationBuilder agg : pivotConfig.getAggregationConfig().getAggregatorFactories()) {
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

        getSourceFieldMappings(config.getSource(), allFieldNames.values().toArray(new String[0]),
                ActionListener.wrap(sourceMappings -> {
                    Map<String, String> targetMapping = resolveMappings(aggregationSourceFieldNames, aggregationTypes,
                            fieldNamesForGrouping, sourceMappings);

                    listener.onResponse(targetMapping);
                }, e -> {
                    listener.onFailure(e);
                }));
    }

    Map<String, String> resolveMappings(Map<String, String> aggregationSourceFieldNames, Map<String, String> aggregationTypes,
            Map<String, String> fieldNamesForGrouping, Map<String, String> sourceMappings) {
        Map<String, String> targetMapping = new HashMap<>();

        aggregationTypes.forEach((targetFieldName, aggregationName) -> {
            String sourceFieldName = aggregationSourceFieldNames.get(targetFieldName);
            String destinationMapping = Aggregations.resolveTargetMapping(aggregationName, sourceMappings.get(sourceFieldName));

            logger.debug("[" + config.getId() + "] Deduced mapping for: [" + targetFieldName + "], agg type [" + aggregationName
                    + "] to [" + destinationMapping + "]");
            if (destinationMapping != null) {
                targetMapping.put(targetFieldName, destinationMapping);
            } else {
                logger.warn("[" + config.getId() + "] Failed to deduce mapping for [" + targetFieldName
                        + "], fall back to double.");
                targetMapping.put(targetFieldName, "double");
            }
        });

        fieldNamesForGrouping.forEach((targetFieldName, sourceFieldName) -> {
            String destinationMapping = sourceMappings.get(sourceFieldName);
            logger.debug(
                    "[" + config.getId() + "] Deduced mapping for: [" + targetFieldName + "] to [" + destinationMapping + "]");
            if (destinationMapping != null) {
                targetMapping.put(targetFieldName, destinationMapping);
            } else {
                logger.warn("[" + config.getId() + "] Failed to deduce mapping for [" + targetFieldName
                        + "], fall back to keyword.");
                targetMapping.put(targetFieldName, "keyword");
            }
        });
        return targetMapping;
    }

    private void runTestQuery(final ActionListener<Boolean> listener) {
        QueryBuilder queryBuilder = new MatchAllQueryBuilder();
        SearchRequest searchRequest = new SearchRequest(config.getSource());
        CompositeAggregationBuilder compositeAggregation;

        try (XContentBuilder builder = jsonBuilder()) {
            // write configuration for composite aggs into builder
            config.getPivotConfig().toCompositeAggXContent(builder, ToXContentObject.EMPTY_PARAMS);
            XContentParser parser = builder.generator().contentType().xContent()
            .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, BytesReference.bytes(builder).streamInput());

            compositeAggregation = CompositeAggregationBuilder.parse(COMPOSITE_AGGREGATION_NAME, parser);
        } catch (IOException e1) {
            throw new RuntimeException(e1);
        }
        compositeAggregation.size(1);

        for (AggregationBuilder agg : config.getPivotConfig().getAggregationConfig().getAggregatorFactories()) {
            compositeAggregation.subAggregation(agg);
        }

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.aggregation(compositeAggregation);
        sourceBuilder.size(0);
        sourceBuilder.query(queryBuilder);
        searchRequest.source(sourceBuilder);

        client.execute(SearchAction.INSTANCE, searchRequest, ActionListener.wrap(response -> {
            if (response == null) {
                listener.onFailure(new RuntimeException("Unexpected null response from test query"));
                return;
            }
            if (response.status() != RestStatus.OK) {
                listener.onFailure(new RuntimeException("Unexpected status from response of test query: " + response.status()));
                return;
            }
            listener.onResponse(true);
        }, e->{
            listener.onFailure(new RuntimeException("Failed to test query",e));
        }));
    }

    /*
     * Very "magic" helper method to extract the source mappings
     */
    private void getSourceFieldMappings(String index, String[] fields,
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

    Map<String, String> extractSourceFieldMappings(Map<String, Map<String, Map<String, FieldMappingMetaData>>> mappings) {
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
                                logger.debug("[" + config.getId() + "] Extracted type for [" + fieldName + "] : [" + type + "]");
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
