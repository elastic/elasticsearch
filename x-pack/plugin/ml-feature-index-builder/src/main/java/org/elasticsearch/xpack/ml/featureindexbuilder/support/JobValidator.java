/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.featureindexbuilder.support;

import org.apache.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsRequest;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.ml.featureindexbuilder.job.FeatureIndexBuilderJobConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class JobValidator {
    private static final String COMPOSITE_AGGREGATION_NAME = "_dataframe";

    private static final Logger logger = Logger.getLogger(JobValidator.class.getName());

    private final Client client;

    public JobValidator(Client client) {
        this.client = Objects.requireNonNull(client);
    }

    public void validate(FeatureIndexBuilderJobConfig config, final ActionListener<Boolean> listener) {
        // step 1: check if used aggregations are supported
        for (AggregationBuilder agg : config.getAggregationConfig().getAggregatorFactories()) {
            if (Aggregations.isSupportedByDataframe(agg.getType()) == false) {
                listener.onFailure(new RuntimeException("Unsupported aggregation type [" + agg.getType() + "]"));
                return;
            }
        }

        // step 2: run a query to validate that config is valid
        runTestQuery(config, ActionListener.wrap((r) -> {
            listener.onResponse(true);
        }, e -> {
            listener.onFailure(e);
        }));
    }

    public void deduceMappings(FeatureIndexBuilderJobConfig config, final ActionListener<Map<String, String>> listener) {
        // collects the fieldnames used as source
        Map<String, String> aggregationSourceFieldNames = new HashMap<>();
        // collects the aggregation types by source name
        Map<String, String> aggregationTypes = new HashMap<>();

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

        getSourceFieldMappings(config.getIndexPattern(),
                aggregationSourceFieldNames.values().toArray(new String[aggregationSourceFieldNames.size()]),
                ActionListener.wrap(sourceMappings -> {
                    Map<String, String> targetMapping = new HashMap<>();

                    aggregationTypes.forEach((targetFieldName, aggregationName) -> {
                        String sourceFieldName = aggregationSourceFieldNames.get(targetFieldName);
                        String destinationMapping = Aggregations.resolveTargetMapping(aggregationName, sourceMappings.get(sourceFieldName));

                        logger.debug("Deduced mapping for: [" + targetFieldName + "], agg type [" + aggregationName + "] to ["
                                + destinationMapping + "]");
                        if (destinationMapping != null) {
                            targetMapping.put(targetFieldName, destinationMapping);
                        }
                        // TODO: else?
                    });

                    listener.onResponse(targetMapping);
                }, e2 -> {
                    listener.onFailure(e2);
                }));
    }

    private void runTestQuery(FeatureIndexBuilderJobConfig config, final ActionListener<Boolean> listener) {
        QueryBuilder queryBuilder = new MatchAllQueryBuilder();
        SearchRequest searchRequest = new SearchRequest(config.getIndexPattern());

        List<CompositeValuesSourceBuilder<?>> sources = config.getSourceConfig().getSources();

        CompositeAggregationBuilder compositeAggregation = new CompositeAggregationBuilder(COMPOSITE_AGGREGATION_NAME, sources);
        compositeAggregation.size(1);

        for (AggregationBuilder agg : config.getAggregationConfig().getAggregatorFactories()) {
            compositeAggregation.subAggregation(agg);
        }

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.aggregation(compositeAggregation);
        sourceBuilder.size(0);
        sourceBuilder.query(queryBuilder);
        searchRequest.source(sourceBuilder);

        client.execute(SearchAction.INSTANCE, searchRequest, ActionListener.wrap(response -> {
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

        Map<String, String> extractedTypes = new HashMap<>();

        client.execute(GetFieldMappingsAction.INSTANCE, fieldMappingRequest, ActionListener.wrap(response -> {
            response.mappings().forEach((indexName, docTypeToMapping) -> {
                // "_doc" ->
                docTypeToMapping.forEach((docType, fieldNameToMapping) -> {
                    // "my_field" ->
                    fieldNameToMapping.forEach((fieldName, fieldMapping) -> {
                        // "mapping" -> "my_field" -> 
                        fieldMapping.sourceAsMap().forEach((name, typeMap) -> {
                            // expected object: { "type": type }
                            if (typeMap instanceof Map) {
                                final Map<?,?> map = (Map<?, ?>) typeMap;
                                if (map.containsKey("type")) {
                                    // TODO: overwrites types, requires resolve if types are mixed
                                    extractedTypes.put(fieldName, map.get("type").toString());
                                }
                            }
                        });
                    });
                });
            });
            listener.onResponse(extractedTypes);
        }, e -> {
            listener.onFailure(e);
        }));
    }
}
