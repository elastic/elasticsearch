/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entityanalytics.models;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.entityanalytics.common.EntityTypeUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RiskScoreQueryHelper {
    private static final int COMPOSITE_PAGE_SIZE_PER_ENTITY_TYPE = 1000;
    private static final int TOP_HITS_PER_ENTITY = 100;
    private static final String[] SOURCE_FIELDS_NEEDED_FOR_RISK_SCORING = new String[] {
        "kibana.alert.risk_score",
        "event.kind",
        "kibana.alert.rule.name",
        "kibana.alert.uuid",
        "@timestamp" };

    /**
     * Given a search response and some entity types, extract a map of entity types and their after_keys
     * @param entityTypes
     * @param searchResponse
     * @return
     */
    public static Map<EntityType, Map<String, Object>> getAfterKeysForEntityTypes(EntityType[] entityTypes, SearchResponse searchResponse) {
        Map<EntityType, Map<String, Object>> afterKeys = new HashMap<>();
        for (EntityType entityType : entityTypes) {
            String aggregationName = EntityTypeUtils.getAggregationNameForEntityType(entityType);
            CompositeAggregation compositeAggregation = searchResponse.getAggregations().get(aggregationName);
            Map<String, Object> newAfterKey = compositeAggregation.afterKey();
            if (newAfterKey != null) {
                afterKeys.put(entityType, newAfterKey);
            }
        }

        return afterKeys;
    }

    /**
     * Build the composite aggregation for a given entity type
     * if afterKeys provided, add those to the composite agg to proceed from the given point
     * @param entityType
     * @param afterKeys
     * @return
     */
    private static CompositeAggregationBuilder buildEntityAggregation(EntityType entityType, Map<String, Object> afterKeys) {
        String identifierField = EntityTypeUtils.getIdentifierFieldForEntityType(entityType);
        String aggregationName = EntityTypeUtils.getAggregationNameForEntityType(entityType);
        List<CompositeValuesSourceBuilder<?>> sources = new ArrayList<>();
        sources.add(new TermsValuesSourceBuilder(identifierField).field(identifierField));

        CompositeAggregationBuilder compositeAggregationBuilder = AggregationBuilders.composite(aggregationName, sources)
            .size(COMPOSITE_PAGE_SIZE_PER_ENTITY_TYPE)
            .subAggregation(
                AggregationBuilders.topHits("top_inputs")
                    .size(TOP_HITS_PER_ENTITY)
                    .sort("kibana.alert.risk_score", SortOrder.DESC)
                    .fetchSource(FetchSourceContext.of(true, SOURCE_FIELDS_NEEDED_FOR_RISK_SCORING, null))
            );

        if (afterKeys != null) {
            compositeAggregationBuilder.aggregateAfter(afterKeys);
        }

        return compositeAggregationBuilder;
    }

    /**
     * Build the risk scoring request for the given index
     * @param index
     * @param entityTypes
     * @return
     */
    public static SearchRequest buildRiskScoreSearchRequest(String index, EntityType[] entityTypes) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Using the query builder was adding some unexpected "boost" calls and an extra filter to the function score
        // I tried using raw json but it didn't improve perf
        // BoolQueryBuilder boolQuery = QueryBuilders.boolQuery()
        // .filter(QueryBuilders.rangeQuery("@timestamp").gte("now-30d").lt("now"))
        // .filter(QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("kibana.alert.workflow_status", "closed")))
        // .filter(QueryBuilders.existsQuery("kibana.alert.risk_score"))
        // .should(QueryBuilders.matchAllQuery());
        //
        // FunctionScoreQueryBuilder functionScoreQuery = QueryBuilders.functionScoreQuery(
        // boolQuery,
        // new FieldValueFactorFunctionBuilder("kibana.alert.risk_score")
        // );
        String json = """
            {
                "function_score": {
                    "query": {
                        "bool": {
                            "filter": [
                                {
                                    "range": {
                                        "@timestamp": {
                                            "lt": "now",
                                            "gte": "now-30d"
                                        }
                                    }
                                },
                                {
                                    "bool": {
                                        "must_not": {
                                            "term": {
                                                "kibana.alert.workflow_status": "closed"
                                            }
                                        }
                                    }
                                },
                                {
                                    "exists": {
                                        "field": "kibana.alert.risk_score"
                                    }
                                }
                            ],
                            "should": [
                                {
                                    "match_all": {}
                                }
                            ]
                        }
                    },
                    "field_value_factor": {
                        "field": "kibana.alert.risk_score"
                    }
                }
            }
            }
            """;

        searchSourceBuilder.query(QueryBuilders.wrapperQuery(json));
        searchSourceBuilder.size(0);

        for (EntityType entityType : entityTypes) {
            searchSourceBuilder.aggregation(buildEntityAggregation(entityType, null));
        }

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(searchSourceBuilder);

        System.out.println("-------------------");
        System.out.println(searchRequest.source().toString());
        System.out.println("-------------------");
        return searchRequest;
    }

    /**
     * Given a search request and some after keys, return a new search request with the
     *  after keys applied to the composite aggregation
     * @param afterKeysByEntityType
     * @param originalSearchRequest
     * @return
     */
    public static SearchRequest updateAggregationsWithAfterKeys(
        Map<EntityType, Map<String, Object>> afterKeysByEntityType,
        SearchRequest originalSearchRequest
    ) {
        SearchSourceBuilder originalSourceBuilder = originalSearchRequest.source();
        QueryBuilder originalQuery = originalSourceBuilder.query();

        SearchSourceBuilder newSourceBuilder = new SearchSourceBuilder();
        newSourceBuilder.query(originalQuery);
        newSourceBuilder.size(0);

        // Update the aggregations with after keys
        for (EntityType entityType : EntityType.values()) {
            Map<String, Object> afterKeys = afterKeysByEntityType.get(entityType);
            CompositeAggregationBuilder newCompositeAggregationBuilder = buildEntityAggregation(entityType, afterKeys);
            newSourceBuilder.aggregation(newCompositeAggregationBuilder);
        }

        // Now build a new search request
        SearchRequest newSearchRequest = new SearchRequest();
        newSearchRequest.source(newSourceBuilder);
        return newSearchRequest;
    }

    private static EntityType getEntityTypeByAggregationName(String aggregationName) {
        for (EntityType entityType : EntityType.values()) {
            if (aggregationName.equals(EntityTypeUtils.getAggregationNameForEntityType(entityType))) {
                return entityType;
            }
        }
        return null;
    }
}
