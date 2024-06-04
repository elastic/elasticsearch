/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entityanalytics.models;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FieldValueFactorFunctionBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.entityanalytics.common.EntityTypeUtils;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RiskScoreQueryHelper {

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

    private static CompositeAggregationBuilder buildEntityAggregation(EntityType entityType, Map<String, Object> afterKeys) {
        String identifierField = EntityTypeUtils.getIdentifierFieldForEntityType(entityType);
        String aggregationName = EntityTypeUtils.getAggregationNameForEntityType(entityType);
        List<CompositeValuesSourceBuilder<?>> sources = new ArrayList<>();
        sources.add(new TermsValuesSourceBuilder(identifierField).field(identifierField));

        CompositeAggregationBuilder compositeAggregationBuilder = AggregationBuilders.composite(aggregationName, sources)
            .size(500) // TODO: page size
            .subAggregation(
                AggregationBuilders.topHits("top_inputs")
                    .size(100)
                    .sort("kibana.alert.risk_score", SortOrder.DESC)
                    .fetchSource(
                        FetchSourceContext.of(
                            true,
                            new String[] {
                                "kibana.alert.risk_score",
                                "event.kind",
                                "kibana.alert.rule.name",
                                "kibana.alert.uuid",
                                "@timestamp" },
                            null
                        )
                    )
            );

        if (afterKeys != null) {
            compositeAggregationBuilder.aggregateAfter(afterKeys);
        }

        return compositeAggregationBuilder;
    }

    public static SearchRequest buildRiskScoreSearchRequest(String index, EntityType[] entityTypes) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Calculate current time and 30 days ago in Unix time as 'now-30d' was causing 'failed to create query: For input
        // string: \"now-30d\"'
        long now = Instant.now().toEpochMilli();
        long nowMinus30Days = Instant.now().minus(30, ChronoUnit.DAYS).toEpochMilli();

        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery()
            .filter(QueryBuilders.rangeQuery("@timestamp").gte(nowMinus30Days).lt(now))
            .filter(QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("kibana.alert.workflow_status", "closed")))
            .filter(QueryBuilders.existsQuery("kibana.alert.risk_score"))
            .should(QueryBuilders.matchAllQuery());

        FunctionScoreQueryBuilder functionScoreQuery = QueryBuilders.functionScoreQuery(
            boolQuery,
            new FieldValueFactorFunctionBuilder("kibana.alert.risk_score")
        );

        searchSourceBuilder.query(functionScoreQuery);
        searchSourceBuilder.size(0);

        for (EntityType entityType : entityTypes) {
            searchSourceBuilder.aggregation(buildEntityAggregation(entityType, null));
        }

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(searchSourceBuilder);
        return searchRequest;
    }

    public static SearchRequest updateAggregationsWithAfterKeys(
        Map<EntityType, Map<String, Object>> afterKeysByEntityType,
        SearchRequest originalSearchRequest
    ) {
        // Extract the original query and filters
        SearchSourceBuilder originalSourceBuilder = originalSearchRequest.source();
        QueryBuilder originalQuery = originalSourceBuilder.query();

        // Create a new search source builder
        SearchSourceBuilder newSourceBuilder = new SearchSourceBuilder();
        newSourceBuilder.query(originalQuery);
        newSourceBuilder.size(0);

        // Update aggregations with after keys
        for (EntityType entityType : EntityType.values()) {
            Map<String, Object> afterKeys = afterKeysByEntityType.get(entityType);
            CompositeAggregationBuilder newCompositeAggregationBuilder = buildEntityAggregation(entityType, afterKeys);
            newSourceBuilder.aggregation(newCompositeAggregationBuilder);
        }

        // Build the new search request
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
