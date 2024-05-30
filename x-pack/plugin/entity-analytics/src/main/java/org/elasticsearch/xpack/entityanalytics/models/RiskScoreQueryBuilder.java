/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entityanalytics.models;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FieldValueFactorFunctionBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.xpack.entityanalytics.common.EntityTypeUtils;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

public class RiskScoreQueryBuilder {
    private static CompositeAggregationBuilder buildEntityAggregation(EntityType entityType) {
        String identifierField = EntityTypeUtils.getIdentifierFieldForEntityType(entityType);
        String aggregationName = EntityTypeUtils.getAggregationNameForEntityType(entityType);
        List<CompositeValuesSourceBuilder<?>> sources = new ArrayList<>();
        sources.add(new TermsValuesSourceBuilder(identifierField).field(identifierField));

        return AggregationBuilders.composite(aggregationName, sources)
            .size(10)
            .subAggregation(
                AggregationBuilders.topHits("top_inputs")
                    .size(100)
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

        for (EntityType entityType : entityTypes) {
            searchSourceBuilder.aggregation(buildEntityAggregation(entityType));
        }

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(searchSourceBuilder);
        return searchRequest;
    }
}
