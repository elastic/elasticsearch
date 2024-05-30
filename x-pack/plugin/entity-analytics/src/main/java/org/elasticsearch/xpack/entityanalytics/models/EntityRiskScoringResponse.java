/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entityanalytics.models;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.metrics.TopHits;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.entityanalytics.common.Constants;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class EntityRiskScoringResponse extends ActionResponse implements ToXContentObject {

    private final Map<String, Map<String, Double>> response;

    public EntityRiskScoringResponse(Map<String, Map<String, Double>> response) {
        this.response = response;
    }

    public static EntityRiskScoringResponse fromSearchResponse(SearchResponse searchResponse, EntityType[] entityTypes) {
        var resp = processResults(entityTypes, searchResponse);
        return new EntityRiskScoringResponse(resp);
    }

    private static Map<String, Double> processResultsForEntityType(EntityType entityType, SearchResponse searchResponse) {
        System.out.println("HELLOWORLD RESPONSE " + searchResponse);
        String aggregationName = (entityType.equals(EntityType.Host)) ? "host" : "user";
        String identifierField = (entityType.equals(EntityType.Host)) ? "host.name" : "user.name";

        Map<String, Double> results = new HashMap<>();
        CompositeAggregation entityCompositeAgg = searchResponse.getAggregations().get(aggregationName);
        for (CompositeAggregation.Bucket eachBucket : entityCompositeAgg.getBuckets()) {
            TopHits hits = eachBucket.getAggregations().get("top_inputs");
            var alertRiskScores = new ArrayList<Double>();
            hits.getHits().forEach(eachAlert -> {
                var alertSource = eachAlert.getSourceAsMap();
                alertRiskScores.add(Double.parseDouble(alertSource.get("kibana.alert.risk_score").toString()));
            });
            alertRiskScores.sort(Collections.reverseOrder());

            double totalScore = 0;
            for (int i = 0; i < alertRiskScores.size(); i++) {
                totalScore += alertRiskScores.get(i) / Math.pow(i + 1, Constants.RISK_SCORING_SUM_VALUE);
            }
            double normalizedScore = (Constants.RISK_SCORING_NORMALIZATION_MAX * totalScore) / Constants.RISK_SCORING_SUM_MAX;
            results.put(eachBucket.getKey().get(identifierField).toString(), normalizedScore);
        }
        return results;
    }

    private static Map<String, Map<String, Double>> processResults(EntityType[] entityTypes, SearchResponse searchResponse) {
        Map<String, Map<String, Double>> results = new HashMap<>();
        for (EntityType entityType : entityTypes) {
            results.put(entityType.toString(), processResultsForEntityType(entityType, searchResponse));
        }
        return results;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(response.toString());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        for (Map.Entry<String, Map<String, Double>> entry : response.entrySet()) {
            builder.field(entry.getKey(), entry.getValue());
        }
        builder.endObject();
        return builder;
    }
}
