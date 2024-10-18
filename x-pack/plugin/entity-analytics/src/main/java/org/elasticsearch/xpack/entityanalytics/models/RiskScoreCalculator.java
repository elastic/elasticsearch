/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entityanalytics.models;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.metrics.TopHits;
import org.elasticsearch.xpack.entityanalytics.common.Constants;
import org.elasticsearch.xpack.entityanalytics.common.EntityTypeUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class RiskScoreCalculator {
    /**
     * Calculate the risk score and top inputs for the given search response
     * @param entityType
     * @param searchResponse
     * @return
     */
    private static EntityScore[] processResultsForEntityType(EntityType entityType, SearchResponse searchResponse) {
        String aggregationName = EntityTypeUtils.getAggregationNameForEntityType(entityType);
        String identifierField = EntityTypeUtils.getIdentifierFieldForEntityType(entityType);
        List<EntityScore> results = new ArrayList<>();
        CompositeAggregation entityCompositeAgg = searchResponse.getAggregations().get(aggregationName);

        for (CompositeAggregation.Bucket eachBucket : entityCompositeAgg.getBuckets()) {
            TopHits topHits = eachBucket.getAggregations().get("top_inputs");
            List<RiskInput> riskInputs = new ArrayList<>();
            double[] totalScore = { 0.0 };
            AtomicInteger counter = new AtomicInteger(0);
            var hits = topHits.getHits();

            hits.forEach(eachAlert -> {
                int i = counter.getAndIncrement();
                var alertSource = eachAlert.getSourceAsMap();
                var riskScoreDouble = Double.parseDouble(alertSource.get("kibana.alert.risk_score").toString());

                double riskContribution = riskScoreDouble / Math.pow(i + 1, Constants.RISK_SCORING_SUM_VALUE);

                if (riskInputs.size() < Constants.MAX_INPUTS_COUNT) {
                    double normalizedRiskContribution = 100 * riskContribution / Constants.RISK_SCORING_SUM_MAX;
                    riskInputs.add(RiskInput.fromAlertHit(eachAlert, normalizedRiskContribution));
                }

                totalScore[0] += riskContribution;
            });
            double normalizedScore = (Constants.RISK_SCORING_NORMALIZATION_MAX * totalScore[0]) / Constants.RISK_SCORING_SUM_MAX;
            int category1Count = Math.toIntExact(hits.getTotalHits().value);
            String identifierValue = eachBucket.getKey().get(identifierField).toString();
            results.add(
                new EntityScore(
                    identifierField,
                    identifierValue,
                    totalScore[0],
                    category1Count,
                    totalScore[0] * Constants.GLOBAL_IDENTIFIER_TYPE_WEIGHT,
                    normalizedScore,
                    riskInputs.toArray(new RiskInput[0])
                )
            );
        }
        return results.toArray(new EntityScore[0]);
    }

    public static RiskScoreResult calculateRiskScores(EntityType[] entityTypes, SearchResponse searchResponse) {
        EntityScore[] hostResults = {};
        EntityScore[] userResults = {};
        var entityTypeList = Arrays.asList(entityTypes);

        if (entityTypeList.contains(EntityType.Host)) {
            hostResults = processResultsForEntityType(EntityType.Host, searchResponse);
        }

        if (entityTypeList.contains(EntityType.User)) {
            userResults = processResultsForEntityType(EntityType.User, searchResponse);
        }

        return new RiskScoreResult(userResults, hostResults);
    }
}
