/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entityanalytics.models;

import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RiskScoreResult implements ToXContentObject {
    public EntityScore[] userScores;
    public EntityScore[] hostScores;

    public RiskScoreResult(EntityScore[] userScores, EntityScore[] hostScores) {
        this.userScores = userScores;
        this.hostScores = hostScores;
    }

    public static RiskScoreResult mergeResults(List<RiskScoreResult> results) {
        List<EntityScore> mergedUserScores = new ArrayList<>();
        List<EntityScore> mergedHostScores = new ArrayList<>();

        for (RiskScoreResult result : results) {
            if (result.userScores != null) {
                Collections.addAll(mergedUserScores, result.userScores);
            }
            if (result.hostScores != null) {
                Collections.addAll(mergedHostScores, result.hostScores);
            }
        }

        EntityScore[] mergedUserScoresArray = mergedUserScores.toArray(new EntityScore[0]);
        EntityScore[] mergedHostScoresArray = mergedHostScores.toArray(new EntityScore[0]);

        return new RiskScoreResult(mergedUserScoresArray, mergedHostScoresArray);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject("scores");
        builder.field("user", userScores);
        builder.field("host", hostScores);
        builder.endObject();
        builder.endObject();
        return builder;
    }
}
