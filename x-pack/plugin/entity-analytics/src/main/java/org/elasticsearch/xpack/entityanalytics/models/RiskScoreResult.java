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
import java.util.List;

public class RiskScoreResult implements ToXContentObject {
    public EntityScore[] userScores;
    public EntityScore[] hostScores;

    public RiskScoreResult() {
        this.userScores = new EntityScore[0];
        this.hostScores = new EntityScore[0];
    }

    public RiskScoreResult(EntityScore[] userScores, EntityScore[] hostScores) {
        this.userScores = userScores;
        this.hostScores = hostScores;
    }

    private void setScores(EntityScore[] userScores, EntityScore[] hostScores) {
        this.userScores = userScores;
        this.hostScores = hostScores;
    }

    public void mergeResult(RiskScoreResult result) {
        List<EntityScore> mergedUserScores = new ArrayList<>();
        List<EntityScore> mergedHostScores = new ArrayList<>();

        mergedUserScores.addAll(List.of(userScores));
        mergedUserScores.addAll(List.of(result.userScores));

        mergedHostScores.addAll(List.of(hostScores));
        mergedHostScores.addAll(List.of(result.hostScores));

        setScores(mergedUserScores.toArray(new EntityScore[0]), mergedHostScores.toArray(new EntityScore[0]));
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
