/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entityanalytics.models;

public class RiskScoreResult {
    public EntityScore[] userScores;
    public EntityScore[] hostScores;

    public RiskScoreResult(EntityScore[] userScores, EntityScore[] hostScores) {
        this.userScores = userScores;
        this.hostScores = hostScores;
    }
}
