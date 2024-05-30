/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entityanalytics.models;

import java.util.Map;

public class RiskScoreResult {
    public Map<String, Double> userScores;
    public Map<String, Double> hostScores;

    public RiskScoreResult(Map<String, Double> userScores, Map<String, Double> hostScores) {
        this.userScores = userScores;
        this.hostScores = hostScores;
    }
}
