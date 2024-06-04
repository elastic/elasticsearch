/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entityanalytics.models;

import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public class EntityScore implements ToXContentObject {
    private final String idField;
    private final String idValue;
    private final double category1score;
    private final double category1count;
    private final double calculatedScore;
    private final double calculatedScoreNorm;
    private final RiskInput[] riskInputs;

    /**
     * Represents a risk score for a single entity
     * @param idField
     * @param idValue
     * @param category1score
     * @param category1count
     * @param calculatedScore
     * @param calculatedScoreNorm
     * @param riskInputs
     */
    public EntityScore(
        String idField,
        String idValue,
        double category1score,
        int category1count,
        double calculatedScore,
        double calculatedScoreNorm,
        RiskInput[] riskInputs
    ) {
        this.idField = idField;
        this.idValue = idValue;
        this.category1score = category1score;
        this.category1count = category1count;
        this.calculatedScore = calculatedScore;
        this.calculatedScoreNorm = calculatedScoreNorm;
        this.riskInputs = riskInputs;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field("id_field", idField);
        builder.field("id_value", idValue);
        builder.field("category_1_score", category1score);
        builder.field("category_1_count", category1count);
        builder.field("calculated_score", calculatedScore);
        builder.field("calculated_score_norm", calculatedScoreNorm);
        builder.field("inputs", riskInputs);
        builder.endObject();
        return builder;
    }
}
