/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.normalizer;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.job.results.PartitionScore;

import java.io.IOException;
import java.util.Objects;


public class PartitionScoreNormalizable extends AbstractLeafNormalizable {
    private final PartitionScore score;

    public PartitionScoreNormalizable(PartitionScore score, String indexName) {
        super(indexName);
        this.score = Objects.requireNonNull(score);
    }

    @Override
    public String getId() {
        throw new UnsupportedOperationException("PartitionScore has no ID as it should not be persisted outside of the owning bucket");
    }

    @Override
    public Level getLevel() {
        return Level.PARTITION;
    }

    @Override
    public String getPartitionFieldName() {
        return score.getPartitionFieldName();
    }

    @Override
    public String getPartitionFieldValue() {
        return score.getPartitionFieldValue();
    }

    @Override
    public String getPersonFieldName() {
        return null;
    }

    @Override
    public String getPersonFieldValue() {
        return null;
    }

    @Override
    public String getFunctionName() {
        return null;
    }

    @Override
    public String getValueFieldName() {
        return null;
    }

    @Override
    public double getProbability() {
        return score.getProbability();
    }

    @Override
    public double getNormalizedScore() {
        return score.getRecordScore();
    }

    @Override
    public void setNormalizedScore(double normalizedScore) {
        score.setRecordScore(normalizedScore);
    }

    @Override
    public void setParentScore(double parentScore) {
        // Do nothing as it is not holding the parent score.
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return score.toXContent(builder, params);
    }
}
