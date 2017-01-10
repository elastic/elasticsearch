/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.normalizer;

import org.elasticsearch.xpack.ml.job.results.AnomalyRecord;

import java.util.Objects;


class RecordNormalizable extends AbstractLeafNormalizable {
    private final AnomalyRecord record;

    public RecordNormalizable(AnomalyRecord record) {
        this.record = Objects.requireNonNull(record);
    }

    @Override
    public Level getLevel() {
        return Level.LEAF;
    }

    @Override
    public String getPartitionFieldName() {
        return record.getPartitionFieldName();
    }

    @Override
    public String getPartitionFieldValue() {
        return record.getPartitionFieldValue();
    }

    @Override
    public String getPersonFieldName() {
        String over = record.getOverFieldName();
        return over != null ? over : record.getByFieldName();
    }

    @Override
    public String getFunctionName() {
        return record.getFunction();
    }

    @Override
    public String getValueFieldName() {
        return record.getFieldName();
    }

    @Override
    public double getProbability() {
        return record.getProbability();
    }

    @Override
    public double getNormalizedScore() {
        return record.getNormalizedProbability();
    }

    @Override
    public void setNormalizedScore(double normalizedScore) {
        record.setNormalizedProbability(normalizedScore);
    }

    @Override
    public void setParentScore(double parentScore) {
        record.setAnomalyScore(parentScore);
    }

    @Override
    public void resetBigChangeFlag() {
        record.resetBigNormalizedUpdateFlag();
    }

    @Override
    public void raiseBigChangeFlag() {
        record.raiseBigNormalizedUpdateFlag();
    }
}
