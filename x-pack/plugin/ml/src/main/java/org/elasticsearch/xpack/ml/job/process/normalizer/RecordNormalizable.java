/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.normalizer;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.job.results.AnomalyRecord;

import java.io.IOException;
import java.util.Objects;


class RecordNormalizable extends AbstractLeafNormalizable {
    private final AnomalyRecord record;

    RecordNormalizable(AnomalyRecord record, String indexName) {
        super(indexName);
        this.record = Objects.requireNonNull(record);
    }

    @Override
    public String getId() {
        return record.getId();
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
    public String getPersonFieldValue() {
        String over = record.getOverFieldValue();
        return over != null ? over : record.getByFieldValue();
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
        return record.getRecordScore();
    }

    @Override
    public void setNormalizedScore(double normalizedScore) {
        record.setRecordScore(normalizedScore);
    }

    @Override
    public void setParentScore(double parentScore) {
        // nothing to do
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return record.toXContent(builder, params);
    }

    public AnomalyRecord getRecord() {
        return record;
    }
}
