/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.process.normalizer;

import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.job.results.Influencer;

import java.io.IOException;
import java.util.Objects;

class InfluencerNormalizable extends AbstractLeafNormalizable {
    private final Influencer influencer;

    InfluencerNormalizable(Influencer influencer, String indexName) {
        super(indexName);
        this.influencer = Objects.requireNonNull(influencer);
    }

    @Override
    public String getId() {
        return influencer.getId();
    }

    @Override
    public Level getLevel() {
        return Level.INFLUENCER;
    }

    @Override
    public String getPartitionFieldName() {
        return null;
    }

    @Override
    public String getPartitionFieldValue() {
        return null;
    }

    @Override
    public String getPersonFieldName() {
        return influencer.getInfluencerFieldName();
    }

    @Override
    public String getPersonFieldValue() {
        return influencer.getInfluencerFieldValue();
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
        return influencer.getProbability();
    }

    @Override
    public double getNormalizedScore() {
        return influencer.getInfluencerScore();
    }

    @Override
    public void setNormalizedScore(double normalizedScore) {
        influencer.setInfluencerScore(normalizedScore);
    }

    @Override
    public void setParentScore(double parentScore) {
        throw new IllegalStateException("Influencer has no parent");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return influencer.toXContent(builder, params);
    }
}
