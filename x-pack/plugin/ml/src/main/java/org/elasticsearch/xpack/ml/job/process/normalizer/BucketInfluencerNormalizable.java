/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.process.normalizer;

import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.job.results.BucketInfluencer;

import java.io.IOException;
import java.util.Objects;

class BucketInfluencerNormalizable extends AbstractLeafNormalizable {
    private final BucketInfluencer bucketInfluencer;

    BucketInfluencerNormalizable(BucketInfluencer influencer, String indexName) {
        super(indexName);
        bucketInfluencer = Objects.requireNonNull(influencer);
    }

    @Override
    public String getId() {
        return bucketInfluencer.getId();
    }

    @Override
    public Level getLevel() {
        return BucketInfluencer.BUCKET_TIME.equals(bucketInfluencer.getInfluencerFieldName()) ? Level.ROOT : Level.BUCKET_INFLUENCER;
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
        return bucketInfluencer.getInfluencerFieldName();
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
        return bucketInfluencer.getProbability();
    }

    @Override
    public double getNormalizedScore() {
        return bucketInfluencer.getAnomalyScore();
    }

    @Override
    public void setNormalizedScore(double normalizedScore) {
        bucketInfluencer.setAnomalyScore(normalizedScore);
    }

    @Override
    public void setParentScore(double parentScore) {
        // Do nothing as it is not holding the parent score.
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return bucketInfluencer.toXContent(builder, params);
    }
}
