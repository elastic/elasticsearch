/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.normalizer;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.ml.job.process.normalizer.Normalizable.ChildType.BUCKET_INFLUENCER;

public class BucketNormalizable extends Normalizable {

    private static final List<ChildType> CHILD_TYPES = Arrays.asList(BUCKET_INFLUENCER);

    private final Bucket bucket;

    public BucketNormalizable(Bucket bucket, String indexName) {
        super(indexName);
        this.bucket = Objects.requireNonNull(bucket);
    }

    public Bucket getBucket() {
        return bucket;
    }

    @Override
    public String getId() {
        return bucket.getId();
    }

    @Override
    public boolean isContainerOnly() {
        return true;
    }

    @Override
    public Level getLevel() {
        return Level.ROOT;
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
        throw new UnsupportedOperationException("Bucket is container only");
    }

    @Override
    public double getNormalizedScore() {
        return bucket.getAnomalyScore();
    }

    @Override
    public void setNormalizedScore(double normalizedScore) {
        bucket.setAnomalyScore(normalizedScore);
    }

    @Override
    public List<ChildType> getChildrenTypes() {
        return CHILD_TYPES;
    }

    @Override
    public List<Normalizable> getChildren() {
        List<Normalizable> children = new ArrayList<>();
        for (ChildType type : getChildrenTypes()) {
            children.addAll(getChildren(type));
        }
        return children;
    }

    @Override
    public List<Normalizable> getChildren(ChildType type) {
        List<Normalizable> children = new ArrayList<>();
        switch (type) {
            case BUCKET_INFLUENCER:
                children.addAll(bucket.getBucketInfluencers().stream()
                        .map(bi -> new BucketInfluencerNormalizable(bi, getOriginatingIndex()))
                        .collect(Collectors.toList()));
                break;
            default:
                throw new IllegalArgumentException("Invalid type: " + type);
        }
        return children;
    }

    @Override
    public boolean setMaxChildrenScore(ChildType childrenType, double maxScore) {
        switch (childrenType) {
            case BUCKET_INFLUENCER:
                double oldScore = bucket.getAnomalyScore();
                bucket.setAnomalyScore(maxScore);
                return maxScore != oldScore;
            default:
                throw new IllegalArgumentException("Invalid type: " + childrenType);
        }

    }

    @Override
    public void setParentScore(double parentScore) {
        throw new UnsupportedOperationException("Bucket has no parent");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return bucket.toXContent(builder, params);
    }
}
