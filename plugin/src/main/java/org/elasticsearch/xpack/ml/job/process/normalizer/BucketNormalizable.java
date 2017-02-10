/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.normalizer;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.ml.job.results.Bucket;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.ml.job.process.normalizer.Normalizable.ChildType.BUCKET_INFLUENCER;
import static org.elasticsearch.xpack.ml.job.process.normalizer.Normalizable.ChildType.PARTITION_SCORE;
import static org.elasticsearch.xpack.ml.job.process.normalizer.Normalizable.ChildType.RECORD;


public class BucketNormalizable extends Normalizable {

    private static final List<ChildType> CHILD_TYPES = Arrays.asList(BUCKET_INFLUENCER, RECORD, PARTITION_SCORE);

    private final Bucket bucket;

    private List<RecordNormalizable> records = Collections.emptyList();

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
    public String getFunctionName() {
        return null;
    }

    @Override
    public String getValueFieldName() {
        return null;
    }

    @Override
    public double getProbability() {
        throw new IllegalStateException("Bucket is container only");
    }

    @Override
    public double getNormalizedScore() {
        return bucket.getAnomalyScore();
    }

    @Override
    public void setNormalizedScore(double normalizedScore) {
        bucket.setAnomalyScore(normalizedScore);
    }

    public List<RecordNormalizable> getRecords() {
        return records;
    }

    public void setRecords(List<RecordNormalizable> records) {
        this.records = records;
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
            case RECORD:
                children.addAll(records);
                break;
            case PARTITION_SCORE:
                children.addAll(bucket.getPartitionScores().stream()
                        .map(ps -> new PartitionScoreNormalizable(ps, getOriginatingIndex()))
                        .collect(Collectors.toList()));
                break;
            default:
                throw new IllegalArgumentException("Invalid type: " + type);
        }
        return children;
    }

    @Override
    public boolean setMaxChildrenScore(ChildType childrenType, double maxScore) {
        double oldScore = 0.0;
        switch (childrenType) {
            case BUCKET_INFLUENCER:
                oldScore = bucket.getAnomalyScore();
                bucket.setAnomalyScore(maxScore);
                break;
            case RECORD:
                oldScore = bucket.getMaxNormalizedProbability();
                bucket.setMaxNormalizedProbability(maxScore);
                break;
            case PARTITION_SCORE:
                break;
            default:
                throw new IllegalArgumentException("Invalid type: " + childrenType);
        }
        return maxScore != oldScore;
    }

    @Override
    public void setParentScore(double parentScore) {
        throw new IllegalStateException("Bucket has no parent");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return bucket.toXContent(builder, params);
    }
}
