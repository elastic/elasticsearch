/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
    private static final int RECORD = 1;
    private static final int PARTITION_SCORE = 2;
    private static final List<Integer> CHILDREN_TYPES =
            Arrays.asList(BUCKET_INFLUENCER, RECORD, PARTITION_SCORE);

    private final Bucket bucket;

    public BucketNormalizable(Bucket bucket) {
        this.bucket = Objects.requireNonNull(bucket);
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

    @Override
    public List<Integer> getChildrenTypes() {
        return CHILDREN_TYPES;
    }

    @Override
    public List<Normalizable> getChildren() {
        List<Normalizable> children = new ArrayList<>();
        for (Integer type : getChildrenTypes()) {
            children.addAll(getChildren(type));
        }
        return children;
    }

    @Override
    public List<Normalizable> getChildren(int type) {
        List<Normalizable> children = new ArrayList<>();
        switch (type) {
            case BUCKET_INFLUENCER:
                bucket.getBucketInfluencers().stream().forEach(
                        influencer -> children.add(new BucketInfluencerNormalizable(influencer)));
                break;
            case RECORD:
                bucket.getRecords().stream().forEach(
                        record -> children.add(new RecordNormalizable(record)));
                break;
            case PARTITION_SCORE:
                bucket.getPartitionScores().stream().forEach(
                        partitionScore -> children.add(new PartitionScoreNormalizable(partitionScore)));
                break;
            default:
                throw new IllegalArgumentException("Invalid type: " + type);
        }
        return children;
    }

    @Override
    public boolean setMaxChildrenScore(int childrenType, double maxScore) {
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
    public void resetBigChangeFlag() {
        bucket.resetBigNormalizedUpdateFlag();
    }

    @Override
    public void raiseBigChangeFlag() {
        bucket.raiseBigNormalizedUpdateFlag();
    }
}
