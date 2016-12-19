/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
    }

    @Override
    public Level getLevel() {
        return BucketInfluencer.BUCKET_TIME.equals(bucketInfluencer.getInfluencerFieldName()) ?
                Level.ROOT : Level.BUCKET_INFLUENCER;
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
    public void resetBigChangeFlag() {
        // Do nothing
    }

    @Override
    public void raiseBigChangeFlag() {
        // Do nothing
    }
}
