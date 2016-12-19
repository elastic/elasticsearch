/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
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
        return score.getAnomalyScore();
    }

    @Override
    public void setNormalizedScore(double normalizedScore) {
        score.setAnomalyScore(normalizedScore);
    }

    @Override
    public void setParentScore(double parentScore) {
        // Do nothing as it is not holding the parent score.
    }

    @Override
    public void resetBigChangeFlag() {
        score.resetBigNormalizedUpdateFlag();
    }

    @Override
    public void raiseBigChangeFlag() {
        score.raiseBigNormalizedUpdateFlag();
    }
}
