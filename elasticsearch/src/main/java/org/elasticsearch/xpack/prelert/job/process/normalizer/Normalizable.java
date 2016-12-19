/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
     */
    boolean isContainerOnly();

    Level getLevel();

    String getPartitionFieldName();

    String getPartitionFieldValue();

    String getPersonFieldName();

    String getFunctionName();

    String getValueFieldName();

    double getProbability();

    double getNormalizedScore();

    void setNormalizedScore(double normalizedScore);

    List<Integer> getChildrenTypes();

    List<Normalizable> getChildren();

    List<Normalizable> getChildren(int type);

    /**
     * Set the aggregate normalized score for a type of children
     *
     * @param childrenType the integer that corresponds to a children type
     * @param maxScore     the aggregate normalized score of the children
     * @return true if the score has changed or false otherwise
     */
    boolean setMaxChildrenScore(int childrenType, double maxScore);

    /**
     * If this {@code Normalizable} holds the score of its parent,
     * set the parent score
     *
     * @param parentScore the score of the parent {@code Normalizable}
     */
    void setParentScore(double parentScore);

    void resetBigChangeFlag();

    void raiseBigChangeFlag();
}
