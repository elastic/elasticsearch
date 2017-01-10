/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.normalizer;

import java.util.List;

interface Normalizable {
    /**
     * A {@code Normalizable} may be the owner of scores or just a
     * container of other {@code Normalizable} objects. A container only
     * {@code Normalizable} does not have any scores to be normalized.
     * It contains scores that are aggregates of its children.
     *
     * @return true if this {@code Normalizable} is only a container
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
