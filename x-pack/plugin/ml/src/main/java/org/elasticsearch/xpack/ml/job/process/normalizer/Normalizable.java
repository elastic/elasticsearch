/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.process.normalizer;

import org.elasticsearch.xcontent.ToXContentObject;

import java.util.List;
import java.util.Objects;

public abstract class Normalizable implements ToXContentObject {
    public enum ChildType {
        BUCKET_INFLUENCER,
        RECORD
    }

    private final String indexName;
    private boolean hadBigNormalizedUpdate;

    public Normalizable(String indexName) {
        this.indexName = Objects.requireNonNull(indexName);
    }

    /**
     * The document ID of the underlying result.
     * @return The document Id string
     */
    public abstract String getId();

    /**
     * A {@code Normalizable} may be the owner of scores or just a
     * container of other {@code Normalizable} objects. A container only
     * {@code Normalizable} does not have any scores to be normalized.
     * It contains scores that are aggregates of its children.
     *
     * @return true if this {@code Normalizable} is only a container
     */
    abstract boolean isContainerOnly();

    abstract Level getLevel();

    abstract String getPartitionFieldName();

    abstract String getPartitionFieldValue();

    abstract String getPersonFieldName();

    abstract String getPersonFieldValue();

    abstract String getFunctionName();

    abstract String getValueFieldName();

    abstract double getProbability();

    abstract double getNormalizedScore();

    abstract void setNormalizedScore(double normalizedScore);

    abstract List<ChildType> getChildrenTypes();

    abstract List<Normalizable> getChildren();

    abstract List<Normalizable> getChildren(ChildType type);

    /**
     * Set the aggregate normalized score for a type of children
     *
     * @param type         the child type
     * @param maxScore     the aggregate normalized score of the children
     * @return true if the score has changed or false otherwise
     */
    abstract boolean setMaxChildrenScore(ChildType type, double maxScore);

    /**
     * If this {@code Normalizable} holds the score of its parent,
     * set the parent score
     *
     * @param parentScore the score of the parent {@code Normalizable}
     */
    abstract void setParentScore(double parentScore);

    public boolean hadBigNormalizedUpdate() {
        return hadBigNormalizedUpdate;
    }

    public void resetBigChangeFlag() {
        hadBigNormalizedUpdate = false;
    }

    public void raiseBigChangeFlag() {
        hadBigNormalizedUpdate = true;
    }

    public String getOriginatingIndex() {
        return indexName;
    }
}
