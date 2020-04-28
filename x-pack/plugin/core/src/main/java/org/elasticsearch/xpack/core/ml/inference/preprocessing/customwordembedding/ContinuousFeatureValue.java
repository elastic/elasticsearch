/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.preprocessing.customwordembedding;

/**
 * Extracted feature that has a continuous weight value
 */
public class ContinuousFeatureValue extends FeatureValue {

    private final int id;
    private final double weight;

    public ContinuousFeatureValue(int id, double weight) {
        this.id = id;
        this.weight = weight;
    }

    @Override
    public int getRow() {
        return id;
    }

    @Override
    public double getWeight() {
        return weight;
    }
}
