/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.preprocessing.customwordembedding;

/**
 * Discrete extracted value with a static weight of 1.0
 */
public class DiscreteFeatureValue extends FeatureValue {

    private final int id;

    DiscreteFeatureValue(int id) {
        this.id = id;
    }

    @Override
    public int getRow() {
        return id;
    }
}
