/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.preprocessing.customwordembedding;

/**
 * Extracted feature values from the text
 */
public abstract class FeatureValue {

    public double getWeight() {
        return 1.0;
    }

    public abstract int getRow();

}
