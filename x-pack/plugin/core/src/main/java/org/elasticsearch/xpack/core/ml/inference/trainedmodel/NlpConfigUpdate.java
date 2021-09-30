/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

public abstract class NlpConfigUpdate implements InferenceConfigUpdate {

    @Override
    public InferenceConfig toConfig() {
        throw new UnsupportedOperationException("cannot serialize to nodes before 7.8");
    }

}
