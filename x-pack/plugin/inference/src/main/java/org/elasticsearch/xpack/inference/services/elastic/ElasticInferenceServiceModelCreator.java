/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic;

import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.xpack.inference.services.ModelCreator;

/**
 * Abstract base class for {@link ElasticInferenceServiceModel} creator instances from config maps
 * or {@link ModelConfigurations} and {@link ModelSecrets} objects.
 */
public abstract class ElasticInferenceServiceModelCreator<M extends ElasticInferenceServiceModel> implements ModelCreator<M> {

    protected final ElasticInferenceServiceComponents elasticInferenceServiceComponents;

    protected ElasticInferenceServiceModelCreator(ElasticInferenceServiceComponents elasticInferenceServiceComponents) {
        this.elasticInferenceServiceComponents = elasticInferenceServiceComponents;
    }
}
