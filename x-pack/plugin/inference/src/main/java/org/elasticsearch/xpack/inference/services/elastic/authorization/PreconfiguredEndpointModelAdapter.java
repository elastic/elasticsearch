/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.authorization;

import org.elasticsearch.inference.EmptySecretSettings;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceComponents;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceModel;
import org.elasticsearch.xpack.inference.services.elastic.InternalPreconfiguredEndpoints;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.inference.services.elastic.InternalPreconfiguredEndpoints.EIS_PRECONFIGURED_ENDPOINT_IDS;

public class PreconfiguredEndpointModelAdapter {
    public static List<Model> getModels(Set<String> inferenceIds, ElasticInferenceServiceComponents elasticInferenceServiceComponents) {
        return inferenceIds.stream()
            .sorted()
            .filter(EIS_PRECONFIGURED_ENDPOINT_IDS::contains)
            .map(id -> createModel(InternalPreconfiguredEndpoints.getWithInferenceId(id), elasticInferenceServiceComponents))
            .toList();
    }

    public static Model createModel(
        InternalPreconfiguredEndpoints.MinimalModel minimalModel,
        ElasticInferenceServiceComponents elasticInferenceServiceComponents
    ) {
        return new ElasticInferenceServiceModel(
            minimalModel.configurations(),
            new ModelSecrets(EmptySecretSettings.INSTANCE),
            minimalModel.rateLimitServiceSettings(),
            elasticInferenceServiceComponents
        );
    }

    private PreconfiguredEndpointModelAdapter() {}
}
