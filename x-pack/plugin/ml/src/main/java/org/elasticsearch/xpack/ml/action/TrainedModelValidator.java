/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.core.ml.MlConfigVersion;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ModelPackageConfig;

import static org.elasticsearch.core.Strings.format;

/**
 * {@TrainedModelValidator} analyzes a trained model config to find various issues w.r.t. the various combinations of model types,
 * packages, etc.
 */
final class TrainedModelValidator {

    static void validatePackage(
        TrainedModelConfig.Builder trainedModelConfig,
        ModelPackageConfig resolvedModelPackageConfig,
        ClusterState state
    ) {
        validateMinimumVersion(resolvedModelPackageConfig, state);
        // check that user doesn't try to override parts that are
        trainedModelConfig.validateNoPackageOverrides();
    }

    static void validateMinimumVersion(ModelPackageConfig resolvedModelPackageConfig, ClusterState state) {
        MlConfigVersion minimumVersion;

        // {@link Version#fromString} interprets an empty string as current version, so we have to check ourselves
        if (Strings.isNullOrEmpty(resolvedModelPackageConfig.getMinimumVersion())) {
            throw new ActionRequestValidationException().addValidationError(
                format(
                    "Invalid model package configuration for [%s], missing minimum_version property",
                    resolvedModelPackageConfig.getPackagedModelId()
                )
            );
        }

        try {
            minimumVersion = MlConfigVersion.fromString(resolvedModelPackageConfig.getMinimumVersion());
        } catch (IllegalArgumentException e) {
            throw new ActionRequestValidationException().addValidationError(
                format(
                    "Invalid model package configuration for [%s], failed to parse the minimum_version property",
                    resolvedModelPackageConfig.getPackagedModelId()
                )
            );
        }

        if (MlConfigVersion.getMinMlConfigVersion(state.nodes()).before(minimumVersion)) {
            throw new ActionRequestValidationException().addValidationError(
                format(
                    "The model [%s] requires that all nodes are at least version [%s]",
                    resolvedModelPackageConfig.getPackagedModelId(),
                    resolvedModelPackageConfig.getMinimumVersion()
                )
            );
        }
    }

    private TrainedModelValidator() {}
}
