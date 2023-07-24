/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ModelPackageConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ModelPackageConfigTests;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TrainedModelValidatorTests extends ESTestCase {

    public void testValidateMinimumVersion() {
        final ModelPackageConfig packageConfig = new ModelPackageConfig.Builder(ModelPackageConfigTests.randomModulePackageConfig())
            .setMinimumVersion("99.9.9")
            .build();

        ClusterState state = mock(ClusterState.class);
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        when(state.nodes()).thenReturn(nodes);
        when(nodes.getMinNodeVersion()).thenReturn(Version.CURRENT);

        Exception e = expectThrows(
            ActionRequestValidationException.class,
            () -> TrainedModelValidator.validateMinimumVersion(packageConfig, state)
        );

        assertEquals(
            "Validation Failed: 1: The model ["
                + packageConfig.getPackagedModelId()
                + "] requires that all nodes are at least version [99.9.9];",
            e.getMessage()
        );

        final ModelPackageConfig packageConfigCurrent = new ModelPackageConfig.Builder(ModelPackageConfigTests.randomModulePackageConfig())
            .setMinimumVersion(Version.CURRENT.toString())
            .build();
        TrainedModelValidator.validateMinimumVersion(packageConfigCurrent, state);

        when(nodes.getMinNodeVersion()).thenReturn(Version.V_8_7_0);

        e = expectThrows(
            ActionRequestValidationException.class,
            () -> TrainedModelValidator.validateMinimumVersion(packageConfigCurrent, state)
        );

        assertEquals(
            "Validation Failed: 1: The model ["
                + packageConfigCurrent.getPackagedModelId()
                + "] requires that all nodes are at least version ["
                + Version.CURRENT
                + "];",
            e.getMessage()
        );

        final ModelPackageConfig packageConfigBroken = new ModelPackageConfig.Builder(ModelPackageConfigTests.randomModulePackageConfig())
            .setMinimumVersion("_broken_version_")
            .build();

        e = expectThrows(
            ActionRequestValidationException.class,
            () -> TrainedModelValidator.validateMinimumVersion(packageConfigBroken, state)
        );

        assertEquals(
            "Validation Failed: 1: Invalid model package configuration for ["
                + packageConfigBroken.getPackagedModelId()
                + "], failed to parse the minimum_version property;",
            e.getMessage()
        );

        final ModelPackageConfig packageConfigVersionMissing = new ModelPackageConfig.Builder(
            ModelPackageConfigTests.randomModulePackageConfig()
        ).setMinimumVersion("").build();

        e = expectThrows(
            ActionRequestValidationException.class,
            () -> TrainedModelValidator.validateMinimumVersion(packageConfigVersionMissing, state)
        );

        assertEquals(
            "Validation Failed: 1: Invalid model package configuration for ["
                + packageConfigVersionMissing.getPackagedModelId()
                + "], missing minimum_version property;",
            e.getMessage()
        );
    }
}
