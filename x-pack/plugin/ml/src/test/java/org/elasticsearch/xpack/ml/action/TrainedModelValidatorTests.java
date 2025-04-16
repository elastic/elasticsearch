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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.MlConfigVersion;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ModelPackageConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ModelPackageConfigTests;

import java.net.InetAddress;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TrainedModelValidatorTests extends ESTestCase {

    public void testValidateMinimumVersion() {
        {
            final ModelPackageConfig packageConfig = new ModelPackageConfig.Builder(ModelPackageConfigTests.randomModulePackageConfig())
                .setMinimumVersion("9999.0.0")
                .build();

            final Map<String, String> attributes = Map.of(MlConfigVersion.ML_CONFIG_VERSION_NODE_ATTR, MlConfigVersion.CURRENT.toString());
            DiscoveryNode node = DiscoveryNodeUtils.create(
                "node1name",
                "node1",
                new TransportAddress(InetAddress.getLoopbackAddress(), 9301),
                attributes,
                DiscoveryNodeRole.roles()
            );

            DiscoveryNodes nodes = DiscoveryNodes.builder().add(node).build();

            ClusterState state = mock(ClusterState.class);

            when(state.nodes()).thenReturn(nodes);

            Exception e = expectThrows(
                ActionRequestValidationException.class,
                () -> TrainedModelValidator.validateMinimumVersion(packageConfig, state)
            );

            assertEquals(
                "Validation Failed: 1: The model ["
                    + packageConfig.getPackagedModelId()
                    + "] requires that all nodes have ML config version [9999.0.0] or higher;",
                e.getMessage()
            );
        }
        {
            ClusterState state = mock(ClusterState.class);

            final ModelPackageConfig packageConfigBroken = new ModelPackageConfig.Builder(
                ModelPackageConfigTests.randomModulePackageConfig()
            ).setMinimumVersion("_broken_version_").build();

            DiscoveryNode node = DiscoveryNodeUtils.create(
                "node1",
                new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                Version.V_8_7_0
            );

            DiscoveryNodes nodes = DiscoveryNodes.builder().add(node).build();

            when(state.nodes()).thenReturn(nodes);

            Exception e = expectThrows(
                ActionRequestValidationException.class,
                () -> TrainedModelValidator.validateMinimumVersion(packageConfigBroken, state)
            );

            assertEquals(
                "Validation Failed: 1: Invalid model package configuration for ["
                    + packageConfigBroken.getPackagedModelId()
                    + "], failed to parse the minimum_version property;",
                e.getMessage()
            );
        }
        {
            ClusterState state = mock(ClusterState.class);

            final ModelPackageConfig packageConfigVersionMissing = new ModelPackageConfig.Builder(
                ModelPackageConfigTests.randomModulePackageConfig()
            ).setMinimumVersion("").build();

            Exception e = expectThrows(
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
}
