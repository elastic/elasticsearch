/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.utils;

import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class MlProcessorsTests extends ESTestCase {

    public void testGet() {
        var node = DiscoveryNodeUtils.builder("foo").attributes(Map.of(MachineLearning.ALLOCATED_PROCESSORS_NODE_ATTR, "8.0")).build();
        var processor = MlProcessors.get(node, 1);
        assertThat(processor.count(), equalTo(8.0));
    }

    public void testGetWithScale() {
        var node = DiscoveryNodeUtils.builder("foo").attributes(Map.of(MachineLearning.ALLOCATED_PROCESSORS_NODE_ATTR, "8.0")).build();
        var processor = MlProcessors.get(node, 2);
        assertThat(processor.count(), equalTo(4.0));
    }

    public void testGetWithNull() {
        var node = DiscoveryNodeUtils.builder("foo").attributes(Map.of(MachineLearning.ALLOCATED_PROCESSORS_NODE_ATTR, "8.0")).build();
        var processor = MlProcessors.get(node, null);
        assertThat(processor.count(), equalTo(8.0));
    }

    public void testGetMaxMlNodeProcessors() {
        var nodes = DiscoveryNodes.builder()
            .add(
                DiscoveryNodeUtils.builder("n1")
                    .roles(Set.of(DiscoveryNodeRole.ML_ROLE))
                    .attributes(Map.of(MachineLearning.ALLOCATED_PROCESSORS_NODE_ATTR, "8.0"))
                    .build()
            )
            .add(
                DiscoveryNodeUtils.builder("n2")
                    .roles(Set.of(DiscoveryNodeRole.DATA_ROLE))
                    .attributes(Map.of(MachineLearning.ALLOCATED_PROCESSORS_NODE_ATTR, "9.0"))
                    .build()
            )
            .add(
                DiscoveryNodeUtils.builder("n3")
                    .roles(Set.of(DiscoveryNodeRole.ML_ROLE))
                    .attributes(Map.of(MachineLearning.ALLOCATED_PROCESSORS_NODE_ATTR, "7.0"))
                    .build()
            )
            .add(
                DiscoveryNodeUtils.builder("n4")
                    .roles(Set.of(DiscoveryNodeRole.MASTER_ROLE))
                    .attributes(Map.of(MachineLearning.ALLOCATED_PROCESSORS_NODE_ATTR, "6.0"))
                    .build()
            )
            .build();
        var processor = MlProcessors.getMaxMlNodeProcessors(nodes, 1);
        assertThat(processor.count(), equalTo(8.0));
    }

    public void testGetMaxMlNodeProcessorsWithScale() {
        var nodes = DiscoveryNodes.builder()
            .add(
                DiscoveryNodeUtils.builder("n1")
                    .roles(Set.of(DiscoveryNodeRole.ML_ROLE))
                    .attributes(Map.of(MachineLearning.ALLOCATED_PROCESSORS_NODE_ATTR, "8.0"))
                    .build()
            )
            .add(
                DiscoveryNodeUtils.builder("n2")
                    .roles(Set.of(DiscoveryNodeRole.DATA_ROLE))
                    .attributes(Map.of(MachineLearning.ALLOCATED_PROCESSORS_NODE_ATTR, "9.0"))
                    .build()
            )
            .add(
                DiscoveryNodeUtils.builder("n3")
                    .roles(Set.of(DiscoveryNodeRole.ML_ROLE))
                    .attributes(Map.of(MachineLearning.ALLOCATED_PROCESSORS_NODE_ATTR, "12.0"))
                    .build()
            )
            .add(
                DiscoveryNodeUtils.builder("n4")
                    .roles(Set.of(DiscoveryNodeRole.MASTER_ROLE))
                    .attributes(Map.of(MachineLearning.ALLOCATED_PROCESSORS_NODE_ATTR, "10.0"))
                    .build()
            )
            .build();
        var processor = MlProcessors.getMaxMlNodeProcessors(nodes, 2);
        assertThat(processor.count(), equalTo(6.0));
    }

    public void testGetMaxMlNodeProcessorsWithNull() {
        var nodes = DiscoveryNodes.builder()
            .add(
                DiscoveryNodeUtils.builder("n1")
                    .roles(Set.of(DiscoveryNodeRole.ML_ROLE))
                    .attributes(Map.of(MachineLearning.ALLOCATED_PROCESSORS_NODE_ATTR, "6.0"))
                    .build()
            )
            .add(
                DiscoveryNodeUtils.builder("n2")
                    .roles(Set.of(DiscoveryNodeRole.DATA_ROLE))
                    .attributes(Map.of(MachineLearning.ALLOCATED_PROCESSORS_NODE_ATTR, "9.0"))
                    .build()
            )
            .add(
                DiscoveryNodeUtils.builder("n3")
                    .roles(Set.of(DiscoveryNodeRole.ML_ROLE))
                    .attributes(Map.of(MachineLearning.ALLOCATED_PROCESSORS_NODE_ATTR, "7.0"))
                    .build()
            )
            .add(
                DiscoveryNodeUtils.builder("n4")
                    .roles(Set.of(DiscoveryNodeRole.MASTER_ROLE))
                    .attributes(Map.of(MachineLearning.ALLOCATED_PROCESSORS_NODE_ATTR, "6.0"))
                    .build()
            )
            .build();
        var processor = MlProcessors.getMaxMlNodeProcessors(nodes, null);
        assertThat(processor.count(), equalTo(7.0));
    }

    public void testGetTotalMlNodeProcessors() {
        var nodes = DiscoveryNodes.builder()
            .add(
                DiscoveryNodeUtils.builder("n1")
                    .roles(Set.of(DiscoveryNodeRole.ML_ROLE))
                    .attributes(Map.of(MachineLearning.ALLOCATED_PROCESSORS_NODE_ATTR, "8.0"))
                    .build()
            )
            .add(
                DiscoveryNodeUtils.builder("n2")
                    .roles(Set.of(DiscoveryNodeRole.DATA_ROLE))
                    .attributes(Map.of(MachineLearning.ALLOCATED_PROCESSORS_NODE_ATTR, "9.0"))
                    .build()
            )
            .add(
                DiscoveryNodeUtils.builder("n3")
                    .roles(Set.of(DiscoveryNodeRole.ML_ROLE))
                    .attributes(Map.of(MachineLearning.ALLOCATED_PROCESSORS_NODE_ATTR, "7.0"))
                    .build()
            )
            .add(
                DiscoveryNodeUtils.builder("n4")
                    .roles(Set.of(DiscoveryNodeRole.MASTER_ROLE))
                    .attributes(Map.of(MachineLearning.ALLOCATED_PROCESSORS_NODE_ATTR, "6.0"))
                    .build()
            )
            .build();
        var processor = MlProcessors.getTotalMlNodeProcessors(nodes, 1);
        assertThat(processor.count(), equalTo(15.0));
    }

    public void testGetTotalMlNodeProcessorsWithZeroProcessors() {
        var nodes = DiscoveryNodes.EMPTY_NODES;
        var processor = MlProcessors.getTotalMlNodeProcessors(nodes, 1);
        assertThat(processor.count(), equalTo(0.0));
    }

    public void testGetTotalMlNodeProcessorsWithScale() {
        var nodes = DiscoveryNodes.builder()
            .add(
                DiscoveryNodeUtils.builder("n1")
                    .roles(Set.of(DiscoveryNodeRole.ML_ROLE))
                    .attributes(Map.of(MachineLearning.ALLOCATED_PROCESSORS_NODE_ATTR, "8.0"))
                    .build()
            )
            .add(
                DiscoveryNodeUtils.builder("n2")
                    .roles(Set.of(DiscoveryNodeRole.DATA_ROLE))
                    .attributes(Map.of(MachineLearning.ALLOCATED_PROCESSORS_NODE_ATTR, "9.0"))
                    .build()
            )
            .add(
                DiscoveryNodeUtils.builder("n3")
                    .roles(Set.of(DiscoveryNodeRole.ML_ROLE))
                    .attributes(Map.of(MachineLearning.ALLOCATED_PROCESSORS_NODE_ATTR, "7.0"))
                    .build()
            )
            .add(
                DiscoveryNodeUtils.builder("n4")
                    .roles(Set.of(DiscoveryNodeRole.MASTER_ROLE))
                    .attributes(Map.of(MachineLearning.ALLOCATED_PROCESSORS_NODE_ATTR, "6.0"))
                    .build()
            )
            .build();
        var processor = MlProcessors.getTotalMlNodeProcessors(nodes, 2);
        assertThat(processor.count(), equalTo(8.0));
    }

    public void testGetTotalMlNodeProcessorsWithNull() {
        var nodes = DiscoveryNodes.builder()
            .add(
                DiscoveryNodeUtils.builder("n1")
                    .roles(Set.of(DiscoveryNodeRole.ML_ROLE))
                    .attributes(Map.of(MachineLearning.ALLOCATED_PROCESSORS_NODE_ATTR, "6.5"))
                    .build()
            )
            .add(
                DiscoveryNodeUtils.builder("n2")
                    .roles(Set.of(DiscoveryNodeRole.DATA_ROLE))
                    .attributes(Map.of(MachineLearning.ALLOCATED_PROCESSORS_NODE_ATTR, "9.0"))
                    .build()
            )
            .add(
                DiscoveryNodeUtils.builder("n3")
                    .roles(Set.of(DiscoveryNodeRole.ML_ROLE))
                    .attributes(Map.of(MachineLearning.ALLOCATED_PROCESSORS_NODE_ATTR, "7.0"))
                    .build()
            )
            .add(
                DiscoveryNodeUtils.builder("n4")
                    .roles(Set.of(DiscoveryNodeRole.MASTER_ROLE))
                    .attributes(Map.of(MachineLearning.ALLOCATED_PROCESSORS_NODE_ATTR, "6.0"))
                    .build()
            )
            .build();
        var processor = MlProcessors.getTotalMlNodeProcessors(nodes, null);
        assertThat(processor.count(), equalTo(14.0));
    }
}
