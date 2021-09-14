/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.net.InetAddress;
import java.util.Collections;

import static org.elasticsearch.xpack.ml.MachineLearning.MAX_LAZY_ML_NODES;
import static org.elasticsearch.xpack.ml.MachineLearning.MAX_MACHINE_MEMORY_PERCENT;
import static org.elasticsearch.xpack.ml.MachineLearning.MAX_ML_NODE_SIZE;
import static org.elasticsearch.xpack.ml.MachineLearning.USE_AUTO_MACHINE_MEMORY_PERCENT;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class TransportMlInfoActionTests extends ESTestCase {

    public void testCalculateEffectiveMaxModelMemoryLimitWithoutMaxMlNodeSize() {

        int mlMemoryPercent = randomIntBetween(5, 90);
        long mlMachineMemory = randomLongBetween(2000000000L, 100000000000L);
        int numMlNodes = randomIntBetween(0, 10);
        int numNonMlNodes = randomIntBetween(0, 10);
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.builder().put(MAX_MACHINE_MEMORY_PERCENT.getKey(), mlMemoryPercent).build(),
            Sets.newHashSet(MAX_LAZY_ML_NODES, MAX_MACHINE_MEMORY_PERCENT, MAX_ML_NODE_SIZE, USE_AUTO_MACHINE_MEMORY_PERCENT));
        long totalMlMemoryBytes = numMlNodes * mlMachineMemory * mlMemoryPercent / 100;

        DiscoveryNodes nodes = randomNodes(numMlNodes, numNonMlNodes, mlMachineMemory);

        ByteSizeValue effectiveMaxModelMemoryLimit = TransportMlInfoAction.calculateEffectiveMaxModelMemoryLimit(clusterSettings, nodes);

        if (numMlNodes == 0) {
            // "Don't know"
            assertThat(effectiveMaxModelMemoryLimit, nullValue());
        } else {
            // Expect configured percentage of current node size (allowing for small rounding errors)
            assertThat(effectiveMaxModelMemoryLimit, notNullValue());
            assertThat(effectiveMaxModelMemoryLimit.getBytes()
                    + Math.max(Job.PROCESS_MEMORY_OVERHEAD.getBytes(), DataFrameAnalyticsConfig.PROCESS_MEMORY_OVERHEAD.getBytes())
                    + MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(),
                lessThanOrEqualTo(mlMachineMemory * mlMemoryPercent / 100));
        }

        ByteSizeValue totalMlMemory = TransportMlInfoAction.calculateTotalMlMemory(clusterSettings, nodes);

        assertThat(totalMlMemory, notNullValue());
        assertThat(totalMlMemory, is(ByteSizeValue.ofMb(totalMlMemoryBytes / (1024 * 1024))));
    }

    public void testCalculateEffectiveMaxModelMemoryLimitNoMlNodesButMaxMlNodeSizeAndLazyNodesAllowed() {

        int mlMemoryPercent = randomIntBetween(5, 90);
        long mlMaxNodeSize = randomLongBetween(2000000000L, 100000000000L);
        int numNonMlNodes = randomIntBetween(0, 10);
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.builder().put(MAX_ML_NODE_SIZE.getKey(), mlMaxNodeSize + "b")
                .put(MAX_LAZY_ML_NODES.getKey(), randomIntBetween(1, 100))
                .put(MAX_MACHINE_MEMORY_PERCENT.getKey(), mlMemoryPercent).build(),
            Sets.newHashSet(MAX_LAZY_ML_NODES, MAX_MACHINE_MEMORY_PERCENT, MAX_ML_NODE_SIZE, USE_AUTO_MACHINE_MEMORY_PERCENT));

        DiscoveryNodes nodes = randomNodes(0, numNonMlNodes, 0);

        ByteSizeValue effectiveMaxModelMemoryLimit = TransportMlInfoAction.calculateEffectiveMaxModelMemoryLimit(clusterSettings, nodes);

        // Expect configured percentage of maximum declared node size (allowing for small rounding errors)
        assertThat(effectiveMaxModelMemoryLimit, notNullValue());
        assertThat(effectiveMaxModelMemoryLimit.getBytes()
                + Math.max(Job.PROCESS_MEMORY_OVERHEAD.getBytes(), DataFrameAnalyticsConfig.PROCESS_MEMORY_OVERHEAD.getBytes())
                + MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(),
            lessThanOrEqualTo(mlMaxNodeSize * mlMemoryPercent / 100));

        ByteSizeValue totalMlMemory = TransportMlInfoAction.calculateTotalMlMemory(clusterSettings, nodes);

        assertThat(totalMlMemory, notNullValue());
        assertThat(totalMlMemory, is(ByteSizeValue.ofMb(0)));
    }

    public void testCalculateEffectiveMaxModelMemoryLimitSmallMlNodesButMaxMlNodeSizeBiggerAndLazyNodesAllowed() {

        int mlMemoryPercent = randomIntBetween(5, 90);
        long mlMaxNodeSize = randomLongBetween(2000000000L, 100000000000L);
        long mlMachineMemory = mlMaxNodeSize / randomLongBetween(3, 5);
        int numMlNodes = randomIntBetween(1, 10);
        int numNonMlNodes = randomIntBetween(0, 10);
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.builder().put(MAX_ML_NODE_SIZE.getKey(), mlMaxNodeSize + "b")
                .put(MAX_LAZY_ML_NODES.getKey(), randomIntBetween(numMlNodes + 1, 100))
                .put(MAX_MACHINE_MEMORY_PERCENT.getKey(), mlMemoryPercent).build(),
            Sets.newHashSet(MAX_LAZY_ML_NODES, MAX_MACHINE_MEMORY_PERCENT, MAX_ML_NODE_SIZE, USE_AUTO_MACHINE_MEMORY_PERCENT));
        long totalMlMemoryBytes = numMlNodes * mlMachineMemory * mlMemoryPercent / 100;

        DiscoveryNodes nodes = randomNodes(numMlNodes, numNonMlNodes, mlMachineMemory);

        ByteSizeValue effectiveMaxModelMemoryLimit = TransportMlInfoAction.calculateEffectiveMaxModelMemoryLimit(clusterSettings, nodes);

        // Expect configured percentage of maximum declared node size (allowing for small rounding errors) - bigger than current node size
        assertThat(effectiveMaxModelMemoryLimit, notNullValue());
        assertThat(effectiveMaxModelMemoryLimit.getBytes()
                + Math.max(Job.PROCESS_MEMORY_OVERHEAD.getBytes(), DataFrameAnalyticsConfig.PROCESS_MEMORY_OVERHEAD.getBytes())
                + MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(),
            lessThanOrEqualTo(mlMaxNodeSize * mlMemoryPercent / 100));
        assertThat(effectiveMaxModelMemoryLimit.getBytes()
                + Math.max(Job.PROCESS_MEMORY_OVERHEAD.getBytes(), DataFrameAnalyticsConfig.PROCESS_MEMORY_OVERHEAD.getBytes())
                + MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(),
            greaterThan(2 * mlMachineMemory * mlMemoryPercent / 100));

        ByteSizeValue totalMlMemory = TransportMlInfoAction.calculateTotalMlMemory(clusterSettings, nodes);

        assertThat(totalMlMemory, notNullValue());
        assertThat(totalMlMemory, is(ByteSizeValue.ofMb(totalMlMemoryBytes / (1024 * 1024))));
    }

    public void testCalculateEffectiveMaxModelMemoryLimitSmallMlNodesButMaxMlNodeSizeBiggerAndLazyNodesExhausted() {

        int mlMemoryPercent = randomIntBetween(10, 90);
        long mlMaxNodeSize = randomLongBetween(2000000000L, 100000000000L);
        long mlMachineMemory = mlMaxNodeSize / randomLongBetween(3, 4);
        int numMlNodes = randomIntBetween(2, 10);
        int numNonMlNodes = randomIntBetween(0, 10);
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.builder().put(MAX_ML_NODE_SIZE.getKey(), mlMaxNodeSize + "b")
                .put(MAX_LAZY_ML_NODES.getKey(), randomIntBetween(1, numMlNodes - 1))
                .put(MAX_MACHINE_MEMORY_PERCENT.getKey(), mlMemoryPercent).build(),
            Sets.newHashSet(MAX_LAZY_ML_NODES, MAX_MACHINE_MEMORY_PERCENT, MAX_ML_NODE_SIZE, USE_AUTO_MACHINE_MEMORY_PERCENT));
        long totalMlMemoryBytes = numMlNodes * mlMachineMemory * mlMemoryPercent / 100;

        DiscoveryNodes nodes = randomNodes(numMlNodes, numNonMlNodes, mlMachineMemory);

        ByteSizeValue effectiveMaxModelMemoryLimit = TransportMlInfoAction.calculateEffectiveMaxModelMemoryLimit(clusterSettings, nodes);

        // Expect configured percentage of current node size (allowing for small rounding errors) - max is bigger but can't be added
        assertThat(effectiveMaxModelMemoryLimit, notNullValue());
        assertThat(effectiveMaxModelMemoryLimit.getBytes()
                + Math.max(Job.PROCESS_MEMORY_OVERHEAD.getBytes(), DataFrameAnalyticsConfig.PROCESS_MEMORY_OVERHEAD.getBytes())
                + MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(),
            lessThanOrEqualTo(mlMachineMemory * mlMemoryPercent / 100));

        ByteSizeValue totalMlMemory = TransportMlInfoAction.calculateTotalMlMemory(clusterSettings, nodes);

        assertThat(totalMlMemory, notNullValue());
        assertThat(totalMlMemory, is(ByteSizeValue.ofMb(totalMlMemoryBytes / (1024 * 1024))));
    }

    DiscoveryNodes randomNodes(int numMlNodes, int numNonMlNodes, long mlMachineMemory) {

        DiscoveryNodes.Builder builder = DiscoveryNodes.builder();

        for (int i = 0; i < numMlNodes + numNonMlNodes; ++i) {
            String nodeName = "_node_name" + i;
            String nodeId = "_node_id" + i;
            TransportAddress ta = new TransportAddress(InetAddress.getLoopbackAddress(), 9300 + i);
            if (i < numMlNodes) {
                // ML node
                builder.add(new DiscoveryNode(nodeName, nodeId, ta,
                    Collections.singletonMap(MachineLearning.MACHINE_MEMORY_NODE_ATTR, String.valueOf(mlMachineMemory)),
                    Collections.emptySet(), Version.CURRENT));
            } else {
                // Not an ML node
                builder.add(new DiscoveryNode(nodeName, nodeId, ta, Collections.emptyMap(), Collections.emptySet(), Version.CURRENT));
            }
        }

        return builder.build();
    }
}
