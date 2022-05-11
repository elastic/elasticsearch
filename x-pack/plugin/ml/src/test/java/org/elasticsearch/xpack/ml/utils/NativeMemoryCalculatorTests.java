/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.utils;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingCapacity;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.autoscaling.NativeMemoryCapacity;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.BiConsumer;

import static org.elasticsearch.xpack.core.ml.MachineLearningField.MAX_MODEL_MEMORY_LIMIT;
import static org.elasticsearch.xpack.ml.MachineLearning.MACHINE_MEMORY_NODE_ATTR;
import static org.elasticsearch.xpack.ml.MachineLearning.MAX_JVM_SIZE_NODE_ATTR;
import static org.elasticsearch.xpack.ml.MachineLearning.MAX_LAZY_ML_NODES;
import static org.elasticsearch.xpack.ml.MachineLearning.MAX_MACHINE_MEMORY_PERCENT;
import static org.elasticsearch.xpack.ml.MachineLearning.MAX_ML_NODE_SIZE;
import static org.elasticsearch.xpack.ml.MachineLearning.USE_AUTO_MACHINE_MEMORY_PERCENT;
import static org.elasticsearch.xpack.ml.autoscaling.MlAutoscalingDeciderServiceTests.AUTO_NODE_TIERS;
import static org.elasticsearch.xpack.ml.utils.NativeMemoryCalculator.MINIMUM_AUTOMATIC_NODE_SIZE;
import static org.elasticsearch.xpack.ml.utils.NativeMemoryCalculator.dynamicallyCalculateJvmSizeFromNodeSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NativeMemoryCalculatorTests extends ESTestCase {

    private static final int NUM_TEST_RUNS = 10;
    private static final Set<Setting<?>> ML_MEMORY_RELATED_SETTINGS = Set.of(
        MAX_LAZY_ML_NODES,
        MAX_MACHINE_MEMORY_PERCENT,
        MAX_ML_NODE_SIZE,
        MAX_MODEL_MEMORY_LIMIT,
        USE_AUTO_MACHINE_MEMORY_PERCENT
    );

    public void testAllowedBytesForMLWhenAutoIsFalse() {
        for (int i = 0; i < NUM_TEST_RUNS; i++) {
            long nodeSize = randomLongBetween(ByteSizeValue.ofMb(500).getBytes(), ByteSizeValue.ofGb(64).getBytes());
            int percent = randomIntBetween(5, 200);
            DiscoveryNode node = newNode(randomBoolean() ? null : randomNonNegativeLong(), nodeSize);
            Settings settings = newSettings(percent, false, ByteSizeValue.ofMb(randomIntBetween(0, 5000)));
            ClusterSettings clusterSettings = newClusterSettings(percent, false, ByteSizeValue.ofMb(randomIntBetween(0, 5000)));

            long expected = (long) (nodeSize * (percent / 100.0));

            assertThat(NativeMemoryCalculator.allowedBytesForMl(node, settings).getAsLong(), equalTo(expected));
            assertThat(NativeMemoryCalculator.allowedBytesForMl(node, clusterSettings).getAsLong(), equalTo(expected));
            assertThat(NativeMemoryCalculator.allowedBytesForMl(node, percent, false).getAsLong(), equalTo(expected));
        }
    }

    public void testConsistencyInAutoCalculation() {
        for (Tuple<Long, Long> nodeAndJvmSize : AUTO_NODE_TIERS) {
            final long trueJvmSize = nodeAndJvmSize.v2();
            final long trueNodeSize = nodeAndJvmSize.v1();
            List<Long> nodeSizes = Arrays.asList(
                trueNodeSize + ByteSizeValue.ofMb(10).getBytes(),
                trueNodeSize - ByteSizeValue.ofMb(10).getBytes(),
                trueNodeSize
            );
            for (long nodeSize : nodeSizes) {
                // Simulate having a true size that already exists from the node vs. us dynamically calculating it
                long jvmSize = randomBoolean() ? dynamicallyCalculateJvmSizeFromNodeSize(nodeSize) : trueJvmSize;
                DiscoveryNode node = newNode(jvmSize, nodeSize);
                Settings settings = newSettings(30, true, ByteSizeValue.ZERO);
                ClusterSettings clusterSettings = newClusterSettings(30, true, ByteSizeValue.ZERO);

                long bytesForML = randomBoolean()
                    ? NativeMemoryCalculator.allowedBytesForMl(node, settings).getAsLong()
                    : NativeMemoryCalculator.allowedBytesForMl(node, clusterSettings).getAsLong();

                NativeMemoryCapacity nativeMemoryCapacity = new NativeMemoryCapacity(bytesForML, bytesForML, jvmSize);

                AutoscalingCapacity capacity = nativeMemoryCapacity.autoscalingCapacity(30, true);
                // We don't allow node sizes below 1GB, so we will always be at least that large
                // Also, allow 1 byte off for weird rounding issues
                assertThat(
                    capacity.node().memory().getBytes(),
                    greaterThanOrEqualTo(Math.max(nodeSize, ByteSizeValue.ofGb(1).getBytes()) - 1L)
                );
                assertThat(
                    capacity.total().memory().getBytes(),
                    greaterThanOrEqualTo(Math.max(nodeSize, ByteSizeValue.ofGb(1).getBytes()) - 1L)
                );
            }
        }
    }

    public void testAllowedBytesForMlWhenAutoIsTrue() {
        for (int i = 0; i < NUM_TEST_RUNS; i++) {
            long nodeSize = randomLongBetween(ByteSizeValue.ofMb(500).getBytes(), ByteSizeValue.ofGb(64).getBytes());
            long jvmSize = randomLongBetween(ByteSizeValue.ofMb(250).getBytes(), nodeSize - ByteSizeValue.ofMb(200).getBytes());
            int percent = randomIntBetween(5, 200);
            DiscoveryNode node = newNode(jvmSize, nodeSize);
            Settings settings = newSettings(percent, true, ByteSizeValue.ofMb(randomIntBetween(0, 5000)));
            ClusterSettings clusterSettings = newClusterSettings(percent, true, ByteSizeValue.ofMb(randomIntBetween(0, 5000)));

            double truePercent = Math.min(90, ((nodeSize - jvmSize - ByteSizeValue.ofMb(200).getBytes()) / (double) nodeSize) * 100.0D);
            long expected = Math.round(nodeSize * (truePercent / 100.0));

            assertThat(NativeMemoryCalculator.allowedBytesForMl(node, settings).getAsLong(), equalTo(expected));
            assertThat(NativeMemoryCalculator.allowedBytesForMl(node, clusterSettings).getAsLong(), equalTo(expected));
            assertThat(NativeMemoryCalculator.allowedBytesForMl(node, percent, true).getAsLong(), equalTo(expected));
        }
    }

    public void testAllowedBytesForMlWhenBothJVMAndNodeSizeAreUnknown() {
        int percent = randomIntBetween(5, 200);
        DiscoveryNode node = newNode(null, null);
        Settings settings = newSettings(percent, randomBoolean(), ByteSizeValue.ofMb(randomIntBetween(0, 5000)));
        ClusterSettings clusterSettings = newClusterSettings(percent, randomBoolean(), ByteSizeValue.ofMb(randomIntBetween(0, 5000)));

        assertThat(NativeMemoryCalculator.allowedBytesForMl(node, settings), equalTo(OptionalLong.empty()));
        assertThat(NativeMemoryCalculator.allowedBytesForMl(node, clusterSettings), equalTo(OptionalLong.empty()));
        assertThat(NativeMemoryCalculator.allowedBytesForMl(node, percent, randomBoolean()), equalTo(OptionalLong.empty()));
    }

    public void testTinyNode() {
        for (int i = 0; i < NUM_TEST_RUNS; i++) {
            long nodeSize = randomLongBetween(0, ByteSizeValue.ofMb(200).getBytes());
            long jvmSize = randomLongBetween(0, ByteSizeValue.ofMb(200).getBytes());
            int percent = randomIntBetween(5, 200);
            DiscoveryNode node = newNode(jvmSize, nodeSize);
            Settings settings = newSettings(percent, true, ByteSizeValue.ofMb(randomIntBetween(0, 5000)));
            ClusterSettings clusterSettings = newClusterSettings(percent, true, ByteSizeValue.ofMb(randomIntBetween(0, 5000)));
            long expected = nodeSize / 100;
            assertThat(NativeMemoryCalculator.allowedBytesForMl(node, settings).getAsLong(), equalTo(expected));
            assertThat(NativeMemoryCalculator.allowedBytesForMl(node, clusterSettings).getAsLong(), equalTo(expected));
            assertThat(NativeMemoryCalculator.allowedBytesForMl(node, percent, true).getAsLong(), equalTo(expected));
        }
    }

    public void testActualNodeSizeCalculationConsistency() {
        final TriConsumer<Long, Integer, Long> consistentAutoAssertions = (nativeMemory, memoryPercentage, delta) -> {
            long autoNodeSize = NativeMemoryCalculator.calculateApproxNecessaryNodeSize(nativeMemory, null, memoryPercentage, true);
            // It should always be greater than the minimum supported node size
            assertThat(
                "node size [" + autoNodeSize + "] smaller than minimum required size [" + MINIMUM_AUTOMATIC_NODE_SIZE + "]",
                autoNodeSize,
                greaterThanOrEqualTo(MINIMUM_AUTOMATIC_NODE_SIZE)
            );
            // Our approximate real node size should always return a usable native memory size that is at least the original native memory
            // size. Rounding errors may cause it to be non-exact.
            long allowedBytesForMl = NativeMemoryCalculator.allowedBytesForMl(autoNodeSize, memoryPercentage, true);
            assertThat(
                "native memory [" + allowedBytesForMl + "] smaller than original native memory [" + nativeMemory + "]",
                allowedBytesForMl,
                greaterThanOrEqualTo(nativeMemory - delta)
            );
        };

        final BiConsumer<Long, Integer> consistentManualAssertions = (nativeMemory, memoryPercentage) -> {
            assertThat(
                NativeMemoryCalculator.calculateApproxNecessaryNodeSize(nativeMemory, null, memoryPercentage, false),
                equalTo((long) ((100.0 / memoryPercentage) * nativeMemory))
            );
            assertThat(
                NativeMemoryCalculator.calculateApproxNecessaryNodeSize(nativeMemory, randomNonNegativeLong(), memoryPercentage, false),
                equalTo((long) ((100.0 / memoryPercentage) * nativeMemory))
            );
        };

        { // 0 memory
            assertThat(
                NativeMemoryCalculator.calculateApproxNecessaryNodeSize(
                    0L,
                    randomLongBetween(0L, ByteSizeValue.ofGb(100).getBytes()),
                    randomIntBetween(0, 100),
                    randomBoolean()
                ),
                equalTo(0L)
            );
            assertThat(
                NativeMemoryCalculator.calculateApproxNecessaryNodeSize(0L, null, randomIntBetween(0, 100), randomBoolean()),
                equalTo(0L)
            );
        }
        for (int i = 0; i < NUM_TEST_RUNS; i++) {
            int memoryPercentage = randomIntBetween(5, 200);
            { // tiny memory
                long nodeMemory = randomLongBetween(ByteSizeValue.ofKb(100).getBytes(), ByteSizeValue.ofMb(500).getBytes());
                consistentAutoAssertions.apply(nodeMemory, memoryPercentage, 1L);
                consistentManualAssertions.accept(nodeMemory, memoryPercentage);
            }
            { // normal-ish memory
                long nodeMemory = randomLongBetween(ByteSizeValue.ofMb(500).getBytes(), ByteSizeValue.ofGb(4).getBytes());
                // periodically, the calculated assertions end up being about 6% off, allowing this small delta to account for flakiness
                consistentAutoAssertions.apply(nodeMemory, memoryPercentage, 1L);
                consistentManualAssertions.accept(nodeMemory, memoryPercentage);
            }
            { // huge memory
                long nodeMemory = randomLongBetween(ByteSizeValue.ofGb(30).getBytes(), ByteSizeValue.ofGb(60).getBytes());
                consistentAutoAssertions.apply(nodeMemory, memoryPercentage, 1L);
                consistentManualAssertions.accept(nodeMemory, memoryPercentage);
            }
        }
    }

    public void testCalculateMaxModelMemoryLimitToFitWithoutMaxMlNodeSize() {

        int mlMemoryPercent = randomIntBetween(5, 90);
        long mlMachineMemory = randomLongBetween(2000000000L, 100000000000L);
        int numMlNodes = randomIntBetween(0, 10);
        int numNonMlNodes = randomIntBetween(0, 10);
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.builder().put(MAX_MACHINE_MEMORY_PERCENT.getKey(), mlMemoryPercent).build(),
            ML_MEMORY_RELATED_SETTINGS
        );
        long totalMlMemoryBytes = numMlNodes * mlMachineMemory * mlMemoryPercent / 100;

        DiscoveryNodes nodes = randomNodes(numMlNodes, numNonMlNodes, mlMachineMemory);

        ByteSizeValue maxModelMemoryLimitToFit = NativeMemoryCalculator.calculateMaxModelMemoryLimitToFit(clusterSettings, nodes);

        if (numMlNodes == 0) {
            // "Don't know"
            assertThat(maxModelMemoryLimitToFit, nullValue());
        } else {
            // Expect configured percentage of current node size (allowing for small rounding errors)
            assertThat(maxModelMemoryLimitToFit, notNullValue());
            assertThat(
                maxModelMemoryLimitToFit.getBytes() + Math.max(
                    Job.PROCESS_MEMORY_OVERHEAD.getBytes(),
                    DataFrameAnalyticsConfig.PROCESS_MEMORY_OVERHEAD.getBytes()
                ) + MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(),
                lessThanOrEqualTo(mlMachineMemory * mlMemoryPercent / 100)
            );
        }

        ByteSizeValue totalMlMemory = NativeMemoryCalculator.calculateTotalMlMemory(clusterSettings, nodes);

        assertThat(totalMlMemory, notNullValue());
        assertThat(totalMlMemory, is(ByteSizeValue.ofMb(totalMlMemoryBytes / (1024 * 1024))));
    }

    public void testCalculateMaxModelMemoryLimitToFitNoMlNodesButMaxMlNodeSizeAndLazyNodesAllowed() {

        int mlMemoryPercent = randomIntBetween(5, 90);
        long mlMaxNodeSize = randomLongBetween(2000000000L, 100000000000L);
        int numNonMlNodes = randomIntBetween(0, 10);
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.builder()
                .put(MAX_ML_NODE_SIZE.getKey(), mlMaxNodeSize + "b")
                .put(MAX_LAZY_ML_NODES.getKey(), randomIntBetween(1, 100))
                .put(MAX_MACHINE_MEMORY_PERCENT.getKey(), mlMemoryPercent)
                .build(),
            ML_MEMORY_RELATED_SETTINGS
        );

        DiscoveryNodes nodes = randomNodes(0, numNonMlNodes, 0);

        ByteSizeValue maxModelMemoryLimitToFit = NativeMemoryCalculator.calculateMaxModelMemoryLimitToFit(clusterSettings, nodes);

        // Expect configured percentage of maximum declared node size (allowing for small rounding errors)
        assertThat(maxModelMemoryLimitToFit, notNullValue());
        assertThat(
            maxModelMemoryLimitToFit.getBytes() + Math.max(
                Job.PROCESS_MEMORY_OVERHEAD.getBytes(),
                DataFrameAnalyticsConfig.PROCESS_MEMORY_OVERHEAD.getBytes()
            ) + MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(),
            lessThanOrEqualTo(mlMaxNodeSize * mlMemoryPercent / 100)
        );

        ByteSizeValue totalMlMemory = NativeMemoryCalculator.calculateTotalMlMemory(clusterSettings, nodes);

        assertThat(totalMlMemory, notNullValue());
        assertThat(totalMlMemory, is(ByteSizeValue.ofMb(0)));
    }

    public void testCalculateMaxModelMemoryToFitLimitSmallMlNodesButMaxMlNodeSizeBiggerAndLazyNodesAllowed() {

        int mlMemoryPercent = randomIntBetween(5, 90);
        long mlMaxNodeSize = randomLongBetween(2000000000L, 100000000000L);
        long mlMachineMemory = mlMaxNodeSize / randomLongBetween(3, 5);
        int numMlNodes = randomIntBetween(1, 10);
        int numNonMlNodes = randomIntBetween(0, 10);
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.builder()
                .put(MAX_ML_NODE_SIZE.getKey(), mlMaxNodeSize + "b")
                .put(MAX_LAZY_ML_NODES.getKey(), randomIntBetween(numMlNodes + 1, 100))
                .put(MAX_MACHINE_MEMORY_PERCENT.getKey(), mlMemoryPercent)
                .build(),
            ML_MEMORY_RELATED_SETTINGS
        );
        long totalMlMemoryBytes = numMlNodes * mlMachineMemory * mlMemoryPercent / 100;

        DiscoveryNodes nodes = randomNodes(numMlNodes, numNonMlNodes, mlMachineMemory);

        ByteSizeValue maxModelMemoryLimitToFit = NativeMemoryCalculator.calculateMaxModelMemoryLimitToFit(clusterSettings, nodes);

        // Expect configured percentage of maximum declared node size (allowing for small rounding errors) - bigger than current node size
        assertThat(maxModelMemoryLimitToFit, notNullValue());
        assertThat(
            maxModelMemoryLimitToFit.getBytes() + Math.max(
                Job.PROCESS_MEMORY_OVERHEAD.getBytes(),
                DataFrameAnalyticsConfig.PROCESS_MEMORY_OVERHEAD.getBytes()
            ) + MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(),
            lessThanOrEqualTo(mlMaxNodeSize * mlMemoryPercent / 100)
        );
        assertThat(
            maxModelMemoryLimitToFit.getBytes() + Math.max(
                Job.PROCESS_MEMORY_OVERHEAD.getBytes(),
                DataFrameAnalyticsConfig.PROCESS_MEMORY_OVERHEAD.getBytes()
            ) + MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(),
            greaterThan(2 * mlMachineMemory * mlMemoryPercent / 100)
        );

        ByteSizeValue totalMlMemory = NativeMemoryCalculator.calculateTotalMlMemory(clusterSettings, nodes);

        assertThat(totalMlMemory, notNullValue());
        assertThat(totalMlMemory, is(ByteSizeValue.ofMb(totalMlMemoryBytes / (1024 * 1024))));
    }

    public void testCalculateMaxModelMemoryLimitToFitSmallMlNodesButMaxMlNodeSizeBiggerAndLazyNodesExhausted() {

        int mlMemoryPercent = randomIntBetween(10, 90);
        long mlMaxNodeSize = randomLongBetween(2000000000L, 100000000000L);
        long mlMachineMemory = mlMaxNodeSize / randomLongBetween(3, 4);
        int numMlNodes = randomIntBetween(2, 10);
        int numNonMlNodes = randomIntBetween(0, 10);
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.builder()
                .put(MAX_ML_NODE_SIZE.getKey(), mlMaxNodeSize + "b")
                .put(MAX_LAZY_ML_NODES.getKey(), randomIntBetween(1, numMlNodes - 1))
                .put(MAX_MACHINE_MEMORY_PERCENT.getKey(), mlMemoryPercent)
                .build(),
            ML_MEMORY_RELATED_SETTINGS
        );
        long totalMlMemoryBytes = numMlNodes * mlMachineMemory * mlMemoryPercent / 100;

        DiscoveryNodes nodes = randomNodes(numMlNodes, numNonMlNodes, mlMachineMemory);

        ByteSizeValue maxModelMemoryLimitToFit = NativeMemoryCalculator.calculateMaxModelMemoryLimitToFit(clusterSettings, nodes);

        // Expect configured percentage of max node size - our lazy nodes are exhausted, but are smaller so should scale up to the max
        assertThat(maxModelMemoryLimitToFit, notNullValue());
        // Memory limit is rounded down to the next whole megabyte, so allow a 1MB range here
        assertThat(
            maxModelMemoryLimitToFit.getBytes() + Math.max(
                Job.PROCESS_MEMORY_OVERHEAD.getBytes(),
                DataFrameAnalyticsConfig.PROCESS_MEMORY_OVERHEAD.getBytes()
            ) + MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(),
            lessThanOrEqualTo(mlMaxNodeSize * mlMemoryPercent / 100)
        );
        assertThat(
            maxModelMemoryLimitToFit.getBytes() + Math.max(
                Job.PROCESS_MEMORY_OVERHEAD.getBytes(),
                DataFrameAnalyticsConfig.PROCESS_MEMORY_OVERHEAD.getBytes()
            ) + MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(),
            greaterThan(mlMaxNodeSize * mlMemoryPercent / 100 - ByteSizeValue.ofMb(1).getBytes())
        );

        ByteSizeValue totalMlMemory = NativeMemoryCalculator.calculateTotalMlMemory(clusterSettings, nodes);

        assertThat(totalMlMemory, notNullValue());
        assertThat(totalMlMemory, is(ByteSizeValue.ofMb(totalMlMemoryBytes / (1024 * 1024))));
    }

    public void testGetMaxModelMemoryLimitGivenNotSetNotAuto() {

        ByteSizeValue noLimit = ByteSizeValue.ZERO;

        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(
            ClusterState.builder(ClusterName.DEFAULT)
                .nodes(randomNodes(randomIntBetween(1, 3), randomIntBetween(1, 3), 10000000000L))
                .build()
        );
        when(clusterService.getClusterSettings()).thenReturn(newClusterSettings(30, false, noLimit));

        assertThat(NativeMemoryCalculator.getMaxModelMemoryLimit(clusterService), is(noLimit));
    }

    public void testGetMaxModelMemoryLimitGivenExplicitlySetNotAuto() {

        ByteSizeValue expectedLimit = ByteSizeValue.ofMb(randomIntBetween(100, 10000));

        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(
            ClusterState.builder(ClusterName.DEFAULT)
                .nodes(randomNodes(randomIntBetween(1, 3), randomIntBetween(1, 3), 10000000000L))
                .build()
        );
        when(clusterService.getClusterSettings()).thenReturn(newClusterSettings(30, false, expectedLimit));

        assertThat(NativeMemoryCalculator.getMaxModelMemoryLimit(clusterService), is(expectedLimit));
    }

    public void testGetMaxModelMemoryLimitGivenNotSetUsingAuto() {

        ClusterService clusterService = mock(ClusterService.class);
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(randomNodes(randomIntBetween(1, 3), randomIntBetween(1, 3), 10000000000L))
            .build();
        when(clusterService.state()).thenReturn(clusterState);
        ClusterSettings clusterSettings = newClusterSettings(30, true, ByteSizeValue.ZERO);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        assertThat(
            NativeMemoryCalculator.getMaxModelMemoryLimit(clusterService),
            is(NativeMemoryCalculator.calculateMaxModelMemoryLimitToFit(clusterSettings, clusterState.getNodes()))
        );
    }

    public void testGetMaxModelMemoryLimitGivenExplicitlySetUsingAuto() {

        ByteSizeValue expectedLimit = ByteSizeValue.ofMb(randomIntBetween(100, 10000));

        ClusterService clusterService = mock(ClusterService.class);
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(randomNodes(randomIntBetween(1, 3), randomIntBetween(1, 3), 10000000000L))
            .build();
        when(clusterService.state()).thenReturn(clusterState);
        ClusterSettings clusterSettings = newClusterSettings(30, true, expectedLimit);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        assertThat(NativeMemoryCalculator.getMaxModelMemoryLimit(clusterService), is(expectedLimit));
    }

    DiscoveryNodes randomNodes(int numMlNodes, int numNonMlNodes, long mlMachineMemory) {

        DiscoveryNodes.Builder builder = DiscoveryNodes.builder();

        for (int i = 0; i < numMlNodes + numNonMlNodes; ++i) {
            String nodeName = "_node_name" + i;
            String nodeId = "_node_id" + i;
            TransportAddress ta = new TransportAddress(InetAddress.getLoopbackAddress(), 9300 + i);
            if (i < numMlNodes) {
                // ML node
                builder.add(
                    new DiscoveryNode(
                        nodeName,
                        nodeId,
                        ta,
                        Collections.singletonMap(MachineLearning.MACHINE_MEMORY_NODE_ATTR, String.valueOf(mlMachineMemory)),
                        Set.of(DiscoveryNodeRole.ML_ROLE),
                        Version.CURRENT
                    )
                );
            } else {
                // Not an ML node
                builder.add(
                    new DiscoveryNode(
                        nodeName,
                        nodeId,
                        ta,
                        Collections.emptyMap(),
                        Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.INGEST_ROLE),
                        Version.CURRENT
                    )
                );
            }
        }

        return builder.build();
    }

    private static Settings newSettings(int maxMemoryPercent, boolean useAuto, ByteSizeValue maxModelMemoryLimit) {
        return Settings.builder()
            .put(USE_AUTO_MACHINE_MEMORY_PERCENT.getKey(), useAuto)
            .put(MAX_MACHINE_MEMORY_PERCENT.getKey(), maxMemoryPercent)
            .put(MAX_MODEL_MEMORY_LIMIT.getKey(), maxModelMemoryLimit)
            .build();
    }

    private static ClusterSettings newClusterSettings(int maxMemoryPercent, boolean useAuto, ByteSizeValue maxModelMemoryLimit) {
        return new ClusterSettings(newSettings(maxMemoryPercent, useAuto, maxModelMemoryLimit), ML_MEMORY_RELATED_SETTINGS);
    }

    private static DiscoveryNode newNode(Long jvmSizeLong, Long mlNodeSizeLong) {
        String jvmSize = jvmSizeLong != null ? jvmSizeLong.toString() : null;
        String mlNodeSize = mlNodeSizeLong != null ? mlNodeSizeLong.toString() : null;
        Map<String, String> attrs = new HashMap<>();
        if (jvmSize != null) {
            attrs.put(MAX_JVM_SIZE_NODE_ATTR, jvmSize);
        }
        Set<DiscoveryNodeRole> roles;
        if (mlNodeSize != null) {
            attrs.put(MACHINE_MEMORY_NODE_ATTR, mlNodeSize);
            roles = Set.of(DiscoveryNodeRole.ML_ROLE);
        } else {
            roles = Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.INGEST_ROLE);
        }
        return new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), attrs, roles, Version.CURRENT);
    }

}
