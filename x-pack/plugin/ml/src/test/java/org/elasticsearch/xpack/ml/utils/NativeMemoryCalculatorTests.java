/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.utils;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingCapacity;
import org.elasticsearch.xpack.ml.autoscaling.NativeMemoryCapacity;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.function.BiConsumer;

import static org.elasticsearch.xpack.ml.MachineLearning.MACHINE_MEMORY_NODE_ATTR;
import static org.elasticsearch.xpack.ml.MachineLearning.MAX_JVM_SIZE_NODE_ATTR;
import static org.elasticsearch.xpack.ml.MachineLearning.MAX_MACHINE_MEMORY_PERCENT;
import static org.elasticsearch.xpack.ml.MachineLearning.USE_AUTO_MACHINE_MEMORY_PERCENT;
import static org.elasticsearch.xpack.ml.utils.NativeMemoryCalculator.MINIMUM_AUTOMATIC_NODE_SIZE;
import static org.elasticsearch.xpack.ml.utils.NativeMemoryCalculator.dynamicallyCalculateJvmSizeFromNodeSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class NativeMemoryCalculatorTests extends ESTestCase{

    private static final int NUM_TEST_RUNS = 10;
    public void testAllowedBytesForMLWhenAutoIsFalse() {
        for (int i = 0; i < NUM_TEST_RUNS; i++) {
            long nodeSize = randomLongBetween(ByteSizeValue.ofMb(500).getBytes(), ByteSizeValue.ofGb(64).getBytes());
            int percent = randomIntBetween(5, 200);
            DiscoveryNode node = newNode(randomBoolean() ? null : randomNonNegativeLong(), nodeSize);
            Settings settings = newSettings(percent, false);
            ClusterSettings clusterSettings = newClusterSettings(percent, false);

            long expected = (long)(nodeSize * (percent / 100.0));

            assertThat(NativeMemoryCalculator.allowedBytesForMl(node, settings).getAsLong(), equalTo(expected));
            assertThat(NativeMemoryCalculator.allowedBytesForMl(node, clusterSettings).getAsLong(), equalTo(expected));
            assertThat(NativeMemoryCalculator.allowedBytesForMl(node, percent, false).getAsLong(), equalTo(expected));
        }
    }

    public void testConsistencyInAutoCalculation() {
        for (Tuple<Long, Long> nodeAndJvmSize : Arrays.asList(
            Tuple.tuple(1073741824L, 432013312L), // 1GB and true JVM size
            Tuple.tuple(2147483648L, 536870912L), // 2GB ...
            Tuple.tuple(4294967296L, 1073741824L), // 4GB ...
            Tuple.tuple(8589934592L, 2147483648L), // 8GB ...
            Tuple.tuple(17179869184L, 2147483648L), // 16GB ...
            Tuple.tuple(34359738368L, 2147483648L), // 32GB ...
            Tuple.tuple(68719476736L, 2147483648L), // 64GB ...
            Tuple.tuple(16106127360L, 2147483648L), // 15GB ...
            Tuple.tuple(32212254720L, 2147483648L), // 30GB ...
            Tuple.tuple(64424509440L, 2147483648L) // 60GB ...
        )) {
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
                Settings settings = newSettings(30, true);
                ClusterSettings clusterSettings = newClusterSettings(30, true);

                long bytesForML = randomBoolean() ?
                    NativeMemoryCalculator.allowedBytesForMl(node, settings).getAsLong() :
                    NativeMemoryCalculator.allowedBytesForMl(node, clusterSettings).getAsLong();

                NativeMemoryCapacity nativeMemoryCapacity = new NativeMemoryCapacity(
                    bytesForML,
                    bytesForML,
                    jvmSize
                );

                AutoscalingCapacity capacity = nativeMemoryCapacity.autoscalingCapacity(30, true);
                // We don't allow node sizes below 1GB, so we will always be at least that large
                // Also, allow 1 byte off for weird rounding issues
                assertThat(capacity.node().memory().getBytes(), greaterThanOrEqualTo(
                    Math.max(nodeSize, ByteSizeValue.ofGb(1).getBytes()) - 1L));
                assertThat(capacity.total().memory().getBytes(), greaterThanOrEqualTo(
                    Math.max(nodeSize, ByteSizeValue.ofGb(1).getBytes()) - 1L));
            }
        }
    }

    public void testAllowedBytesForMlWhenAutoIsTrue() {
        for (int i = 0; i < NUM_TEST_RUNS; i++) {
            long nodeSize = randomLongBetween(ByteSizeValue.ofMb(500).getBytes(), ByteSizeValue.ofGb(64).getBytes());
            long jvmSize = randomLongBetween(ByteSizeValue.ofMb(250).getBytes(), nodeSize - ByteSizeValue.ofMb(200).getBytes());
            int percent = randomIntBetween(5, 200);
            DiscoveryNode node = newNode(jvmSize, nodeSize);
            Settings settings = newSettings(percent, true);
            ClusterSettings clusterSettings = newClusterSettings(percent, true);

            double truePercent = Math.min(
                90,
                ((nodeSize - jvmSize - ByteSizeValue.ofMb(200).getBytes()) / (double)nodeSize) * 100.0D);
            long expected = Math.round(nodeSize * (truePercent / 100.0));

            assertThat(NativeMemoryCalculator.allowedBytesForMl(node, settings).getAsLong(), equalTo(expected));
            assertThat(NativeMemoryCalculator.allowedBytesForMl(node, clusterSettings).getAsLong(), equalTo(expected));
            assertThat(NativeMemoryCalculator.allowedBytesForMl(node, percent, true).getAsLong(), equalTo(expected));
        }
    }

    public void testAllowedBytesForMlWhenBothJVMAndNodeSizeAreUnknown() {
        int percent = randomIntBetween(5, 200);
        DiscoveryNode node = newNode(null, null);
        Settings settings = newSettings(percent, randomBoolean());
        ClusterSettings clusterSettings = newClusterSettings(percent, randomBoolean());

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
            Settings settings = newSettings(percent, true);
            ClusterSettings clusterSettings = newClusterSettings(percent, true);
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
            assertThat("node size [" + autoNodeSize +"] smaller than minimum required size [" + MINIMUM_AUTOMATIC_NODE_SIZE + "]",
                autoNodeSize,
                greaterThanOrEqualTo(MINIMUM_AUTOMATIC_NODE_SIZE));
            // Our approximate real node size should always return a usable native memory size that is at least the original native memory
            // size. Rounding errors may cause it to be non-exact.
            long allowedBytesForMl = NativeMemoryCalculator.allowedBytesForMl(autoNodeSize, memoryPercentage, true);
            assertThat("native memory ["
                    + allowedBytesForMl
                    + "] smaller than original native memory ["
                    + nativeMemory
                    + "]",
                allowedBytesForMl,
                greaterThanOrEqualTo(nativeMemory - delta));
        };

        final BiConsumer<Long, Integer> consistentManualAssertions = (nativeMemory, memoryPercentage) -> {
            assertThat(NativeMemoryCalculator.calculateApproxNecessaryNodeSize(nativeMemory, null, memoryPercentage, false),
                equalTo((long)((100.0/memoryPercentage) * nativeMemory)));
            assertThat(NativeMemoryCalculator.calculateApproxNecessaryNodeSize(
                nativeMemory,
                randomNonNegativeLong(),
                memoryPercentage,
                false),
                equalTo((long)((100.0/memoryPercentage) * nativeMemory)));
        };

        { // 0 memory
            assertThat(NativeMemoryCalculator.calculateApproxNecessaryNodeSize(
                0L,
                randomLongBetween(0L, ByteSizeValue.ofGb(100).getBytes()),
                randomIntBetween(0, 100),
                randomBoolean()
                ),
                equalTo(0L));
            assertThat(
                NativeMemoryCalculator.calculateApproxNecessaryNodeSize(0L, null, randomIntBetween(0, 100), randomBoolean()),
                equalTo(0L));
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

    private static Settings newSettings(int maxMemoryPercent, boolean useAuto) {
        return Settings.builder()
            .put(USE_AUTO_MACHINE_MEMORY_PERCENT.getKey(), useAuto)
            .put(MAX_MACHINE_MEMORY_PERCENT.getKey(), maxMemoryPercent)
            .build();
    }

    private static ClusterSettings newClusterSettings(int maxMemoryPercent, boolean useAuto) {
        return new ClusterSettings(
            newSettings(maxMemoryPercent, useAuto),
            Sets.newHashSet(USE_AUTO_MACHINE_MEMORY_PERCENT, MAX_MACHINE_MEMORY_PERCENT));
    }

    private static DiscoveryNode newNode(Long jvmSizeLong, Long nodeSizeLong) {
        String jvmSize = jvmSizeLong != null ? jvmSizeLong.toString() : null;
        String nodeSize = nodeSizeLong != null ? nodeSizeLong.toString() : null;
        Map<String, String> attrs = new HashMap<>();
        if (jvmSize != null) {
            attrs.put(MAX_JVM_SIZE_NODE_ATTR, jvmSize);
        }
        if (nodeSize != null) {
            attrs.put(MACHINE_MEMORY_NODE_ATTR, nodeSize);
        }
        return new DiscoveryNode(
            "node",
            ESTestCase.buildNewFakeTransportAddress(),
            attrs,
            DiscoveryNodeRole.roles(),
            Version.CURRENT
        );
    }

}
