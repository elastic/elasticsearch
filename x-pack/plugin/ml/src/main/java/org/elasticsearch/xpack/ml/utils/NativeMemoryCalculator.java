/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.utils;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.util.OptionalLong;

import static org.elasticsearch.xpack.ml.MachineLearning.DYNAMIC_MEMORY_PERCENT;
import static org.elasticsearch.xpack.ml.MachineLearning.MACHINE_MEMORY_NODE_ATTR;
import static org.elasticsearch.xpack.ml.MachineLearning.MAX_JVM_SIZE_NODE_ATTR;
import static org.elasticsearch.xpack.ml.MachineLearning.MAX_MACHINE_MEMORY_PERCENT;

public final class NativeMemoryCalculator {

    private static final long OS_OVERHEAD = ByteSizeValue.ofMb(200L).getBytes();

    private NativeMemoryCalculator() {
    }

    public static OptionalLong allowedBytesForMl(DiscoveryNode node, Settings settings) {
        return allowedBytesForMl(
            node.getAttributes().get(MACHINE_MEMORY_NODE_ATTR),
            node.getAttributes().get(MAX_JVM_SIZE_NODE_ATTR),
            MAX_MACHINE_MEMORY_PERCENT.get(settings));
    }

    public static OptionalLong allowedBytesForMl(DiscoveryNode node, ClusterSettings settings) {
        return allowedBytesForMl(
            node.getAttributes().get(MACHINE_MEMORY_NODE_ATTR),
            node.getAttributes().get(MAX_JVM_SIZE_NODE_ATTR),
            settings.get(MAX_MACHINE_MEMORY_PERCENT));
    }

    public static OptionalLong allowedBytesForMl(DiscoveryNode node, int maxMemoryPercent) {
        return allowedBytesForMl(
            node.getAttributes().get(MACHINE_MEMORY_NODE_ATTR),
            node.getAttributes().get(MAX_JVM_SIZE_NODE_ATTR),
            maxMemoryPercent);
    }

    private static OptionalLong allowedBytesForMl(String nodeBytes, String jvmBytes, int maxMemoryPercent) {
        if (nodeBytes == null) {
            return OptionalLong.empty();
        }
        final long machineMemory;
        try {
            machineMemory = Long.parseLong(nodeBytes);
        } catch (NumberFormatException e) {
            return OptionalLong.empty();
        }
        Long jvmMemory = null;
        try {
            if (Strings.isNullOrEmpty(jvmBytes) == false) {
                jvmMemory = Long.parseLong(jvmBytes);
            }
        } catch (NumberFormatException e) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(allowedBytesForMl(machineMemory, jvmMemory, maxMemoryPercent));
    }

    public static int modelMemoryPercent(long machineMemory, long jvmSize, int maxMemoryPercent) {
        if (maxMemoryPercent == DYNAMIC_MEMORY_PERCENT) {
            // This calculation is dynamic and designed to maximally take advantage of the underlying machine for machine learning
            // We only allow 200MB for the Operating system itself and take up to 90% of the underlying native memory left
            // Example calculations:
            // 1GB node -> 41%
            // 2GB node -> 66%
            // 16GB node -> 87%
            // 64GB node -> 90%
            return Math.min(90, (int)Math.ceil(((machineMemory - jvmSize - OS_OVERHEAD) / (double)machineMemory) * 100.0D));
        }
        return maxMemoryPercent;
    }

    public static int modelMemoryPercent(long machineMemory, double jvmFraction, int maxMemoryPercent) {
        return modelMemoryPercent(machineMemory, (long) (jvmFraction * machineMemory), maxMemoryPercent);
    }

    public static int modelMemoryPercent(long machineMemory, int maxMemoryPercent) {
        return modelMemoryPercent(machineMemory,
            maxMemoryPercent == DYNAMIC_MEMORY_PERCENT ? dynamicallyCalculateJvm(machineMemory) : machineMemory / 2,
            maxMemoryPercent);
    }

    public static long allowedBytesForMl(long machineMemory, Long jvmSize, int maxMemoryPercent) {
        if (maxMemoryPercent == DYNAMIC_MEMORY_PERCENT) {
            assert jvmSize != null;
            // This calculation is dynamic and designed to maximally take advantage of the underlying machine for machine learning
            // We only allow 200MB for the Operating system itself and take up to 90% of the underlying native memory left
            // Example calculations:
            // 1GB node -> 41%
            // 2GB node -> 66%
            // 16GB node -> 87%
            // 64GB node -> 90%
            long memoryPercent = Math.min(90, (int)Math.ceil(((machineMemory - jvmSize - OS_OVERHEAD) / (double)machineMemory) * 100.0D));
            return machineMemory * memoryPercent / 100;
        }

        return machineMemory * maxMemoryPercent / 100;
    }

    public static long allowedBytesForMl(long machineMemory, int maxMemoryPercent) {
        return allowedBytesForMl(machineMemory,
            maxMemoryPercent == DYNAMIC_MEMORY_PERCENT ? dynamicallyCalculateJvm(machineMemory) : machineMemory / 2,
            maxMemoryPercent);
    }

    // TODO how can we make this better???
    private static long dynamicallyCalculateJvm(long nodeSize) {
        if (nodeSize < new ByteSizeValue(2, ByteSizeUnit.GB).getBytes()) {
            return (long)(nodeSize * 0.40);
        }
        if (nodeSize < new ByteSizeValue(8, ByteSizeUnit.GB).getBytes()) {
            return (long) (nodeSize * 0.25);
        }
        return new ByteSizeValue(2, ByteSizeUnit.GB).getBytes();
    }

    // TODO
    // How can we determine the accurate node size needed knowing the required memory for the job?
    public static long requiredNodeSize(long requiredModelMemory, long jvmSize, int maxMemoryPercent) {
        return 0L;
    }

}
