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
import org.elasticsearch.common.unit.ByteSizeValue;

import java.util.Locale;
import java.util.OptionalLong;

import static org.elasticsearch.xpack.ml.MachineLearning.MACHINE_MEMORY_NODE_ATTR;
import static org.elasticsearch.xpack.ml.MachineLearning.MAX_JVM_SIZE_NODE_ATTR;
import static org.elasticsearch.xpack.ml.MachineLearning.MAX_MACHINE_MEMORY_PERCENT;
import static org.elasticsearch.xpack.ml.MachineLearning.USE_AUTO_MACHINE_MEMORY_PERCENT;

public final class NativeMemoryCalculator {

    static final long MINIMUM_AUTOMATIC_NODE_SIZE = ByteSizeValue.ofGb(1).getBytes();
    private static final long OS_OVERHEAD = ByteSizeValue.ofMb(200L).getBytes();

    private NativeMemoryCalculator() { }

    public static OptionalLong allowedBytesForMl(DiscoveryNode node, Settings settings) {
        return allowedBytesForMl(
            node.getAttributes().get(MACHINE_MEMORY_NODE_ATTR),
            node.getAttributes().get(MAX_JVM_SIZE_NODE_ATTR),
            MAX_MACHINE_MEMORY_PERCENT.get(settings),
            USE_AUTO_MACHINE_MEMORY_PERCENT.get(settings));
    }

    public static OptionalLong allowedBytesForMl(DiscoveryNode node, ClusterSettings settings) {
        return allowedBytesForMl(
            node.getAttributes().get(MACHINE_MEMORY_NODE_ATTR),
            node.getAttributes().get(MAX_JVM_SIZE_NODE_ATTR),
            settings.get(MAX_MACHINE_MEMORY_PERCENT),
            settings.get(USE_AUTO_MACHINE_MEMORY_PERCENT));
    }

    public static OptionalLong allowedBytesForMl(DiscoveryNode node, int maxMemoryPercent, boolean useAutoPercent) {
        return allowedBytesForMl(
            node.getAttributes().get(MACHINE_MEMORY_NODE_ATTR),
            node.getAttributes().get(MAX_JVM_SIZE_NODE_ATTR),
            maxMemoryPercent,
            useAutoPercent);
    }

    private static OptionalLong allowedBytesForMl(String nodeBytes, String jvmBytes, int maxMemoryPercent, boolean useAuto) {
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
        return OptionalLong.of(allowedBytesForMl(machineMemory, jvmMemory, maxMemoryPercent, useAuto));
    }

    public static long calculateApproxNecessaryNodeSize(long nativeMachineMemory, Long jvmSize, int maxMemoryPercent, boolean useAuto) {
        if (nativeMachineMemory == 0) {
            return 0;
        }
        if (useAuto) {
            // TODO utilize official ergonomic JVM size calculations when available.
            jvmSize = jvmSize == null ? dynamicallyCalculateJvmSizeFromNativeMemorySize(nativeMachineMemory) : jvmSize;
            // We use a Math.floor here to ensure we have AT LEAST enough memory given rounding.
            int modelMemoryPercent = (int)Math.floor(modelMemoryPercent(
                nativeMachineMemory + jvmSize + OS_OVERHEAD,
                jvmSize,
                maxMemoryPercent,
                true));
            // We calculate the inverse percentage of `nativeMachineMemory + OS_OVERHEAD` as `OS_OVERHEAD` is always present
            // on the native memory side and we need to account for it when we invert the model memory percentage
            return Math.max((long)Math.ceil((100.0/modelMemoryPercent) * (nativeMachineMemory + OS_OVERHEAD)), MINIMUM_AUTOMATIC_NODE_SIZE);
        }
        return (long) ((100.0/maxMemoryPercent) * nativeMachineMemory);
    }

    public static double modelMemoryPercent(long machineMemory, Long jvmSize, int maxMemoryPercent, boolean useAuto) {
        if (useAuto) {
            jvmSize = jvmSize == null ? dynamicallyCalculateJvmSizeFromNodeSize(machineMemory) : jvmSize;
            if (machineMemory - jvmSize < OS_OVERHEAD || machineMemory == 0) {
                assert false: String.format(
                    Locale.ROOT,
                    "machine memory [%d] minus jvm [%d] is less than overhead [%d]",
                    machineMemory,
                    jvmSize,
                    OS_OVERHEAD
                );
                return maxMemoryPercent;
            }
            // This calculation is dynamic and designed to maximally take advantage of the underlying machine for machine learning
            // We only allow 200MB for the Operating system itself and take up to 90% of the underlying native memory left
            // Example calculations:
            // 1GB node -> 41%
            // 2GB node -> 66%
            // 16GB node -> 87%
            // 64GB node -> 90%
            return Math.min(90.0, ((machineMemory - jvmSize - OS_OVERHEAD) / (double)machineMemory) * 100.0D);
        }
        return maxMemoryPercent;
    }

    public static int modelMemoryPercent(long machineMemory, int maxMemoryPercent, boolean useAuto) {
        return (int)Math.ceil(modelMemoryPercent(machineMemory,
            null,
            maxMemoryPercent,
            useAuto));
    }

    private static long allowedBytesForMl(long machineMemory, Long jvmSize, int maxMemoryPercent, boolean useAuto) {
        if (useAuto && jvmSize != null) {
            // It is conceivable that there is a machine smaller than 200MB.
            // If the administrator wants to use the auto configuration, the node should be larger.
            if (machineMemory - jvmSize < OS_OVERHEAD || machineMemory == 0) {
                return machineMemory / 100;
            }
            // This calculation is dynamic and designed to maximally take advantage of the underlying machine for machine learning
            // We only allow 200MB for the Operating system itself and take up to 90% of the underlying native memory left
            // Example calculations:
            // 1GB node -> 41%
            // 2GB node -> 66%
            // 16GB node -> 87%
            // 64GB node -> 90%
            long memoryPercent = Math.min(90, (int)Math.ceil(((machineMemory - jvmSize - OS_OVERHEAD) / (double)machineMemory) * 100.0D));
            return (long)(machineMemory * (memoryPercent / 100.0));
        }

        return (long)(machineMemory * (maxMemoryPercent / 100.0));
    }

    public static long allowedBytesForMl(long machineMemory, int maxMemoryPercent, boolean useAuto) {
        return allowedBytesForMl(machineMemory,
            useAuto ? dynamicallyCalculateJvmSizeFromNodeSize(machineMemory) : machineMemory / 2,
            maxMemoryPercent,
            useAuto);
    }

    // TODO replace with official ergonomic calculation
    private static long dynamicallyCalculateJvmSizeFromNodeSize(long nodeSize) {
        if (nodeSize < ByteSizeValue.ofGb(2).getBytes()) {
            return (long) (nodeSize * 0.40);
        }
        if (nodeSize < ByteSizeValue.ofGb(8).getBytes()) {
            return (long) (nodeSize * 0.25);
        }
        return ByteSizeValue.ofGb(2).getBytes();
    }

    private static long dynamicallyCalculateJvmSizeFromNativeMemorySize(long nativeMachineMemory) {
        // See dynamicallyCalculateJvm the following JVM calculations are arithmetic inverses of JVM calculation
        //
        // Example: For < 2GB node, the JVM is 0.4 * total_node_size. This means, the rest is 0.6 the node size.
        // So, the `nativeAndOverhead` is == 0.6 * total_node_size => total_node_size = (nativeAndOverHead / 0.6)
        // Consequently jvmSize = (nativeAndOverHead / 0.6)*0.4 = nativeAndOverHead * 2/3
        long nativeAndOverhead = nativeMachineMemory + OS_OVERHEAD;
        if (nativeAndOverhead < (ByteSizeValue.ofGb(2).getBytes() * 0.60)) {
            return (long) Math.ceil(nativeAndOverhead * (2.0 / 3.0));
        }
        if (nativeAndOverhead < (ByteSizeValue.ofGb(8).getBytes() * 0.75)) {
            return (long) Math.ceil(nativeAndOverhead / 3.0);
        }
        return ByteSizeValue.ofGb(2).getBytes();
    }

}
