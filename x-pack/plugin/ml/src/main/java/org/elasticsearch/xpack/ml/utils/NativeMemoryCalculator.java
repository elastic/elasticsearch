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

import java.util.OptionalLong;

import static org.elasticsearch.xpack.ml.MachineLearning.MACHINE_MEMORY_NODE_ATTR;
import static org.elasticsearch.xpack.ml.MachineLearning.MAX_JVM_SIZE_NODE_ATTR;
import static org.elasticsearch.xpack.ml.MachineLearning.MAX_MACHINE_MEMORY_PERCENT;
import static org.elasticsearch.xpack.ml.MachineLearning.USE_AUTO_MACHINE_MEMORY_PERCENT;

public final class NativeMemoryCalculator {

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

    public static OptionalLong allowedBytesForMl(DiscoveryNode node, int maxMemoryPercent, boolean useAuto) {
        return allowedBytesForMl(
            node.getAttributes().get(MACHINE_MEMORY_NODE_ATTR),
            node.getAttributes().get(MAX_JVM_SIZE_NODE_ATTR),
            maxMemoryPercent,
            useAuto);
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
            return machineMemory * memoryPercent / 100;
        }

        return machineMemory * maxMemoryPercent / 100;
    }

}
