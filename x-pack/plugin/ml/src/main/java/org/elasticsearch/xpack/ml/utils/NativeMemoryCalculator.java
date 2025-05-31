/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.utils;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.util.OptionalLong;

import static org.elasticsearch.xpack.core.ml.MachineLearningField.MAX_LAZY_ML_NODES;
import static org.elasticsearch.xpack.core.ml.MachineLearningField.USE_AUTO_MACHINE_MEMORY_PERCENT;
import static org.elasticsearch.xpack.ml.MachineLearning.MACHINE_MEMORY_NODE_ATTR;
import static org.elasticsearch.xpack.ml.MachineLearning.MAX_JVM_SIZE_NODE_ATTR;
import static org.elasticsearch.xpack.ml.MachineLearning.MAX_MACHINE_MEMORY_PERCENT;
import static org.elasticsearch.xpack.ml.MachineLearning.MAX_ML_NODE_SIZE;

public final class NativeMemoryCalculator {

    // Maximum permitted JVM heap size when auto-configured.
    // Must match the value used in MachineDependentHeap.MachineNodeRole.ML_ONLY.
    public static final long STATIC_JVM_UPPER_THRESHOLD = ByteSizeValue.ofGb(31).getBytes();
    public static final long MINIMUM_AUTOMATIC_NODE_SIZE = ByteSizeValue.ofMb(512).getBytes();
    private static final long OS_OVERHEAD = ByteSizeValue.ofMb(200).getBytes();
    // Memory size beyond which the JVM is given 10% of memory instead of 40%.
    // Must match the value used in MachineDependentHeap.MachineNodeRole.ML_ONLY.
    public static final long JVM_SIZE_KNOT_POINT = ByteSizeValue.ofGb(16).getBytes();
    private static final long BYTES_IN_4MB = ByteSizeValue.ofMb(4).getBytes();
    // The minimum automatic node size implicitly defines a minimum JVM size
    private static final long MINIMUM_AUTOMATIC_JVM_SIZE = dynamicallyCalculateJvmSizeFromNodeSize(MINIMUM_AUTOMATIC_NODE_SIZE);

    private NativeMemoryCalculator() {}

    public static OptionalLong allowedBytesForMl(DiscoveryNode node, Settings settings) {
        if (node.getRoles().contains(DiscoveryNodeRole.ML_ROLE) == false) {
            return OptionalLong.empty();
        }
        return allowedBytesForMl(
            node.getAttributes().get(MACHINE_MEMORY_NODE_ATTR),
            node.getAttributes().get(MAX_JVM_SIZE_NODE_ATTR),
            MAX_MACHINE_MEMORY_PERCENT.get(settings),
            USE_AUTO_MACHINE_MEMORY_PERCENT.get(settings)
        );
    }

    public static OptionalLong allowedBytesForMl(DiscoveryNode node, ClusterSettings settings) {
        if (node.getRoles().contains(DiscoveryNodeRole.ML_ROLE) == false) {
            return OptionalLong.empty();
        }
        return allowedBytesForMl(
            node.getAttributes().get(MACHINE_MEMORY_NODE_ATTR),
            node.getAttributes().get(MAX_JVM_SIZE_NODE_ATTR),
            settings.get(MAX_MACHINE_MEMORY_PERCENT),
            settings.get(USE_AUTO_MACHINE_MEMORY_PERCENT)
        );
    }

    public static OptionalLong allowedBytesForMl(DiscoveryNode node, int maxMemoryPercent, boolean useAutoPercent) {
        if (node.getRoles().contains(DiscoveryNodeRole.ML_ROLE) == false) {
            return OptionalLong.empty();
        }
        return allowedBytesForMl(
            node.getAttributes().get(MACHINE_MEMORY_NODE_ATTR),
            node.getAttributes().get(MAX_JVM_SIZE_NODE_ATTR),
            maxMemoryPercent,
            useAutoPercent
        );
    }

    private static OptionalLong allowedBytesForMl(String nodeBytes, String jvmBytes, int maxMemoryPercent, boolean useAuto) {
        assert nodeBytes != null
            : "This private method should only be called for ML nodes, and all ML nodes should have the ml.machine_memory node attribute";
        if (nodeBytes == null) {
            return OptionalLong.empty();
        }
        final long machineMemory;
        try {
            machineMemory = Long.parseLong(nodeBytes);
        } catch (NumberFormatException e) {
            assert e == null : "ml.machine_memory should parse because we set it internally: invalid value was " + nodeBytes;
            return OptionalLong.empty();
        }
        assert jvmBytes != null
            : "This private method should only be called for ML nodes, and all ML nodes should have the ml.max_jvm_size node attribute";
        if (jvmBytes == null) {
            return OptionalLong.empty();
        }
        long jvmMemory;
        try {
            jvmMemory = Long.parseLong(jvmBytes);
        } catch (NumberFormatException e) {
            assert e == null : "ml.max_jvm_size should parse because we set it internally: invalid value was " + jvmBytes;
            return OptionalLong.empty();
        }
        return OptionalLong.of(allowedBytesForMl(machineMemory, jvmMemory, maxMemoryPercent, useAuto));
    }

    public static long calculateApproxNecessaryNodeSize(
        long mlNativeMemoryRequirement,
        Long jvmSize,
        int maxMemoryPercent,
        boolean useAuto
    ) {
        if (mlNativeMemoryRequirement == 0) {
            return 0;
        }
        if (useAuto) {
            jvmSize = jvmSize == null ? dynamicallyCalculateJvmSizeFromMlNativeMemorySize(mlNativeMemoryRequirement) : jvmSize;
            return Math.max(mlNativeMemoryRequirement + jvmSize + OS_OVERHEAD, MINIMUM_AUTOMATIC_NODE_SIZE);
        }
        // Round up here, to ensure enough ML memory when the formula is reversed
        return (long) Math.ceil((100.0 / maxMemoryPercent) * mlNativeMemoryRequirement);
    }

    static long allowedBytesForMl(long machineMemory, long jvmSize, int maxMemoryPercent, boolean useAuto) {
        // machineMemory can get set to -1 if the OS probe that determines memory fails
        if (machineMemory <= 0) {
            return 0L;
        }
        if (useAuto) {
            // It is conceivable that there is a machine smaller than 200MB.
            // If the administrator wants to use the auto configuration, the node should be larger.
            if (machineMemory - jvmSize <= OS_OVERHEAD) {
                return machineMemory / 100;
            }
            // This calculation is dynamic and designed to maximally take advantage of the underlying machine for machine learning.
            // We allow for the JVM and 200MB for the operating system, and then use the remaining space for ML native memory subject
            // to a maximum of 90% of the node size.
            return Math.min(machineMemory - jvmSize - OS_OVERHEAD, machineMemory * 9 / 10);
        }
        // Round down here, so we don't permit a model that's 1 byte too big after rounding
        return machineMemory * maxMemoryPercent / 100;
    }

    public static long allowedBytesForMl(long machineMemory, int maxMemoryPercent, boolean useAuto) {
        return allowedBytesForMl(
            machineMemory,
            useAuto ? dynamicallyCalculateJvmSizeFromNodeSize(machineMemory) : Math.min(machineMemory / 2, STATIC_JVM_UPPER_THRESHOLD),
            maxMemoryPercent,
            useAuto
        );
    }

    public static long dynamicallyCalculateJvmHeapSizeFromNodeSize(long nodeSize) {
        // This must match the logic in MachineDependentHeap.MachineNodeRole.ML_ONLY,
        // including rounding down to the next lower multiple of 4 megabytes.
        if (nodeSize <= JVM_SIZE_KNOT_POINT) {
            return ((long) (nodeSize * 0.4) / BYTES_IN_4MB) * BYTES_IN_4MB;
        }
        return Math.min(
            ((long) (JVM_SIZE_KNOT_POINT * 0.4 + (nodeSize - JVM_SIZE_KNOT_POINT) * 0.1) / BYTES_IN_4MB) * BYTES_IN_4MB,
            STATIC_JVM_UPPER_THRESHOLD
        );
    }

    public static long dynamicallyCalculateJvmSizeFromNodeSize(long nodeSize) {
        // The direct memory size is half of the JVM heap size.
        return dynamicallyCalculateJvmHeapSizeFromNodeSize(nodeSize) * 3 / 2;
    }

    public static long dynamicallyCalculateJvmSizeFromMlNativeMemorySize(long mlNativeMemorySize) {
        // For <= 16GB node, the JVM heap is 40% of te memory, direct memory is 20% and the
        // remainder of 40% is for ML + OS. Therefore, heapSize = nativeAndOverhead.
        //
        // For > 16GB node, the JVM heap is 40% of the first 16GB plus 10% of the remainder.
        // The direct memory is half of the heap (so 20% of 16GB + 5% of remainder)
        // ML + OS gets the rest of the memory (so 40% of 16GB + 85% of the remainder).
        long nativeAndOverhead = mlNativeMemorySize + OS_OVERHEAD;
        long heapSize;
        if (nativeAndOverhead <= (JVM_SIZE_KNOT_POINT - dynamicallyCalculateJvmSizeFromNodeSize(JVM_SIZE_KNOT_POINT))) {
            heapSize = nativeAndOverhead;
        } else {
            double nativeAndOverheadAbove16GB = nativeAndOverhead - JVM_SIZE_KNOT_POINT * 0.4;
            heapSize = ((long) (JVM_SIZE_KNOT_POINT * 0.4 + nativeAndOverheadAbove16GB / 0.85 * 0.1));
        }
        // Round JVM size to the next lower multiple of 4 megabytes to match MachineDependentHeap.MachineNodeRole.ML_ONLY.
        heapSize = heapSize / BYTES_IN_4MB * BYTES_IN_4MB;
        // Because we're rounding JVM size to a multiple of 4MB there will be a range of node sizes that can satisfy the required
        // amount of ML memory. It's better to choose the lower size, because it avoids waste and avoids the possibility of a
        // scale up followed by a scale down. For example, suppose we asked for a 2049MB node when the job would also fit on a
        // 2048MB node. Then Cloud will give us a 4096MB node (because it can only give us certain fixed sizes). That immediately
        // shows up as wasteful in the ML overview in the UI where the user can visually see that half the ML memory is unused.
        // And an hour later the downscale will realise that the job could fit on a smaller node and will downscale to a 2048MB
        // node. So it's better all round to choose the slightly lower size from the start if everything will still fit.
        while (heapSize > BYTES_IN_4MB) {
            long lowerAnswer = heapSize - BYTES_IN_4MB;
            long nodeSizeImpliedByLowerAnswer = nativeAndOverhead + lowerAnswer * 3 / 2;
            if (dynamicallyCalculateJvmHeapSizeFromNodeSize(nodeSizeImpliedByLowerAnswer) == lowerAnswer) {
                heapSize = lowerAnswer;
            } else {
                break;
            }
        }
        heapSize = Math.max(MINIMUM_AUTOMATIC_JVM_SIZE, Math.min(heapSize, STATIC_JVM_UPPER_THRESHOLD));
        long directMemorySize = heapSize / 2;
        return heapSize + directMemorySize;
    }

    /**
     * Calculates the highest model memory limit that a job could be
     * given and still stand a chance of being assigned in the cluster.
     * The calculation takes into account the possibility of autoscaling,
     * i.e. if lazy nodes are available then the maximum possible node
     * size is considered as well as the sizes of nodes in the current
     * cluster.
     */
    public static ByteSizeValue calculateMaxModelMemoryLimitToFit(ClusterSettings clusterSettings, DiscoveryNodes nodes) {

        long maxMlMemory = 0;

        for (DiscoveryNode node : nodes) {
            OptionalLong limit = allowedBytesForMl(node, clusterSettings);
            if (limit.isEmpty()) {
                continue;
            }
            maxMlMemory = Math.max(maxMlMemory, limit.getAsLong());
        }

        // It is possible that there is scope for more ML nodes to be added
        // to the cluster, in which case take those into account too
        long maxMlNodeSize = clusterSettings.get(MAX_ML_NODE_SIZE).getBytes();
        int maxLazyNodes = clusterSettings.get(MAX_LAZY_ML_NODES);
        // Even if all the lazy nodes have been added to the cluster, we make
        // the assumption that if any were configured they'll be able to grow
        // to the maximum ML node size. (We are assuming that lazy nodes always
        // behave like they do with Elastic Cloud autoscaling, where vertical
        // scaling is possible.)
        if (maxMlNodeSize > 0 && maxLazyNodes > 0) {
            maxMlMemory = Math.max(
                maxMlMemory,
                allowedBytesForMl(
                    maxMlNodeSize,
                    clusterSettings.get(MAX_MACHINE_MEMORY_PERCENT),
                    clusterSettings.get(USE_AUTO_MACHINE_MEMORY_PERCENT)
                )
            );
        }

        if (maxMlMemory == 0L) {
            // This implies there are currently no ML nodes in the cluster, and
            // no automatic mechanism for adding one, so we have no idea what
            // the effective limit would be if one were added
            return null;
        }

        maxMlMemory -= Math.max(Job.PROCESS_MEMORY_OVERHEAD.getBytes(), DataFrameAnalyticsConfig.PROCESS_MEMORY_OVERHEAD.getBytes());
        maxMlMemory -= MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes();
        return ByteSizeValue.ofMb(ByteSizeUnit.BYTES.toMB(Math.max(0L, maxMlMemory)));
    }

    public static ByteSizeValue calculateTotalMlMemory(ClusterSettings clusterSettings, DiscoveryNodes nodes) {

        long totalMlMemory = 0;

        for (DiscoveryNode node : nodes) {
            OptionalLong limit = allowedBytesForMl(node, clusterSettings);
            if (limit.isEmpty()) {
                continue;
            }
            totalMlMemory += limit.getAsLong();
        }

        // Round down to a whole number of megabytes, since we generally deal with model
        // memory limits in whole megabytes
        return ByteSizeValue.ofMb(ByteSizeUnit.BYTES.toMB(totalMlMemory));
    }

    /**
     * Get the maximum value of model memory limit that a user may set in a job config.
     * If the xpack.ml.max_model_memory_limit setting is set then the value comes from that.
     * Otherwise, if xpack.ml.use_auto_machine_memory_percent is set then the maximum model
     * memory limit is considered to be the largest model memory limit that could fit into
     * the cluster (on the assumption that configured lazy nodes will be added and other
     * jobs stopped to make space).
     * @return The maximum model memory limit calculated from the current cluster settings,
     *         or {@link ByteSizeValue#ZERO} if there is no limit.
     */
    public static ByteSizeValue getMaxModelMemoryLimit(ClusterService clusterService) {
        ClusterSettings clusterSettings = clusterService.getClusterSettings();
        ByteSizeValue maxModelMemoryLimit = clusterSettings.get(MachineLearningField.MAX_MODEL_MEMORY_LIMIT);
        if (maxModelMemoryLimit != null && maxModelMemoryLimit.getBytes() > 0) {
            return maxModelMemoryLimit;
        }
        // When the ML memory percent is being set automatically and no explicit max model memory limit is set,
        // max model memory limit is considered to be the max model memory limit that will fit in the cluster
        Boolean autoMemory = clusterSettings.get(MachineLearningField.USE_AUTO_MACHINE_MEMORY_PERCENT);
        if (autoMemory) {
            DiscoveryNodes nodes = clusterService.state().getNodes();
            ByteSizeValue modelMemoryLimitToFit = calculateMaxModelMemoryLimitToFit(clusterSettings, nodes);
            if (modelMemoryLimitToFit != null) {
                return modelMemoryLimitToFit;
            }
        }
        return ByteSizeValue.ZERO;
    }
}
