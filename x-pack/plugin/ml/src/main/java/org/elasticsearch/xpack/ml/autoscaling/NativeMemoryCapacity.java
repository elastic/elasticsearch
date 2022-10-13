/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.autoscaling;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.utils.NativeMemoryCalculator;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;

import static org.elasticsearch.xpack.ml.MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD;

/**
 * Used for storing native memory capacity and then transforming it into an autoscaling capacity
 * which takes into account the whole node size.
 */
public class NativeMemoryCapacity {

    static final NativeMemoryCapacity ZERO = new NativeMemoryCapacity(0L, 0L);

    private static final Logger logger = LogManager.getLogger(NativeMemoryCapacity.class);

    private final long tierMlNativeMemoryRequirementExcludingOverhead;
    private final long nodeMlNativeMemoryRequirementExcludingOverhead;
    private final Long jvmSize;

    /**
     * @param tierMlNativeMemoryRequirementExcludingOverhead Sum of the native memory requirements for all jobs and models
     *                                                       <em>excluding</em> the memory requirement that is per-node rather than per
     *                                                       job/model.
     * @param nodeMlNativeMemoryRequirementExcludingOverhead Largest native memory requirement for any single job or model
     *                                                       <em>excluding</em> the memory requirement that is per-node rather than per
     *                                                       job/model.
     * @param jvmSize The JVM heap size, if accurately known for the required node and tier sizes. If the JVM heap size is not accurately
     *                known <code>null</code> should be provided so that a best effort is made to calculate it.
     */
    public NativeMemoryCapacity(
        long tierMlNativeMemoryRequirementExcludingOverhead,
        long nodeMlNativeMemoryRequirementExcludingOverhead,
        Long jvmSize
    ) {
        this.tierMlNativeMemoryRequirementExcludingOverhead = tierMlNativeMemoryRequirementExcludingOverhead;
        this.nodeMlNativeMemoryRequirementExcludingOverhead = nodeMlNativeMemoryRequirementExcludingOverhead;
        this.jvmSize = jvmSize;
    }

    NativeMemoryCapacity(long tierMlNativeMemoryRequirementExcludingOverhead, long nodeMlNativeMemoryRequirementExcludingOverhead) {
        this(tierMlNativeMemoryRequirementExcludingOverhead, nodeMlNativeMemoryRequirementExcludingOverhead, null);
    }

    /**
     * Merges the passed capacity with the current one. A new instance is created and returned
     * @param nativeMemoryCapacity the capacity to merge with
     * @return a new instance with the merged capacity values
     */
    NativeMemoryCapacity merge(final NativeMemoryCapacity nativeMemoryCapacity) {
        if (this == nativeMemoryCapacity) {
            return this;
        }
        if (nativeMemoryCapacity.nodeMlNativeMemoryRequirementExcludingOverhead == 0
            && nativeMemoryCapacity.tierMlNativeMemoryRequirementExcludingOverhead == 0) {
            return this;
        }
        if (this.nodeMlNativeMemoryRequirementExcludingOverhead == 0 && this.tierMlNativeMemoryRequirementExcludingOverhead == 0) {
            return nativeMemoryCapacity;
        }
        long tier = this.tierMlNativeMemoryRequirementExcludingOverhead
            + nativeMemoryCapacity.tierMlNativeMemoryRequirementExcludingOverhead;
        long node = Math.max(
            nativeMemoryCapacity.nodeMlNativeMemoryRequirementExcludingOverhead,
            this.nodeMlNativeMemoryRequirementExcludingOverhead
        );
        // If either new size is different we have no way of knowing if the JVM size would stay the same, so null out
        return new NativeMemoryCapacity(tier, node, null);
    }

    /**
     * Find the minimum node size required for ML nodes and the minimum tier size required for the complete ML
     * tier. The object already knows the required ML native memory per node and for the tier, but autoscaling
     * requires a response in terms of node memory. This method therefore maps back from ML native memory to
     * total node memory, taking into account the amount of memory needed for the JVM heap.
     * @param maxMemoryPercent The value from <code>xpack.ml.max_machine_memory_percent</code>, which sets the
     *                         percentage of total memory that can be used by ML processes <em>unless</em> using
     *                         auto mode. In auto mode this argument has <em>no effect</em>.
     * @param useAuto Are we automatically determining the amount of memory ML processes can use? Autoscaling
     *                works best when this is <code>true</code>.
     * @param mlNativeMemoryForLargestMlNode How much memory is available for ML processes on the largest possible
     *                                       ML node? In practice, what number do you get if you apply the formula
     *                                       for finding ML memory from node memory to the value of
     *                                       <code>xpack.ml.max_ml_node_size</code>?
     * @param numMlAvailabilityZones How many availability zones are ML nodes spread between within the cluster?
     *                               We make the assumption that ML nodes will be added evenly to all availability
     *                               zones that are configured for the ML tier. This number may be 0 if there are
     *                               currently no ML nodes and hence the number of ML availability zones is not
     *                               known.
     * @return The minimum node size required for ML nodes and the minimum tier size required for the complete ML
     *         tier.
     */
    public MlMemoryAutoscalingCapacity.Builder autoscalingCapacity(
        int maxMemoryPercent,
        boolean useAuto,
        long mlNativeMemoryForLargestMlNode,
        int numMlAvailabilityZones
    ) {
        // We cannot assert this in the constructor, as sometimes objects containing capacities are constructed to be merged
        // with current capacity. But by the time this method is called the merging should have created an object that obeys
        // these conditions.
        assert nodeMlNativeMemoryRequirementExcludingOverhead >= 0
            : "Node required should not be negative - was: " + nodeMlNativeMemoryRequirementExcludingOverhead;
        assert tierMlNativeMemoryRequirementExcludingOverhead >= nodeMlNativeMemoryRequirementExcludingOverhead
            : "Total tier required should never be smaller than largest node size required: "
                + tierMlNativeMemoryRequirementExcludingOverhead
                + " < "
                + nodeMlNativeMemoryRequirementExcludingOverhead;

        if (tierMlNativeMemoryRequirementExcludingOverhead <= 0 || nodeMlNativeMemoryRequirementExcludingOverhead <= 0) {
            if (tierMlNativeMemoryRequirementExcludingOverhead != 0 || nodeMlNativeMemoryRequirementExcludingOverhead != 0) {
                logger.error(
                    "Request to calculate autoscaling capacity with tier requirement [{}] and node requirement [{}]",
                    tierMlNativeMemoryRequirementExcludingOverhead,
                    nodeMlNativeMemoryRequirementExcludingOverhead
                );
            }
            return MlMemoryAutoscalingCapacity.builder(ByteSizeValue.ZERO, ByteSizeValue.ZERO);
        }

        if (mlNativeMemoryForLargestMlNode <= NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes()) {
            // This should never happen in Elastic Cloud, as the nodes are sized appropriately.
            // The logging is primarily to help diagnose failures in randomized unit tests.
            // This could also happen to a self-managed user if they set xpack.ml.max_ml_node_size
            // inappropriately and then called the autoscaling API.
            logger.error(
                "Largest possible ML node is too small to satisfy required ML native code overhead of [{}]",
                NATIVE_EXECUTABLE_CODE_OVERHEAD
            );
            // Ensure a value greater than zero to prevent division by zero lower down this method.
            mlNativeMemoryForLargestMlNode = NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes() + 1;
        } else if (mlNativeMemoryForLargestMlNode > Long.MAX_VALUE / 2) {
            // We need to cap "infinity" type inputs to prevent overflows in the calculations lower down this method.
            // The assumption is that half the maximum long is still high enough to mean unlimited.
            mlNativeMemoryForLargestMlNode = Long.MAX_VALUE / 2;
        }
        long mlNativeMemoryForLargestMlNodeExcludingOverhead = mlNativeMemoryForLargestMlNode - NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes();

        // This calculation is complicated, because we know the required ML native memory size but need to return
        // the required node size. The required node size is required native memory + 200MB + JVM size. But the
        // JVM size depends on the node size. There is a method to calculate JVM size from the ML native memory
        // size on a single node, but this will not necessarily be the minimum single node memory we could tolerate.
        // It might be bigger if the orchestrator gives us bigger nodes in order to satisfy the tier requirement.
        // We make a guess that the orchestrator will give us the same number of nodes per availability zone.
        //
        // We treat 0 ML availability zones the same way as 1. In reality we don't know how many ML availability
        // zones there will be when the ML tier first scales into existence, but the default is 1 so assume that.
        // Whatever we return from this method will at least force the ML tier into existence. If there are multiple
        // availability zones and we are right on the boundary of what size node is needed for the first jobs then
        // it is possible that we'll specify a size that's too small (due to the need for OS overhead on multiple
        // nodes when there are multiple availability zones). In that case the cluster will scale again once the
        // newly created tier reveals the number of ML availability zones. But this is an edge case because it
        // requires that many large jobs are all created in the period before the ML tier is scaled into existence.
        // It shouldn't be seen very often.
        long tierMlNativeMemoryRequirementPerZoneExcludingOverhead = (numMlAvailabilityZones > 1)
            ? ((tierMlNativeMemoryRequirementExcludingOverhead + numMlAvailabilityZones - 1) / numMlAvailabilityZones)
            : tierMlNativeMemoryRequirementExcludingOverhead;
        int numNodesPerZone = (int) ((tierMlNativeMemoryRequirementPerZoneExcludingOverhead
            + mlNativeMemoryForLargestMlNodeExcludingOverhead - 1) / mlNativeMemoryForLargestMlNodeExcludingOverhead);
        assert numNodesPerZone > 0
            : "calculated "
                + numNodesPerZone
                + " nodes per zone when tier memory requirement per zone was "
                + tierMlNativeMemoryRequirementPerZoneExcludingOverhead;
        long tierBasedMlNativeMemoryPerNodeExcludingOverhead = (tierMlNativeMemoryRequirementPerZoneExcludingOverhead + numNodesPerZone - 1)
            / numNodesPerZone;

        // We first need to calculate the required node size given the required native ML memory size.
        long requiredNodeSize = NativeMemoryCalculator.calculateApproxNecessaryNodeSize(
            nodeMlNativeMemoryRequirementExcludingOverhead + NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(),
            jvmSize,
            maxMemoryPercent,
            useAuto
        );

        // This number will allow us to calculate a more accurate ratio to apply to the tier.
        // Calculating the scaling factor for the tier using the minimum possible node size
        // results in too high a value, because the JVM occupies a smaller percentage of
        // larger nodes. For this reason we calculate based on the likely node size rather
        // than the minimum.
        long tierBasedRequiredNodeSize = NativeMemoryCalculator.calculateApproxNecessaryNodeSize(
            tierBasedMlNativeMemoryPerNodeExcludingOverhead + NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(),
            jvmSize,
            maxMemoryPercent,
            useAuto
        );

        // We make the assumption all nodes in the tier will be identical.
        long requiredTierSize = tierBasedRequiredNodeSize * numNodesPerZone * Math.max(1, numMlAvailabilityZones);
        if (nodeMlNativeMemoryRequirementExcludingOverhead > mlNativeMemoryForLargestMlNodeExcludingOverhead) {
            // This could happen if a cluster was migrated from one Cloud provider to another with smaller
            // maximum node size, and a job existed that only just fitted in the original cluster.
            logger.warn(
                "Node memory requirement (excluding per-node overhead) [{}]"
                    + " exceeds ML memory (excluding per-node overhead) available on largest possible ML node [{}]",
                nodeMlNativeMemoryRequirementExcludingOverhead,
                mlNativeMemoryForLargestMlNodeExcludingOverhead
            );
        } else {
            // If the calculation hasn't been distorted by the inconsistent input of node requirement being higher
            // than the maximum possible then we should have calculated a tier size bigger than the node size.
            assert requiredTierSize >= requiredNodeSize : "Tier should always be AT LEAST the largest node size";
        }
        // The assertion above should hold, but the Math.max below catches the case with inconsistent
        // inputs plus any bugs that weren't caught in tests.
        return MlMemoryAutoscalingCapacity.builder(
            ByteSizeValue.ofBytes(requiredNodeSize),
            ByteSizeValue.ofBytes(Math.max(requiredTierSize, requiredNodeSize))
        );
    }

    public long getTierMlNativeMemoryRequirementExcludingOverhead() {
        return tierMlNativeMemoryRequirementExcludingOverhead;
    }

    public long getNodeMlNativeMemoryRequirementExcludingOverhead() {
        return nodeMlNativeMemoryRequirementExcludingOverhead;
    }

    public Long getJvmSize() {
        return jvmSize;
    }

    @Override
    public String toString() {
        return "NativeMemoryCapacity{"
            + "total ML native bytes="
            + ByteSizeValue.ofBytes(tierMlNativeMemoryRequirementExcludingOverhead)
            + ", largest node ML native bytes="
            + ByteSizeValue.ofBytes(nodeMlNativeMemoryRequirementExcludingOverhead)
            + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NativeMemoryCapacity that = (NativeMemoryCapacity) o;
        return tierMlNativeMemoryRequirementExcludingOverhead == that.tierMlNativeMemoryRequirementExcludingOverhead
            && nodeMlNativeMemoryRequirementExcludingOverhead == that.nodeMlNativeMemoryRequirementExcludingOverhead
            && Objects.equals(jvmSize, that.jvmSize);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tierMlNativeMemoryRequirementExcludingOverhead, nodeMlNativeMemoryRequirementExcludingOverhead, jvmSize);
    }

    /**
     * The "current scale" is defined as the possible capacity of the current cluster, not
     * the sum of what's actually in use.
     * @return A {@link NativeMemoryCapacity} object where the "tier requirement" is the sum of
     *         the ML native memory allowance (less per-node overhead) on all ML nodes, the
     *         "node requirement" is the highest ML native memory allowance (less per-node overhead)
     *         across all ML nodes and the JVM size is the biggest JVM size across all ML nodes.
     */
    public static NativeMemoryCapacity currentScale(
        final List<DiscoveryNode> machineLearningNodes,
        int maxMachineMemoryPercent,
        boolean useAuto
    ) {
        long[] mlMemory = machineLearningNodes.stream()
            .mapToLong(node -> NativeMemoryCalculator.allowedBytesForMl(node, maxMachineMemoryPercent, useAuto).orElse(0L))
            // NativeMemoryCapacity is in terms of ML memory excluding the per-node overhead
            .map(mem -> Math.max(mem - NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(), 0L))
            .toArray();

        return new NativeMemoryCapacity(
            Arrays.stream(mlMemory).sum(),
            Arrays.stream(mlMemory).max().orElse(0L),
            // We assume that JVM size is universal, at least, the largest JVM indicates the largest node
            machineLearningNodes.stream()
                .map(NativeMemoryCapacity::getNodeJvmSize)
                .filter(OptionalLong::isPresent)
                .map(OptionalLong::getAsLong)
                .max(Long::compare)
                .orElse(null)
        );
    }

    static OptionalLong getNodeJvmSize(DiscoveryNode node) {
        Map<String, String> nodeAttributes = node.getAttributes();
        String valueStr = nodeAttributes.get(MachineLearning.MAX_JVM_SIZE_NODE_ATTR);
        try {
            return OptionalLong.of(Long.parseLong(valueStr));
        } catch (NumberFormatException e) {
            assert e == null : "ml.max_jvm_size should parse because we set it internally: invalid value was " + valueStr;
            logger.debug(
                "could not parse stored string value [{}] in node attribute [{}]",
                valueStr,
                MachineLearning.MAX_JVM_SIZE_NODE_ATTR
            );
        }
        return OptionalLong.empty();
    }
}
