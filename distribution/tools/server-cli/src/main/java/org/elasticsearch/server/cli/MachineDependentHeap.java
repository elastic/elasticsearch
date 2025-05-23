/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.server.cli;

import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.NodeRoleSettings;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.MASTER_ROLE;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.ML_ROLE;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE;
import static org.elasticsearch.server.cli.JvmOption.isInitialHeapSpecified;
import static org.elasticsearch.server.cli.JvmOption.isMaxHeapSpecified;
import static org.elasticsearch.server.cli.JvmOption.isMinHeapSpecified;

/**
 * Determines optimal default heap settings based on available system memory and assigned node roles.
 */
public class MachineDependentHeap {
    protected static final long GB = 1024L * 1024L * 1024L; // 1GB
    protected static final long MAX_HEAP_SIZE = GB * 31; // 31GB
    protected static final long MIN_HEAP_SIZE = 1024 * 1024 * 128; // 128MB

    public MachineDependentHeap() {}

    /**
     * Calculate heap options.
     *
     * @param nodeSettings the settings for the node
     * @param userDefinedJvmOptions JVM arguments provided by the user
     * @return final heap options, or an empty collection if user provided heap options are to be used
     * @throws IOException if unable to load elasticsearch.yml
     */
    public final List<String> determineHeapSettings(
        Settings nodeSettings,
        SystemMemoryInfo systemMemoryInfo,
        List<String> userDefinedJvmOptions
    ) throws IOException, InterruptedException {
        // TODO: this could be more efficient, to only parse final options once
        final Map<String, JvmOption> finalJvmOptions = JvmOption.findFinalOptions(userDefinedJvmOptions);
        if (isMaxHeapSpecified(finalJvmOptions) || isMinHeapSpecified(finalJvmOptions) || isInitialHeapSpecified(finalJvmOptions)) {
            // User has explicitly set memory settings so we use those
            return Collections.emptyList();
        }

        List<DiscoveryNodeRole> roles = NodeRoleSettings.NODE_ROLES_SETTING.get(nodeSettings);
        long availableSystemMemory = systemMemoryInfo.availableSystemMemory();
        MachineNodeRole nodeRole = mapNodeRole(roles);
        return options(getHeapSizeMb(nodeSettings, nodeRole, availableSystemMemory));
    }

    protected int getHeapSizeMb(Settings nodeSettings, MachineNodeRole role, long availableMemory) {
        return switch (role) {
            /*
             * Master-only node.
             *
             * <p>Heap is computed as 60% of total system memory up to a maximum of 31 gigabytes.
             */
            case MASTER_ONLY -> mb(min((long) (availableMemory * .6), MAX_HEAP_SIZE));
            /*
             * Machine learning only node.
             *
             * <p>Heap is computed as:
             * <ul>
             *     <li>40% of total system memory when total system memory 16 gigabytes or less.</li>
             *     <li>40% of the first 16 gigabytes plus 10% of memory above that when total system memory is more than 16 gigabytes.</li>
             *     <li>The absolute maximum heap size is 31 gigabytes.</li>
             * </ul>
             *
             * In all cases the result is rounded down to the next whole multiple of 4 megabytes.
             * The reason for doing this is that Java will round requested heap sizes to a multiple
             * of 4 megabytes (certainly versions 11 to 18 do this), so by doing this ourselves we
             * are more likely to actually get the amount we request. This is worthwhile for ML where
             * the ML autoscaling code needs to be able to calculate the JVM size for different sizes
             * of ML node, and if Java is also rounding then this causes a discrepancy. It's possible
             * that a future version of Java could round to an even bigger number of megabytes, which
             * would cause a discrepancy for people using that version of Java. But there's no harm
             * in a bit of extra rounding here - it can only reduce discrepancies.
             *
             * If this formula is changed then corresponding changes must be made to the {@code NativeMemoryCalculator} and
             * {@code MlAutoscalingDeciderServiceTests} classes in the ML plugin code. Failure to keep the logic synchronized
             * could result in ML processes crashing with OOM errors or repeated autoscaling up and down.
             */
            case ML_ONLY -> {
                if (availableMemory <= (GB * 16)) {
                    yield mb((long) (availableMemory * .4), 4);
                } else {
                    yield mb((long) min((GB * 16) * .4 + (availableMemory - GB * 16) * .1, MAX_HEAP_SIZE), 4);
                }
            }
            /*
             * Data node. Essentially any node that isn't a master or ML only node.
             *
             * <p>Heap is computed as:
             * <ul>
             *     <li>40% of total system memory when less than 1 gigabyte with a minimum of 128 megabytes.</li>
             *     <li>50% of total system memory when greater than 1 gigabyte up to a maximum of 31 gigabytes.</li>
             * </ul>
             */
            case DATA -> {
                if (availableMemory < GB) {
                    yield mb(max((long) (availableMemory * .4), MIN_HEAP_SIZE));
                } else {
                    yield mb(min((long) (availableMemory * .5), MAX_HEAP_SIZE));
                }
            }
        };
    }

    protected static int mb(long bytes) {
        return (int) (bytes / (1024 * 1024));
    }

    protected static int mb(long bytes, int toLowerMultipleOfMb) {
        return toLowerMultipleOfMb * (int) (bytes / (1024 * 1024 * toLowerMultipleOfMb));
    }

    private static MachineNodeRole mapNodeRole(List<DiscoveryNodeRole> roles) {
        if (roles.isEmpty()) {
            // If roles are missing or empty (coordinating node) assume defaults and consider this a data node
            return MachineNodeRole.DATA;
        } else if (containsOnly(roles, MASTER_ROLE)) {
            return MachineNodeRole.MASTER_ONLY;
        } else if (roles.contains(ML_ROLE) && containsOnly(roles, ML_ROLE, REMOTE_CLUSTER_CLIENT_ROLE)) {
            return MachineNodeRole.ML_ONLY;
        } else {
            return MachineNodeRole.DATA;
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> boolean containsOnly(Collection<T> collection, T... items) {
        return Arrays.asList(items).containsAll(collection);
    }

    private static List<String> options(int heapSize) {
        return List.of("-Xms" + heapSize + "m", "-Xmx" + heapSize + "m");
    }

    protected enum MachineNodeRole {
        MASTER_ONLY,
        ML_ONLY,
        DATA;
    }
}
