/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.autoscaling.memory;

import co.elastic.elasticsearch.serverless.constants.ProjectType;

import org.elasticsearch.common.unit.ByteSizeUnit;

/**
 * Converts required heap memory to system memory based on the same numbers used
 * in {@code org.elasticsearch.server.cli.MachineDependentHeap}.
 *
 * TODO: merge this with MachineDependentHeap to avoid these value diverging over time.
 */
public final class HeapToSystemMemory {
    // Based on {@code org.elasticsearch.server.cli.MachineDependentHeap}
    static final long MAX_HEAP_SIZE = ByteSizeUnit.GB.toBytes(31);
    /**
     * Threshold based on {@code org.elasticsearch.server.cli.ServerlessMachineDependentHeap} where for
     * vector search project we use 1/4 memory to heap ratio for memory >= 8Gb and 1/2 otherwise.
    */
    static final long VECTOR_HEAP_THRESHOLD = ByteSizeUnit.GB.toBytes(2);

    /**
     * Estimate the system memory required for a given heap value. This is based on the reverse of the formula
     * used for calculating heap from available system memory for DATA nodes. Note that a Stateless node would
     * require at least 500MB heap (see {@link MemoryMetricsService#WORKLOAD_MEMORY_OVERHEAD}), therefore we can
     * avoid handling lower heap values and their different ratios, that are instead handled by MachineDependentHeap.
     * We use 4x heap value for the system memory for vector search projects with heap >= VECTOR_HEAP_THRESHOLD,
     * and we use 2x heap value for the system memory in all other cases.
     */
    public static long dataNode(long heapInBytes, ProjectType projectType) {
        assert heapInBytes >= MemoryMetricsService.WORKLOAD_MEMORY_OVERHEAD
            : "Stateless node heap cannot be less than the base workload memory overhead";
        long heap = Math.min(heapInBytes, MAX_HEAP_SIZE);
        if (projectType == ProjectType.ELASTICSEARCH_VECTOR && heapInBytes >= VECTOR_HEAP_THRESHOLD) {
            return heap * 4;
        }
        return heap * 2;
    }

    /**
     * Convert the required system memory based on the required heap memory of the entire tier.
     */
    public static long tier(long heapInBytes, ProjectType projectType) {
        if (projectType == ProjectType.ELASTICSEARCH_VECTOR && heapInBytes >= VECTOR_HEAP_THRESHOLD) {
            return heapInBytes * 4;
        }
        return heapInBytes * 2;
    }
}
