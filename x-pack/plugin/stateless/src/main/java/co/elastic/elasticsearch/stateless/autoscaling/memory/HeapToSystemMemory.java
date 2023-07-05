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

import org.elasticsearch.common.unit.ByteSizeUnit;

/**
 * Converts required heap memory to system memory based on the same numbers used
 * in {@code org.elasticsearch.server.cli.MachineDependentHeap}.
 * <p>
 * Note that for heap values around the threshold (400MB) the conversion between heap and system memory might result in some
 * edge cases that could potentially lead to unnecessary scaling up/down. However, this can only be an issue in case of very small nodes
 * with less than 1GB system memory. See https://github.com/elastic/elasticsearch-serverless/pull/505#issuecomment-1620536015
 * <p>
 * // TODO: Do we need to address this case?
 */
public final class HeapToSystemMemory {
    private static final long HEAP_THRESHOLD = ByteSizeUnit.MB.toBytes(400); // 400MB
    static final long MIN_HEAP_SIZE = ByteSizeUnit.MB.toBytes(128); // 128MB
    static final long MAX_HEAP_SIZE = ByteSizeUnit.GB.toBytes(31);

    /**
     * Estimate the system memory required for a given heap value. This is the reverse of the formula
     * used for calculating heap from available system memory for DATA nodes.
     * <ul>
     *  <li> Up to 400MB heap, the heap to memory ratio is 40%, therefore we'd have 2.5 times the heap value for the system memory.</li>
     *  <li> For larger heap values up to max heap size, the ratio of heap to memory is 50%, therefore we'd have 2 times the heap value
     *  for the system memory.</li>
     * </ul>
     */
    public static long dataNode(long heapInBytes) {
        long heap = Math.min(Math.max(heapInBytes, MIN_HEAP_SIZE), MAX_HEAP_SIZE);

        if (heap <= HEAP_THRESHOLD) {
            return (long) (heap * 2.5);
        }
        return heap * 2;
    }
}
