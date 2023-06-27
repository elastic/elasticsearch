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

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.stateless.autoscaling.search;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.LongSupplier;

/**
 * This class is responsible for computing disk related search tier autoscaling metrics
 * as well as accumulating required raw data for the computation.
 */
public class AutoscalingDiskSizeMetricsService {

    private final ClusterSettings clusterSettings;
    private final LongSupplier currentTimeMillsSupplier;

    private final ConcurrentMap<SegmentKey, SegmentStats> segmentStats = new ConcurrentHashMap<>();
    private long interactiveDataAge = TimeValue.timeValueDays(7).millis();// TODO ES-6224 make it a cluster setting

    /**
     * Represents the maxTimestamp value when corresponding index does not have @timestamp field
     */
    public static final long NO_TIMESTAMP = -1L;

    public AutoscalingDiskSizeMetricsService(ClusterSettings clusterSettings, ThreadPool threadPool) {
        this(clusterSettings, threadPool::relativeTimeInMillis);
    }

    public AutoscalingDiskSizeMetricsService(ClusterSettings clusterSettings, LongSupplier currentTimeMillsSupplier) {
        this.clusterSettings = clusterSettings;
        this.currentTimeMillsSupplier = currentTimeMillsSupplier;
    }

    public void onSegmentCreated(ShardId shardId, String segmentName, long size, long maxTimestamp) {
        segmentStats.put(new SegmentKey(shardId, segmentName), new SegmentStats(size, maxTimestamp));
    }

    public void onSegmentDeleted(ShardId shardId, String segmentName) {
        segmentStats.remove(new SegmentKey(shardId, segmentName));
    }

    public void onShardDeleted(ShardId shardId) {
        segmentStats.keySet().removeIf(key -> Objects.equals(key.shardId(), shardId));
    }

    public void onNoLongerMaster() {
        segmentStats.clear();
    }

    public DiskSizeMetrics getDiskSizeMetrics() {
        long currentTimeMillis = currentTimeMillsSupplier.getAsLong();
        long maxInteractiveDataSize = 0L;
        long totalInteractiveDataSize = 0L;
        long totalNonInteractiveDataSize = 0L;

        for (SegmentStats stats : segmentStats.values()) {
            if (isInteractive(stats.maxTimestamp, currentTimeMillis)) {
                maxInteractiveDataSize = Math.max(maxInteractiveDataSize, stats.size);
                totalInteractiveDataSize += stats.size;
            } else {
                totalNonInteractiveDataSize += stats.size;
            }
        }

        return new DiskSizeMetrics(true, maxInteractiveDataSize, totalInteractiveDataSize, totalNonInteractiveDataSize);
    }

    private boolean isInteractive(long timestamp, long currentTimeMillis) {
        return timestamp == NO_TIMESTAMP || (currentTimeMillis - timestamp) <= interactiveDataAge;
    }

    /**
     * @param isComplete is {@code ture} when all shard sizes are considered or {@code false} otherwise
     */
    public record DiskSizeMetrics(
        boolean isComplete,
        long maxInteractiveDataSize,
        long totalInteractiveDataSize,
        long totalNonInteractiveDataSize
    ) {}

    private record SegmentKey(ShardId shardId, String name) {}

    /**
     * @param size represents size of the segment
     * @param maxTimestamp represent the newest doc in the segment or {@code -1} if the corresponding index does not have @timestamp field
     */
    private record SegmentStats(long size, long maxTimestamp) {}
}
