/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.MergePolicy;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.merge.OnGoingMerge;

import java.util.Collections;
import java.util.Locale;
import java.util.Set;
import java.util.function.DoubleSupplier;

public class MergeTracking {

    protected final Logger logger;
    private final DoubleSupplier mbPerSecAutoThrottle;

    private final MeanMetric totalMerges = new MeanMetric();
    private final CounterMetric totalMergesNumDocs = new CounterMetric();
    private final CounterMetric totalMergesSizeInBytes = new CounterMetric();
    private final CounterMetric currentMerges = new CounterMetric();
    private final CounterMetric currentMergesNumDocs = new CounterMetric();
    private final CounterMetric currentMergesSizeInBytes = new CounterMetric();
    private final CounterMetric totalMergeStoppedTime = new CounterMetric();
    private final CounterMetric totalMergeThrottledTime = new CounterMetric();

    private final Set<OnGoingMerge> onGoingMerges = ConcurrentCollections.newConcurrentSet();
    private final Set<OnGoingMerge> readOnlyOnGoingMerges = Collections.unmodifiableSet(onGoingMerges);

    public MergeTracking(Logger logger, DoubleSupplier mbPerSecAutoThrottle) {
        this.logger = logger;
        this.mbPerSecAutoThrottle = mbPerSecAutoThrottle;
    }

    public Set<OnGoingMerge> onGoingMerges() {
        return readOnlyOnGoingMerges;
    }

    public void mergeStarted(OnGoingMerge onGoingMerge) {
        MergePolicy.OneMerge merge = onGoingMerge.getMerge();
        int totalNumDocs = merge.totalNumDocs();
        long totalSizeInBytes = merge.totalBytesSize();
        currentMerges.inc();
        currentMergesNumDocs.inc(totalNumDocs);
        currentMergesSizeInBytes.inc(totalSizeInBytes);
        onGoingMerges.add(onGoingMerge);

        if (logger.isTraceEnabled()) {
            logger.trace(
                "merge [{}] starting: merging [{}] segments, [{}] docs, [{}] size, into [{}] estimated_size",
                onGoingMerge.getId(),
                merge.segments.size(),
                totalNumDocs,
                ByteSizeValue.ofBytes(totalSizeInBytes),
                ByteSizeValue.ofBytes(merge.estimatedMergeBytes)
            );
        }
    }

    public void mergeFinished(final MergePolicy.OneMerge merge, final OnGoingMerge onGoingMerge, long tookMS) {
        int totalNumDocs = merge.totalNumDocs();
        long totalSizeInBytes = merge.totalBytesSize();

        onGoingMerges.remove(onGoingMerge);

        currentMerges.dec();
        currentMergesNumDocs.dec(totalNumDocs);
        currentMergesSizeInBytes.dec(totalSizeInBytes);

        totalMergesNumDocs.inc(totalNumDocs);
        totalMergesSizeInBytes.inc(totalSizeInBytes);
        totalMerges.inc(tookMS);
        long stoppedMS = TimeValue.nsecToMSec(
            merge.getMergeProgress().getPauseTimes().get(MergePolicy.OneMergeProgress.PauseReason.STOPPED)
        );
        long throttledMS = TimeValue.nsecToMSec(
            merge.getMergeProgress().getPauseTimes().get(MergePolicy.OneMergeProgress.PauseReason.PAUSED)
        );
        totalMergeStoppedTime.inc(stoppedMS);
        totalMergeThrottledTime.inc(throttledMS);

        String message = String.format(
            Locale.ROOT,
            "merge [%s] segment [%s] done: took [%s], [%s], [%,d] docs, [%s] stopped, [%s] throttled",
            onGoingMerge.getId(),
            getSegmentName(merge),
            TimeValue.timeValueMillis(tookMS),
            ByteSizeValue.ofBytes(totalSizeInBytes),
            totalNumDocs,
            TimeValue.timeValueMillis(stoppedMS),
            TimeValue.timeValueMillis(throttledMS)
        );

        if (tookMS > 20000) { // if more than 20 seconds, DEBUG log it
            logger.debug("{}", message);
        } else if (logger.isTraceEnabled()) {
            logger.trace("{}", message);
        }
    }

    public MergeStats stats() {
        final MergeStats mergeStats = new MergeStats();
        mergeStats.add(
            totalMerges.count(),
            totalMerges.sum(),
            totalMergesNumDocs.count(),
            totalMergesSizeInBytes.count(),
            currentMerges.count(),
            currentMergesNumDocs.count(),
            currentMergesSizeInBytes.count(),
            totalMergeStoppedTime.count(),
            totalMergeThrottledTime.count(),
            mbPerSecAutoThrottle.getAsDouble()
        );
        return mergeStats;
    }

    private static String getSegmentName(MergePolicy.OneMerge merge) {
        return merge.getMergeInfo() != null ? merge.getMergeInfo().info.name : "_na_";
    }
}
