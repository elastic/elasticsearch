/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.lucene.index;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.merge.OnGoingMerge;

import java.io.IOException;
import java.util.Collections;
import java.util.Locale;
import java.util.Set;

/**
 * An extension to the {@link ConcurrentMergeScheduler} that provides tracking on merge times, total
 * and current merges.
 */
public class TrackingConcurrentMergeScheduler extends ConcurrentMergeScheduler {

    protected final ESLogger logger;

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

    public TrackingConcurrentMergeScheduler(ESLogger logger) {
        super();
        this.logger = logger;
    }

    public long totalMerges() {
        return totalMerges.count();
    }

    public long totalMergeTime() {
        return totalMerges.sum();
    }

    public long totalMergeNumDocs() {
        return totalMergesNumDocs.count();
    }

    public long totalMergeSizeInBytes() {
        return totalMergesSizeInBytes.count();
    }

    public long currentMerges() {
        return currentMerges.count();
    }

    public long currentMergesNumDocs() {
        return currentMergesNumDocs.count();
    }

    public long currentMergesSizeInBytes() {
        return currentMergesSizeInBytes.count();
    }

    public long totalMergeStoppedTimeMillis() {
        return totalMergeStoppedTime.count();
    }

    public long totalMergeThrottledTimeMillis() {
        return totalMergeThrottledTime.count();
    }

    public Set<OnGoingMerge> onGoingMerges() {
        return readOnlyOnGoingMerges;
    }

    @Override
    protected void doMerge(IndexWriter writer, MergePolicy.OneMerge merge) throws IOException {
        int totalNumDocs = merge.totalNumDocs();
        long totalSizeInBytes = merge.totalBytesSize();
        long time = System.currentTimeMillis();
        currentMerges.inc();
        currentMergesNumDocs.inc(totalNumDocs);
        currentMergesSizeInBytes.inc(totalSizeInBytes);

        OnGoingMerge onGoingMerge = new OnGoingMerge(merge);
        onGoingMerges.add(onGoingMerge);

        if (logger.isTraceEnabled()) {
            logger.trace("merge [{}] starting..., merging [{}] segments, [{}] docs, [{}] size, into [{}] estimated_size", merge.info == null ? "_na_" : merge.info.info.name, merge.segments.size(), totalNumDocs, new ByteSizeValue(totalSizeInBytes), new ByteSizeValue(merge.estimatedMergeBytes));
        }
        try {
            beforeMerge(onGoingMerge);
            super.doMerge(writer, merge);
        } finally {
            long took = System.currentTimeMillis() - time;

            onGoingMerges.remove(onGoingMerge);
            afterMerge(onGoingMerge);

            currentMerges.dec();
            currentMergesNumDocs.dec(totalNumDocs);
            currentMergesSizeInBytes.dec(totalSizeInBytes);

            totalMergesNumDocs.inc(totalNumDocs);
            totalMergesSizeInBytes.inc(totalSizeInBytes);
            totalMerges.inc(took);

            long stoppedMS = merge.rateLimiter.getTotalStoppedNS()/1000000;
            long throttledMS = merge.rateLimiter.getTotalPausedNS()/1000000;

            totalMergeStoppedTime.inc(stoppedMS);
            totalMergeThrottledTime.inc(throttledMS);

            String message = String.format(Locale.ROOT,
                                           "merge segment [%s] done: took [%s], [%,.1f MB], [%,d docs], [%s stopped], [%s throttled], [%,.1f MB written], [%,.1f MB/sec throttle]",
                                           merge.info == null ? "_na_" : merge.info.info.name,
                                           TimeValue.timeValueMillis(took),
                                           totalSizeInBytes/1024f/1024f,
                                           totalNumDocs,
                                           TimeValue.timeValueMillis(stoppedMS),
                                           TimeValue.timeValueMillis(throttledMS),
                                           merge.rateLimiter.getTotalBytesWritten()/1024f/1024f,
                                           merge.rateLimiter.getMBPerSec());

            if (took > 20000) { // if more than 20 seconds, DEBUG log it
                logger.debug(message);
            } else if (logger.isTraceEnabled()) {
                logger.trace(message);
            }
        }
    }

    /**
     * A callback allowing for custom logic before an actual merge starts.
     */
    protected void beforeMerge(OnGoingMerge merge) {

    }

    /**
     * A callback allowing for custom logic before an actual merge starts.
     */
    protected void afterMerge(OnGoingMerge merge) {

    }

    @Override
    public MergeScheduler clone() {
        // Lucene IW makes a clone internally but since we hold on to this instance 
        // the clone will just be the identity.
        return this;
    }
}
