/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.monitor.memory.alpha;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.SizeUnit;
import org.elasticsearch.common.unit.SizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.indices.IndicesMemoryCleaner;
import org.elasticsearch.monitor.memory.MemoryMonitor;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.common.unit.TimeValue.*;

/**
 * @author kimchy (shay.banon)
 */
public class AlphaMemoryMonitor extends AbstractLifecycleComponent<MemoryMonitor> implements MemoryMonitor {

    private final double upperMemoryThreshold;

    private final double lowerMemoryThreshold;

    private final TimeValue interval;

    private final int fullThreshold;

    private final int cleanThreshold;

    private final SizeValue minimumFlushableSizeToClean;

    private final int translogNumberOfOperationsThreshold;

    private final ThreadPool threadPool;

    private final IndicesMemoryCleaner indicesMemoryCleaner;

    private final Runtime runtime;

    private final SizeValue maxMemory;

    private final SizeValue totalMemory;

    private volatile ScheduledFuture scheduledFuture;

    private AtomicLong totalCleans = new AtomicLong();
    private AtomicLong totalFull = new AtomicLong();

    @Inject public AlphaMemoryMonitor(Settings settings, ThreadPool threadPool, IndicesMemoryCleaner indicesMemoryCleaner) {
        super(settings);
        this.threadPool = threadPool;
        this.indicesMemoryCleaner = indicesMemoryCleaner;

        this.upperMemoryThreshold = componentSettings.getAsDouble("upper_memory_threshold", 0.8);
        this.lowerMemoryThreshold = componentSettings.getAsDouble("lower_memory_threshold", 0.5);
        this.interval = componentSettings.getAsTime("interval", timeValueMillis(500));
        this.fullThreshold = componentSettings.getAsInt("full_threshold", 2);
        this.cleanThreshold = componentSettings.getAsInt("clean_threshold", 10);
        this.minimumFlushableSizeToClean = componentSettings.getAsSize("minimum_flushable_size_to_clean", new SizeValue(5, SizeUnit.MB));
        this.translogNumberOfOperationsThreshold = componentSettings.getAsInt("translog_number_of_operations_threshold", 5000);

        logger.debug("interval [" + interval + "], upper_memory_threshold [" + upperMemoryThreshold + "], lower_memory_threshold [" + lowerMemoryThreshold + "], translog_number_of_operations_threshold [" + translogNumberOfOperationsThreshold + "]");

        this.runtime = Runtime.getRuntime();
        this.maxMemory = new SizeValue(runtime.maxMemory());
        this.totalMemory = maxMemory.bytes() == runtime.totalMemory() ? new SizeValue(runtime.totalMemory()) : null; // Xmx==Xms when the JVM was started.
    }

    @Override protected void doStart() throws ElasticSearchException {
        scheduledFuture = threadPool.scheduleWithFixedDelay(new MemoryCleaner(), interval);
    }

    @Override protected void doStop() throws ElasticSearchException {
        scheduledFuture.cancel(true);
    }

    @Override protected void doClose() throws ElasticSearchException {
    }

    private long freeMemory() {
        return runtime.freeMemory();
    }

    private long totalMemory() {
        return totalMemory == null ? runtime.totalMemory() : totalMemory.bytes();
    }

    private class MemoryCleaner implements Runnable {

        private int fullCounter;

        private boolean performedClean;

        private int cleanCounter;

        @Override public void run() {
            try {
                // clear unreferenced in the cache
                indicesMemoryCleaner.cacheClearUnreferenced();

                // try and clean translog based on a threshold, since we don't want to get a very large transaction log
                // which means recovery it will take a long time (since the target re-index all this data)
                IndicesMemoryCleaner.TranslogCleanResult translogCleanResult = indicesMemoryCleaner.cleanTranslog(translogNumberOfOperationsThreshold);
                if (translogCleanResult.cleanedShards() > 0) {
                    long totalClean = totalCleans.incrementAndGet();
                    logger.debug("[" + totalClean + "] [Translog] " + translogCleanResult);
                }

                // the logic is simple, if the used memory is above the upper threshold, we need to clean
                // we clean down as much as we can to down to the lower threshold

                // in order not to get trashing, we only perform a clean after another clean if a the clean counter
                // has expired.

                // we also do the same for GC invocations

                long upperMemory = maxMemory.bytes();
                long totalMemory = totalMemory();
                long usedMemory = totalMemory - freeMemory();
                long upperThresholdMemory = (long) (upperMemory * upperMemoryThreshold);

                if (usedMemory - upperThresholdMemory <= 0) {
                    fullCounter = 0;
                    performedClean = false;
                    cleanCounter = 0;
                    return;
                }

                if (performedClean) {
                    if (++cleanCounter < cleanThreshold) {
                        return;
                    }
                }


                long lowerThresholdMemory = (long) (upperMemory * lowerMemoryThreshold);
                long memoryToClean = usedMemory - lowerThresholdMemory;

                if (fullCounter++ >= fullThreshold) {
                    long total = totalFull.incrementAndGet();
                    if (logger.isInfoEnabled()) {
                        StringBuilder sb = new StringBuilder();
                        sb.append('[').append(total).append("] ");
                        sb.append("[Full    ] Ran after [").append(fullThreshold).append("] consecutive clean swipes");
                        sb.append(", memory_to_clean [").append(new SizeValue(memoryToClean)).append(']');
                        sb.append(", lower_memory_threshold [").append(new SizeValue(lowerThresholdMemory)).append(']');
                        sb.append(", upper_memory_threshold [").append(new SizeValue(upperThresholdMemory)).append(']');
                        sb.append(", used_memory [").append(new SizeValue(usedMemory)).append(']');
                        sb.append(", total_memory[").append(new SizeValue(totalMemory)).append(']');
                        sb.append(", max_memory[").append(maxMemory).append(']');
                        logger.info(sb.toString());
                    }
                    indicesMemoryCleaner.cacheClear();
                    // TODO this ends up doing a flush with "true", basically, at the end, replacing the IndexWriter, might not be needed with Lucene 3.0.2.
                    indicesMemoryCleaner.fullMemoryClean();
                    // don't clean thread locals, let GC clean them (so we won't run into visibility issues)
                    // ThreadLocals.clearReferencesThreadLocals();
                    fullCounter = 0;
                } else {
                    long totalClean = totalCleans.incrementAndGet();
                    if (logger.isDebugEnabled()) {
                        StringBuilder sb = new StringBuilder();
                        sb.append('[').append(totalClean).append("] ");
                        sb.append("[Cleaning] memory_to_clean [").append(new SizeValue(memoryToClean)).append(']');
                        sb.append(", lower_memory_threshold [").append(new SizeValue(lowerThresholdMemory)).append(']');
                        sb.append(", upper_memory_threshold [").append(new SizeValue(upperThresholdMemory)).append(']');
                        sb.append(", used_memory [").append(new SizeValue(usedMemory)).append(']');
                        sb.append(", total_memory[").append(new SizeValue(totalMemory)).append(']');
                        sb.append(", max_memory[").append(maxMemory).append(']');
                        logger.debug(sb.toString());
                    }

                    IndicesMemoryCleaner.MemoryCleanResult memoryCleanResult = indicesMemoryCleaner.cleanMemory(memoryToClean, minimumFlushableSizeToClean);
                    boolean forceClean = false;
                    if (memoryCleanResult.cleaned().bytes() < memoryToClean && (fullCounter > (fullThreshold / 2))) {
                        forceClean = true;
                        indicesMemoryCleaner.forceCleanMemory(memoryCleanResult.shardsCleaned());
                    }

                    if (logger.isDebugEnabled()) {
                        logger.debug("[" + totalClean + "] [Cleaned ] force_clean [" + forceClean + "], " + memoryCleanResult);
                    }
                }

                performedClean = true;
                cleanCounter = 0;
            } catch (Exception e) {
                logger.info("Failed to run memory monitor", e);
            }
        }
    }
}