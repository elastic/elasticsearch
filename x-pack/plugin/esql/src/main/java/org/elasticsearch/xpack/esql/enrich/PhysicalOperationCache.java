/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transports;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner.PhysicalOperation;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Thread-safe cache for PhysicalOperation instances by lookupSessionId.
 * This cache allows reusing PhysicalOperation instances across multiple requests
 * with the same lookupSessionId, avoiding redundant planning work.
 * <p>
 * The cache automatically removes entries that have been inactive for longer than
 * the configured inactive interval using a background reaper thread.
 */
public class PhysicalOperationCache {
    private static final Logger logger = LogManager.getLogger(PhysicalOperationCache.class);

    /**
     * The time interval for a cached PhysicalOperation to be considered inactive and subsequently
     * removed from the cache if it hasn't been accessed.
     */
    public static final String INACTIVE_INTERVAL_SETTING = "esql.lookup.physical_operation_cache_inactive_interval";
    public static final TimeValue INACTIVE_INTERVAL_DEFAULT = TimeValue.timeValueMinutes(5);

    public static final Setting<TimeValue> INACTIVE_INTERVAL_SETTING_CONFIG = Setting.timeSetting(
        INACTIVE_INTERVAL_SETTING,
        INACTIVE_INTERVAL_DEFAULT,
        Setting.Property.NodeScope
    );

    private final Map<String, CacheEntry> cache = new ConcurrentHashMap<>();
    private final ThreadPool threadPool;
    private final Executor executor;
    private final TimeValue inactiveInterval;

    /**
     * Cache entry that tracks both the PhysicalOperation and its last access time.
     */
    private static class CacheEntry {
        final PhysicalOperation physicalOperation;
        final AtomicLong lastAccessTimeInMillis;

        CacheEntry(PhysicalOperation physicalOperation, long initialAccessTimeInMillis) {
            this.physicalOperation = physicalOperation;
            this.lastAccessTimeInMillis = new AtomicLong(initialAccessTimeInMillis);
        }

        long lastAccessTimeInMillis() {
            return lastAccessTimeInMillis.get();
        }

        void updateAccessTime(long accessTimeInMillis) {
            lastAccessTimeInMillis.accumulateAndGet(accessTimeInMillis, Math::max);
        }
    }

    /**
     * Creates a new PhysicalOperationCache.
     *
     * @param settings The settings to read the inactive interval from
     * @param threadPool The thread pool to schedule the reaper task
     * @param executorName The executor name to run the reaper task on
     */
    public PhysicalOperationCache(Settings settings, ThreadPool threadPool, String executorName) {
        this.threadPool = threadPool;
        this.executor = threadPool.executor(executorName);
        this.inactiveInterval = INACTIVE_INTERVAL_SETTING_CONFIG.get(settings);
        // Run the reaper every half of the inactive interval
        this.threadPool.scheduleWithFixedDelay(
            new InactiveEntriesReaper(),
            TimeValue.timeValueMillis(Math.max(1, inactiveInterval.millis() / 2)),
            executor
        );
    }

    /**
     * Returns the cached PhysicalOperation for the given sessionId, or computes
     * it using the supplier function if not present. Updates the access time on access.
     *
     * @param sessionId The lookup session ID
     * @param supplier Function that computes the PhysicalOperation if not cached
     * @return The cached or newly computed PhysicalOperation
     */
    public PhysicalOperation getOrCompute(String sessionId, Function<String, PhysicalOperation> supplier) {
        final long nowInMillis = threadPool.relativeTimeInMillis();
        CacheEntry entry = cache.computeIfAbsent(sessionId, id -> {
            PhysicalOperation physicalOperation = supplier.apply(id);
            return new CacheEntry(physicalOperation, nowInMillis);
        });
        // Update access time even if entry was already present
        entry.updateAccessTime(nowInMillis);
        return entry.physicalOperation;
    }

    /**
     * Removes the cached PhysicalOperation for the given sessionId.
     *
     * @param sessionId The lookup session ID to remove
     * @return The removed PhysicalOperation, or null if not present
     */
    public PhysicalOperation remove(String sessionId) {
        CacheEntry entry = cache.remove(sessionId);
        return entry != null ? entry.physicalOperation : null;
    }

    /**
     * Clears all cached PhysicalOperation instances.
     */
    public void clear() {
        cache.clear();
    }

    /**
     * Returns the number of cached PhysicalOperation instances.
     *
     * @return The cache size
     */
    public int size() {
        return cache.size();
    }

    /**
     * Reaper that removes inactive cache entries.
     */
    private final class InactiveEntriesReaper extends AbstractRunnable {
        @Override
        protected void doRun() {
            assert Transports.assertNotTransportThread("reaping inactive cache entries can be expensive");
            assert ThreadPool.assertNotScheduleThread("reaping inactive cache entries can be expensive");
            logger.debug("start removing inactive physical operation cache entries");
            final long nowInMillis = threadPool.relativeTimeInMillis();
            for (Map.Entry<String, CacheEntry> e : cache.entrySet()) {
                CacheEntry entry = e.getValue();
                long elapsedInMillis = nowInMillis - entry.lastAccessTimeInMillis();
                if (elapsedInMillis > inactiveInterval.millis()) {
                    TimeValue elapsedTime = TimeValue.timeValueMillis(elapsedInMillis);
                    logger.debug("removed physical operation cache entry {} inactive for {}", e.getKey(), elapsedTime);
                    cache.remove(e.getKey());
                }
            }
        }

        @Override
        public void onFailure(Exception e) {
            logger.error("unexpected error when removing inactive physical operation cache entries", e);
            assert false : e;
        }

        @Override
        public void onRejection(Exception e) {
            if (e instanceof EsRejectedExecutionException esre && esre.isExecutorShutdown()) {
                logger.debug("rejected execution when removing inactive physical operation cache entries");
            } else {
                onFailure(e);
            }
        }

        @Override
        public boolean isForceExecution() {
            // mustn't reject this task even if the queue is full
            return true;
        }
    }
}
