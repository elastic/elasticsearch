/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;

/**
 * The merge scheduler (<code>ConcurrentMergeScheduler</code>) controls the execution of
 * merge operations once they are needed (according to the merge policy).  Merges
 * run in separate threads, and when the maximum number of threads is reached,
 * further merges will wait until a merge thread becomes available.
 *
 * <p>The merge scheduler supports the following <b>dynamic</b> settings:
 *
 * <ul>
 * <li> <code>index.merge.scheduler.max_thread_count</code>:
 *
 *     The maximum number of threads that may be merging at once. Defaults to
 *     half of {@link Runtime#availableProcessors()}, with a minimum of 1,
 *     which works well for a good non-volatile memory express (NVMe) solid-state-disk (SSD).
 *     If your index is on spinning platter drives instead, decrease this to 1.
 *     This default is independent of {@code node.processors}.
 *
 * <li><code>index.merge.scheduler.auto_throttle</code>:
 *
 *     If this is true (the default), then the merge scheduler will rate-limit IO
 *     (writes) for merges to an adaptive value depending on how many merges are
 *     requested over time.  An application with a low indexing rate that
 *     unluckily suddenly requires a large merge will see that merge aggressively
 *     throttled, while an application doing heavy indexing will see the throttle
 *     move higher to allow merges to keep up with ongoing indexing.
 * </ul>
 */
public final class MergeSchedulerConfig {

    public static final Setting<Integer> MAX_THREAD_COUNT_SETTING = new Setting<>(
        "index.merge.scheduler.max_thread_count",
        (s) -> Integer.toString(Math.max(1, Runtime.getRuntime().availableProcessors() / 2)),
        (s) -> Setting.parseInt(s, 1, "index.merge.scheduler.max_thread_count"),
        Property.Dynamic,
        Property.IndexScope
    );
    public static final Setting<Integer> MAX_MERGE_COUNT_SETTING = new Setting<>(
        "index.merge.scheduler.max_merge_count",
        (s) -> Integer.toString(MAX_THREAD_COUNT_SETTING.get(s) + 5),
        (s) -> Setting.parseInt(s, 1, "index.merge.scheduler.max_merge_count"),
        Property.Dynamic,
        Property.IndexScope
    );
    public static final Setting<Boolean> AUTO_THROTTLE_SETTING = Setting.boolSetting(
        "index.merge.scheduler.auto_throttle",
        true,
        Property.Dynamic,
        Property.IndexScope
    );

    /**
     * Snapshot of the last applied merge-scheduler thread/merge counts.
     */
    record AppliedCounts(int requestedMaxThreadCount, int maxThreadCount, int maxMergeCount) {
        boolean isMaxThreadCountClamped() {
            return maxThreadCount < requestedMaxThreadCount;
        }
    }

    private volatile boolean autoThrottle;
    private volatile AppliedCounts appliedCounts;

    MergeSchedulerConfig(IndexSettings indexSettings) {
        setMaxThreadAndMergeCount(indexSettings.getValue(MAX_THREAD_COUNT_SETTING), indexSettings.getValue(MAX_MERGE_COUNT_SETTING));
        this.autoThrottle = indexSettings.getValue(AUTO_THROTTLE_SETTING);
    }

    /**
     * Returns <code>true</code> iff auto throttle is enabled.
     *
     * @see ConcurrentMergeScheduler#enableAutoIOThrottle()
     */
    public boolean isAutoThrottle() {
        return autoThrottle;
    }

    /**
     * Enables / disables auto throttling on the {@link ConcurrentMergeScheduler}
     */
    void setAutoThrottle(boolean autoThrottle) {
        this.autoThrottle = autoThrottle;
    }

    /**
     * Returns {@code maxThreadCount}.
     */
    public int getMaxThreadCount() {
        return appliedCounts.maxThreadCount();
    }

    /**
     * Returns {@code maxMergeCount}.
     */
    public int getMaxMergeCount() {
        return appliedCounts.maxMergeCount();
    }

    /**
     * Returns the last applied counts snapshot.
     */
    AppliedCounts getAppliedCounts() {
        return appliedCounts;
    }

    /**
     * Expert: set the maximum number of merge threads and simultaneous merges allowed.
     * {@code maxThreadCount} is capped at {@code maxMergeCount} so cluster-state application
     * cannot fail when a node-local default exceeds a published setting. Callers that own a
     * live index should log when the applied counts are clamped; throwaway
     * {@link IndexSettings} constructions must stay silent.
     */
    void setMaxThreadAndMergeCount(int maxThreadCount, int maxMergeCount) {
        if (maxThreadCount < 1) {
            throw new IllegalArgumentException("maxThreadCount should be at least 1");
        }
        if (maxMergeCount < 1) {
            throw new IllegalArgumentException("maxMergeCount should be at least 1");
        }
        this.appliedCounts = new AppliedCounts(maxThreadCount, Math.min(maxThreadCount, maxMergeCount), maxMergeCount);
    }

    /**
     * Logs when the current applied counts had {@code max_thread_count} clamped to {@code max_merge_count}.
     */
    void warnIfMaxThreadCountClamped(Logger logger) {
        final AppliedCounts counts = this.appliedCounts;
        if (counts.isMaxThreadCountClamped() == false) {
            return;
        }
        logger.warn(
            "[{}] (= {}) exceeds [{}] (= {}); using {}",
            MAX_THREAD_COUNT_SETTING.getKey(),
            counts.requestedMaxThreadCount(),
            MAX_MERGE_COUNT_SETTING.getKey(),
            counts.maxMergeCount(),
            counts.maxThreadCount()
        );
    }
}
