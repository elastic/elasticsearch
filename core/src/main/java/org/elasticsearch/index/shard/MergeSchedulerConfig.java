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

package org.elasticsearch.index.shard;

import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;

/**
 *
 */
public final class MergeSchedulerConfig {

    public static final String MAX_THREAD_COUNT = "index.merge.scheduler.max_thread_count";
    public static final String MAX_MERGE_COUNT = "index.merge.scheduler.max_merge_count";
    public static final String AUTO_THROTTLE = "index.merge.scheduler.auto_throttle";
    public static final String NOTIFY_ON_MERGE_FAILURE = "index.merge.scheduler.notify_on_failure"; // why would we not wanna do this?

    private volatile boolean autoThrottle;
    private volatile int maxThreadCount;
    private volatile int maxMergeCount;
    private final boolean notifyOnMergeFailure;

    public MergeSchedulerConfig(Settings indexSettings) {
        maxThreadCount = indexSettings.getAsInt(MAX_THREAD_COUNT, Math.max(1, Math.min(4, EsExecutors.boundedNumberOfProcessors(indexSettings) / 2)));
        maxMergeCount = indexSettings.getAsInt(MAX_MERGE_COUNT, maxThreadCount + 5);
        this.autoThrottle = indexSettings.getAsBoolean(AUTO_THROTTLE, true);
        notifyOnMergeFailure = indexSettings.getAsBoolean(NOTIFY_ON_MERGE_FAILURE, true);
    }

    /**
     * Returns <code>true</code> iff auto throttle is enabled.
     * @see ConcurrentMergeScheduler#enableAutoIOThrottle()
     */
    public boolean isAutoThrottle() {
        return autoThrottle;
    }

    /**
     * Enables / disables auto throttling on the {@link ConcurrentMergeScheduler}
     */
    public void setAutoThrottle(boolean autoThrottle) {
        this.autoThrottle = autoThrottle;
    }

    /**
     * Returns {@code maxThreadCount}.
     */
    public int getMaxThreadCount() {
        return maxThreadCount;
    }

    /**
     * Expert: directly set the maximum number of merge threads and
     * simultaneous merges allowed.
     */
    public void setMaxThreadCount(int maxThreadCount) {
        this.maxThreadCount = maxThreadCount;
    }

    /**
     * Returns {@code maxMergeCount}.
     */
    public int getMaxMergeCount() {
        return maxMergeCount;
    }

    /**
     *
     * Expert: set the maximum number of simultaneous merges allowed.
     */
    public void setMaxMergeCount(int maxMergeCount) {
        this.maxMergeCount = maxMergeCount;
    }

    /**
     * Returns <code>true</code> iff we fail the engine on a merge failure. Default is <code>true</code>
     */
    public boolean isNotifyOnMergeFailure() {
        return notifyOnMergeFailure;
    }
}
