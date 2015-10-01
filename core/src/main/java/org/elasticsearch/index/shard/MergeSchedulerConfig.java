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
 *     <code>Math.max(1, Math.min(4, Runtime.getRuntime().availableProcessors() / 2))</code>
 *     which works well for a good solid-state-disk (SSD).  If your index is on
 *     spinning platter drives instead, decrease this to 1.
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
