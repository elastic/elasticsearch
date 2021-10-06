/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.util.offheap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class SimpleOffHeapDecider extends AbstractLifecycleComponent implements OffHeapDecider {

    private static final Logger logger = LogManager.getLogger(SimpleOffHeapDecider.class);

    private static final float defaultMaxDirectRatio = 0.8f;
    private static final float defaultMaxDiskRatio = 0.7f;
    private static final TimeValue defaultInterval = new TimeValue(5, TimeUnit.SECONDS);

    public static final Setting<Boolean> SETTING_OFF_HEAP_ENABLED = Setting.boolSetting(
        "indices.big_arrays.off_heap.enabled", false, Setting.Property.Dynamic,  Setting.Property.NodeScope);
    public static final Setting<Float> SETTING_OFF_HEAP_MAX_DIRECT_MEMORY_RATIO = Setting.floatSetting(
        "indices.big_arrays.off_heap_max_direct_memory_ratio",
        defaultMaxDirectRatio, 0f, Setting.Property.Dynamic, Setting.Property.NodeScope);
    public static final Setting<Float> SETTING_OFF_HEAP_MAX_DISK_RATIO = Setting.floatSetting(
        "indices.big_arrays.off_heap_max_disk_ratio",
        defaultMaxDiskRatio, 0f, Setting.Property.Dynamic, Setting.Property.NodeScope);
    public static final Setting<TimeValue> SETTING_OFF_HEAP_DIRECT_MEMORY_COLLECT_INTEVAL = Setting.timeSetting(
        "indices.big_arrays.usage_collect_interval", defaultInterval, Setting.Property.NodeScope);

    private static final long MAX_DIRECT_MEMORY_SIZE_IN_BYTES = getMaxDirectMemorySizeInBytes();
    private long maxDirectMem = (long)(MAX_DIRECT_MEMORY_SIZE_IN_BYTES * defaultMaxDirectRatio);
    private float maxDiskRatio = defaultMaxDiskRatio;
    private final ThreadPool threadPool;
    private final AtomicLong usedDirectMemSizeInBytes = new AtomicLong(getCurrentNoneHeapUsed().getUsed());
    private final String localNodeId;
    private Scheduler.Cancellable task;
    private volatile DiskUsage diskUsage;
    private boolean enabled;

    public SimpleOffHeapDecider(ClusterSettings clusterSettings, ClusterInfoService clusterInfoService, ThreadPool threadPool,
                                String localNodeId) {
        this.threadPool = threadPool;
        this.localNodeId = localNodeId;
        clusterInfoService.addListener(this::updateClusterInfo);
        clusterSettings.addSettingsUpdateConsumer(SETTING_OFF_HEAP_ENABLED, this::setEnabled);
        clusterSettings.addSettingsUpdateConsumer(SETTING_OFF_HEAP_MAX_DISK_RATIO, this::setMaxDiskRatio);
        clusterSettings.addSettingsUpdateConsumer(SETTING_OFF_HEAP_MAX_DIRECT_MEMORY_RATIO, this::setMaxDirectMemRatio);
    }

    public void updateClusterInfo(ClusterInfo clusterInfo) {
        diskUsage = clusterInfo.getNodeLeastAvailableDiskUsages().get(localNodeId);
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public void setMaxDirectMemRatio(float maxDirectMemRatio) {
        this.maxDirectMem = (long)(MAX_DIRECT_MEMORY_SIZE_IN_BYTES * maxDirectMemRatio);
    }

    public void setMaxDiskRatio(float maxDiskRatio) {
        this.maxDiskRatio = maxDiskRatio;
    }

    @Override
    public Decision decide(long requireSizeInBytes) {
        Decision decision;
        if (enabled == false) {
            decision = Decision.None;
        } else if (canAllocateDirect(requireSizeInBytes)) {
            decision = Decision.Direct;
        } else if (canAllocateDisk(requireSizeInBytes)) {
            decision = Decision.Mapped;
        } else {
            decision = Decision.None;
        }
        if (logger.isDebugEnabled()) {
            logger.debug("off-heap allocation decision: " + decision);
        }
        return decision;
    }

    private static long getMaxDirectMemorySizeInBytes() {
        long configured =  getCurrentNoneHeapUsed().getMax();
        if (configured < 0) {
            logger.warn("max direct memory is not configured, use heap size instead");
            return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax(); // same as heap size
        } else {
            return configured;
        }
    }

    private static MemoryUsage getCurrentNoneHeapUsed() {
        return ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage();
    }

    private boolean canAllocateDirect(long requireSizeInBytes) {
        return (maxDirectMem > requireSizeInBytes + usedDirectMemSizeInBytes.get());
    }

    private boolean canAllocateDisk(long requireSizeInBytes) {
        if (diskUsage == null) {
            return false;
        }
        return diskUsage.getUsedBytes() + requireSizeInBytes < diskUsage.getTotalBytes() * maxDiskRatio;
    }

    @Override
    protected void doStart() {
        this.task = threadPool.scheduleWithFixedDelay(() ->
                usedDirectMemSizeInBytes.set(getCurrentNoneHeapUsed().getUsed()), defaultInterval, ThreadPool.Names.GENERIC);
    }

    @Override
    protected void doStop() {
        if (task != null && task.isCancelled() == false) {
            task.cancel();
        }
    }

    @Override
    protected void doClose() throws IOException {

    }

}
