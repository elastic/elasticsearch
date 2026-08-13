/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.breaker;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.monitor.os.OsProbe;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.OptionalLong;

/**
 * Periodically samples the cgroup memory usage so that we can gate new native memory allocations
 * when usage exceeds a configurable high-watermark, resuming once it drops below the watermark
 * minus a fixed hysteresis band.
 *
 * <p>This is the "measured backstop" complementing the {@link NativeMemoryCircuitBreakerService}
 * accounting-based limit. Unlike the circuit breaker, which tracks only allocations explicitly
 * charged to it, this class reads actual process RSS from the cgroup filesystem. That makes it
 * the only mechanism that can see native memory from compression libraries and other consumers
 * that are never accounted through a circuit breaker.
 *
 * <p>When no cgroup limit is available, the backstop stays inactive: absent a hard limit there
 * is no measured constraint to enforce, and the absence of a* signal must not be interpreted as
 * no headroom.
 */
public class NativeMemoryCgroupBackstop extends AbstractLifecycleComponent {

    private static final Logger logger = LogManager.getLogger(NativeMemoryCgroupBackstop.class);

    /**
     * The cgroup memory usage percentage at which new native memory allocations are refused.
     * Defaults to 85 — leaves headroom for ongoing operation teardown and compression
     * buffers that transiently inflate RSS before the OOM killer fires.
     */
    public static final Setting<Integer> HIGH_WATERMARK_SETTING = Setting.intSetting(
        "indices.breaker.native_memory.cgroup_high_watermark_percent",
        85,
        50,
        99,
        Property.Dynamic,
        Property.NodeScope
    );

    /**
     * How often cgroup memory usage is sampled. Not dynamic; changing it requires a
     * node restart. Five seconds provides a coarse but sufficient signal for admission
     * control without meaningfully impacting the cgroup filesystem.
     */
    public static final Setting<TimeValue> POLL_INTERVAL_SETTING = Setting.timeSetting(
        "indices.breaker.native_memory.cgroup_poll_interval",
        TimeValue.timeValueSeconds(5),
        TimeValue.timeValueSeconds(1),
        Property.NodeScope
    );

    /**
     * Fixed hysteresis gap below the high watermark at which allocations are admitted again.
     * A node hovering at the watermark would otherwise flip between refusing and accepting
     * on every poll cycle; 5% of hysteresis prevents that oscillation.
     */
    static final int HYSTERESIS_PERCENT = 5;

    private final ThreadPool threadPool;
    private final TimeValue pollInterval;
    private volatile int highWatermark;
    private volatile boolean refusing = false;
    private volatile Scheduler.Cancellable cancellable;

    public NativeMemoryCgroupBackstop(Settings settings, ClusterSettings clusterSettings, ThreadPool threadPool) {
        this.threadPool = threadPool;
        this.pollInterval = POLL_INTERVAL_SETTING.get(settings);
        this.highWatermark = HIGH_WATERMARK_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(HIGH_WATERMARK_SETTING, v -> this.highWatermark = v);
    }

    @Override
    protected void doStart() {
        if (OsProbe.getInstance().getCgroupMemoryLimitInBytes().isEmpty()) {
            logger.debug("Native memory cgroup backstop inactive — no cgroup memory limit detected");
            return;
        }
        cancellable = threadPool.scheduleWithFixedDelay(new AbstractRunnable() {
            @Override
            protected void doRun() {
                check();
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn("Error sampling cgroup memory usage for native memory backstop", e);
            }

            @Override
            public boolean isForceExecution() {
                return true;
            }
        }, pollInterval, threadPool.executor(ThreadPool.Names.GENERIC));
    }

    @Override
    protected void doStop() {
        Scheduler.Cancellable c = this.cancellable;
        if (c != null) {
            c.cancel();
        }
    }

    @Override
    protected void doClose() {
        doStop();
    }

    /**
     * Returns {@code true} when new native memory allocations should be rejected because the
     * measured cgroup memory usage is above the high watermark.
     *
     * <p>Returns {@code false} when the backstop has not yet completed its first
     * poll, when the node is not inside a cgroup with a memory limit, or when
     * memory pressure has dropped below the resume threshold.
     */
    public boolean isRefusing() {
        return refusing;
    }

    /** The current high-watermark percentage. Exposed for use in exception messages. */
    int getHighWatermark() {
        return highWatermark;
    }

    /**
     * Reads the current cgroup memory usage and limit and updates the {@link #refusing} flag.
     * Package-private for unit testing.
     */
    void check() {
        OptionalLong usage = readCgroupUsage();
        OptionalLong limit = readCgroupLimit();
        if (usage.isEmpty() || limit.isEmpty()) {
            // No cgroup limit is available — treat as no constraint, not as full.
            return;
        }
        long u = usage.getAsLong();
        long l = limit.getAsLong();
        if (l <= 0) {
            return;
        }
        int usagePct = (int) (u * 100L / l);
        int wm = highWatermark;
        if (refusing == false && usagePct >= wm) {
            refusing = true;
            logger.warn(
                "Native memory allocations refused: cgroup memory usage {}% >= high-watermark {}% " + "(usage={} bytes, limit={} bytes)",
                usagePct,
                wm,
                u,
                l
            );
        } else if (refusing && usagePct < wm - HYSTERESIS_PERCENT) {
            refusing = false;
            logger.info(
                "Native memory allocations resumed: cgroup memory usage {}% < resume threshold {}% " + "(usage={} bytes, limit={} bytes)",
                usagePct,
                wm - HYSTERESIS_PERCENT,
                u,
                l
            );
        }
    }

    /**
     * Overridable in tests to inject synthetic cgroup usage readings.
     * Returns the working set (total usage minus inactive file cache), which reflects true
     * memory pressure — the kernel evicts inactive file cache on demand before OOM-killing.
     */
    OptionalLong readCgroupUsage() {
        return OsProbe.getInstance().getCgroupMemoryWorkingSetInBytes();
    }

    /** Overridable in tests to inject synthetic cgroup limit readings. */
    OptionalLong readCgroupLimit() {
        return OsProbe.getInstance().getCgroupMemoryLimitInBytes();
    }
}
