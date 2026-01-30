/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.logging;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.util.List;
import java.util.Objects;

/**
 * This class manages the settings for a {@link Throttler}.
 */
public class ThrottlerManager implements Closeable {
    private static final TimeValue DEFAULT_STATS_RESET_INTERVAL_TIME = TimeValue.timeValueDays(1);
    /**
     * Legacy log throttling setting, kept for BWC compatibility. This setting has no effect in 9.1.0 and later. Do not use.
     * TODO remove in 10.0
     */
    @UpdateForV10(owner = UpdateForV10.Owner.MACHINE_LEARNING)
    @Deprecated
    public static final Setting<TimeValue> STATS_RESET_INTERVAL_SETTING = Setting.timeSetting(
        "xpack.inference.logging.reset_interval",
        DEFAULT_STATS_RESET_INTERVAL_TIME,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private static final TimeValue DEFAULT_WAIT_DURATION_TIME = TimeValue.timeValueHours(1);
    /**
     * Legacy log throttling setting, kept for BWC compatibility. This setting has no effect in 9.1.0 and later. Do not use.
     * TODO remove in 10.0
     */
    @UpdateForV10(owner = UpdateForV10.Owner.MACHINE_LEARNING)
    @Deprecated
    public static final Setting<TimeValue> LOGGER_WAIT_DURATION_SETTING = Setting.timeSetting(
        "xpack.inference.logging.wait_duration",
        DEFAULT_WAIT_DURATION_TIME,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private static final TimeValue DEFAULT_LOG_EMIT_INTERVAL = TimeValue.timeValueHours(1);
    /**
     * This setting specifies how often a thread will run to emit repeated log messages.
     */
    public static final Setting<TimeValue> LOG_EMIT_INTERVAL = Setting.timeSetting(
        "xpack.inference.logging.interval",
        DEFAULT_LOG_EMIT_INTERVAL,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private volatile TimeValue logInterval;

    private Throttler throttler;

    public ThrottlerManager(Settings settings, ThreadPool threadPool) {
        Objects.requireNonNull(settings);

        throttler = new Throttler(LOG_EMIT_INTERVAL.get(settings), threadPool);
        throttler.init();
    }

    public void init(ClusterService clusterService) {
        Objects.requireNonNull(clusterService);

        clusterService.getClusterSettings().addSettingsUpdateConsumer(LOG_EMIT_INTERVAL, this::setLogInterval);
    }

    // default for testing
    void setLogInterval(TimeValue logInterval) {
        this.logInterval = logInterval;

        var oldThrottler = throttler;
        throttler = new Throttler(oldThrottler, this.logInterval);
        throttler.init();
        oldThrottler.close();
    }

    // default for testing
    Throttler getThrottler() {
        return throttler;
    }

    public void warn(Logger logger, String message, Throwable e) {
        Objects.requireNonNull(message);
        Objects.requireNonNull(e);

        throttler.execute(logger, Level.WARN, message, e);
    }

    public void warn(Logger logger, String message) {
        Objects.requireNonNull(message);

        throttler.execute(logger, Level.WARN, message);
    }

    @Override
    public void close() {
        throttler.close();
    }

    public static List<Setting<?>> getSettingsDefinitions() {
        return List.of(LOG_EMIT_INTERVAL, STATS_RESET_INTERVAL_SETTING, LOGGER_WAIT_DURATION_SETTING);
    }
}
