/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.logging;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
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
     * A setting specifying the interval for clearing the cached log message stats
     */
    public static final Setting<TimeValue> STATS_RESET_INTERVAL_SETTING = Setting.timeSetting(
        "xpack.inference.logging.reset_interval",
        DEFAULT_STATS_RESET_INTERVAL_TIME,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private static final TimeValue DEFAULT_WAIT_DURATION_TIME = TimeValue.timeValueHours(1);
    /**
     * A setting specifying the amount of time to wait after a log call occurs before allowing another log call.
     */
    public static final Setting<TimeValue> LOGGER_WAIT_DURATION_SETTING = Setting.timeSetting(
        "xpack.inference.logging.wait_duration",
        DEFAULT_WAIT_DURATION_TIME,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private final ThreadPool threadPool;
    private Throttler throttler;
    private LoggerSettings loggerSettings;

    public ThrottlerManager(Settings settings, ThreadPool threadPool, ClusterService clusterService) {
        Objects.requireNonNull(settings);
        Objects.requireNonNull(clusterService);

        this.threadPool = Objects.requireNonNull(threadPool);
        this.loggerSettings = LoggerSettings.fromSettings(settings);

        throttler = new Throttler(loggerSettings.resetInterval(), loggerSettings.waitDuration(), threadPool);
        this.addSettingsUpdateConsumers(clusterService);
    }

    private void addSettingsUpdateConsumers(ClusterService clusterService) {
        clusterService.getClusterSettings().addSettingsUpdateConsumer(STATS_RESET_INTERVAL_SETTING, this::setResetInterval);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(LOGGER_WAIT_DURATION_SETTING, this::setWaitDuration);
    }

    // default for testing
    void setWaitDuration(TimeValue waitDuration) {
        loggerSettings = loggerSettings.createWithWaitDuration(waitDuration);

        throttler.setDurationToWait(waitDuration);
    }

    // default for testing
    void setResetInterval(TimeValue resetInterval) {
        loggerSettings = loggerSettings.createWithResetInterval(resetInterval);

        throttler.close();
        throttler = new Throttler(loggerSettings.resetInterval(), loggerSettings.waitDuration(), threadPool);
    }

    // default for testing
    Throttler getThrottler() {
        return throttler;
    }

    public void warn(Logger logger, String message, Throwable e) {
        Objects.requireNonNull(message);
        Objects.requireNonNull(e);

        throttler.execute(message, messageToLog -> logger.warn(messageToLog, e));
    }

    public void warn(Logger logger, String message) {
        Objects.requireNonNull(message);

        throttler.execute(message, logger::warn);
    }

    @Override
    public void close() {
        throttler.close();
    }

    public static List<Setting<?>> getSettingsDefinitions() {
        return List.of(STATS_RESET_INTERVAL_SETTING, LOGGER_WAIT_DURATION_SETTING);
    }

    private record LoggerSettings(TimeValue resetInterval, TimeValue waitDuration) {
        LoggerSettings {
            Objects.requireNonNull(resetInterval);
            Objects.requireNonNull(waitDuration);
        }

        static LoggerSettings fromSettings(Settings settings) {
            return new LoggerSettings(STATS_RESET_INTERVAL_SETTING.get(settings), LOGGER_WAIT_DURATION_SETTING.get(settings));
        }

        LoggerSettings createWithResetInterval(TimeValue resetInterval) {
            return new LoggerSettings(resetInterval, waitDuration);
        }

        LoggerSettings createWithWaitDuration(TimeValue waitDuration) {
            return new LoggerSettings(resetInterval, waitDuration);
        }
    }
}
