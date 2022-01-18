/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Checks the time spent evaluating authorized indices for a request.
 * Has configurable logging levels based on execution time.
 */
class LoadAuthorizedIndicesTimeChecker implements Consumer<Collection<String>> {

    private final Logger logger;
    private final long startNanos;
    private final AuthorizationEngine.RequestInfo requestInfo;
    private final Thresholds thresholds;

    LoadAuthorizedIndicesTimeChecker(Logger logger, long startNanos, AuthorizationEngine.RequestInfo requestInfo, Thresholds thresholds) {
        this.logger = logger;
        this.startNanos = startNanos;
        this.requestInfo = requestInfo;
        this.thresholds = thresholds;
    }

    @Override
    public void accept(Collection<String> indices) {
        final long end = System.nanoTime();
        final long millis = TimeUnit.NANOSECONDS.toMillis(end - startNanos);
        final Level level = thresholds.getLogLevel(millis);
        if (level == Level.WARN) {
            logger.warn(
                "Resolving [{}] indices for action [{}] and user [{}] took [{}ms] which is greater than the threshold of {}ms;"
                    + " The index privileges for this user may be too complex for this cluster.",
                indices.size(),
                requestInfo.getAction(),
                requestInfo.getAuthentication().getUser().principal(),
                millis,
                thresholds.warnThresholdMs
            );
        } else {
            logger.log(
                level,
                "Took [{}ms] to resolve [{}] indices for action [{}] and user [{}]",
                millis,
                indices.size(),
                requestInfo.getAction(),
                requestInfo.getAuthentication().getUser().principal()
            );
        }
    }

    static final Setting<Boolean> LOGGING_ENABLED_SETTING = Setting.boolSetting(
        "xpack.security.authz.timer.indices.enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    static final Setting<TimeValue> DEBUG_THRESHOLD_SETTING = Setting.timeSetting(
        "xpack.security.authz.timer.indices.threshold.debug",
        TimeValue.timeValueMillis(20),
        Setting.Property.NodeScope
    );
    static final Setting<TimeValue> INFO_THRESHOLD_SETTING = Setting.timeSetting(
        "xpack.security.authz.timer.indices.threshold.info",
        TimeValue.timeValueMillis(100),
        Setting.Property.NodeScope
    );
    static final Setting<TimeValue> WARN_THRESHOLD_SETTING = Setting.timeSetting(
        "xpack.security.authz.timer.indices.threshold.warn",
        TimeValue.timeValueMillis(200),
        Setting.Property.NodeScope
    );

    static class Thresholds {
        private final long debugThresholdMs;
        private final long infoThresholdMs;
        private final long warnThresholdMs;

        Thresholds(TimeValue debugThreshold, TimeValue infoThreshold, TimeValue warnThreshold) {
            this.debugThresholdMs = debugThreshold.millis();
            this.infoThresholdMs = infoThreshold.millis();
            this.warnThresholdMs = warnThreshold.millis();
        }

        public Level getLogLevel(long millis) {
            if (millis > warnThresholdMs) {
                return Level.WARN;
            }
            if (millis > infoThresholdMs) {
                return Level.INFO;
            }
            if (millis > debugThresholdMs) {
                return Level.DEBUG;
            }
            return Level.TRACE;
        }

        long getDebugThresholdMs() {
            return debugThresholdMs;
        }

        long getInfoThresholdMs() {
            return infoThresholdMs;
        }

        long getWarnThresholdMs() {
            return warnThresholdMs;
        }
    }

    static final Consumer<Collection<String>> NO_OP_CONSUMER = ignore -> {};

    static class Factory {
        private final Logger logger;
        private volatile boolean loggingEnabled;
        private final Thresholds thresholds;

        Factory(Logger logger, Settings settings, ClusterSettings clusterSettings) {
            this.logger = logger;
            this.loggingEnabled = LOGGING_ENABLED_SETTING.get(settings);
            clusterSettings.addSettingsUpdateConsumer(LOGGING_ENABLED_SETTING, enabled -> this.loggingEnabled = enabled);

            TimeValue debugThreshold = DEBUG_THRESHOLD_SETTING.get(settings);
            TimeValue infoThreshold = INFO_THRESHOLD_SETTING.get(settings);
            TimeValue warnThreshold = WARN_THRESHOLD_SETTING.get(settings);

            if (infoThreshold.compareTo(debugThreshold) < 0) {
                throw new SettingsException(
                    "Setting [{}] ({}) cannot be less than the setting [{}] ({})",
                    INFO_THRESHOLD_SETTING.getKey(),
                    infoThreshold,
                    DEBUG_THRESHOLD_SETTING.getKey(),
                    debugThreshold
                );
            }
            if (warnThreshold.compareTo(infoThreshold) < 0) {
                throw new SettingsException(
                    "Setting [{}] ({}) cannot be less than the setting [{}] ({})",
                    WARN_THRESHOLD_SETTING.getKey(),
                    warnThreshold,
                    INFO_THRESHOLD_SETTING.getKey(),
                    infoThreshold
                );
            }

            this.thresholds = new Thresholds(debugThreshold, infoThreshold, warnThreshold);
        }

        public static Set<Setting<?>> getSettings() {
            return org.elasticsearch.core.Set.of(
                LOGGING_ENABLED_SETTING,
                DEBUG_THRESHOLD_SETTING,
                INFO_THRESHOLD_SETTING,
                WARN_THRESHOLD_SETTING
            );
        }

        public Consumer<Collection<String>> newTimer(AuthorizationEngine.RequestInfo requestInfo) {
            if (loggingEnabled) {
                return new LoadAuthorizedIndicesTimeChecker(logger, System.nanoTime(), requestInfo, thresholds);
            } else {
                return NO_OP_CONSUMER;
            }
        }

        public Thresholds getThresholds() {
            return thresholds;
        }
    }

}
