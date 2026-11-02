/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.utils;

import org.apache.logging.log4j.Logger;

import java.util.function.LongSupplier;

/**
 * Emits rate-limited log lines carrying a stable {@code [ml-invariant:<kind>]} token, so log-based detectors can match an
 * invariant by token rather than by message wording. Log {@link #violated} only for a broken invariant and {@link #degraded}
 * only for a degraded outcome; neither may fire in healthy operation, including node churn.
 */
public final class MlInvariantLogger {

    /** One entry per invariant; the token appears nowhere else. */
    public enum Kind {
        /** A job update that changes groups was dropped from the full process-update queue (calendar API changes bypass that queue). */
        CALENDAR_UPDATE_DROPPED("calendar_update_dropped");

        private final String token;

        Kind(String kind) {
            this.token = "[ml-invariant:" + kind + "]";
        }

        public String token() {
            return token;
        }
    }

    /** Key for invariants throttled once per kind rather than per resource id. */
    public static final String KIND_ONLY = "";

    private final LogThrottle throttle;

    public MlInvariantLogger() {
        this(new LogThrottle());
    }

    public MlInvariantLogger(LongSupplier clockMillis) {
        this(new LogThrottle(clockMillis, LogThrottle.DEFAULT_INTERVAL, LogThrottle.DEFAULT_MAX_KEYS));
    }

    private MlInvariantLogger(LogThrottle throttle) {
        this.throttle = throttle;
    }

    /** Logs at ERROR, at most once per one minute per (kind, key). */
    public void violated(Logger logger, Kind kind, String key, String message) {
        LogThrottle.Decision decision = throttle.recordOccurrence(kind.name() + '/' + key);
        if (decision.shouldLog()) {
            logger.error(format(kind, message, decision.suppressedCount()));
        }
    }

    /** Logs at WARN, at most once per one minute per (kind, key). */
    public void degraded(Logger logger, Kind kind, String key, String message) {
        LogThrottle.Decision decision = throttle.recordOccurrence(kind.name() + '/' + key);
        if (decision.shouldLog()) {
            logger.warn(format(kind, message, decision.suppressedCount()));
        }
    }

    private static String format(Kind kind, String message, int suppressedCount) {
        if (suppressedCount == 0) {
            return kind.token() + " " + message;
        }
        return kind.token() + " " + message + "; suppressed [" + suppressedCount + "] similar events since the previous report";
    }
}
