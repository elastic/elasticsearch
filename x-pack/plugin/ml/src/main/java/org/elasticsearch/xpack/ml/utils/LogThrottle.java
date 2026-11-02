/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.utils;

import org.elasticsearch.core.TimeValue;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * Per-key rate limiter for repeated log lines: the first occurrence of a key is emitted, later ones within the interval are
 * counted, and the count is handed back with the next emitted occurrence.
 */
final class LogThrottle {

    static final TimeValue DEFAULT_INTERVAL = TimeValue.timeValueMinutes(1);
    static final int DEFAULT_MAX_KEYS = 1000;

    /**
     * @param suppressedCount occurrences of the key swallowed since the previously emitted one; zero unless {@code shouldLog}
     */
    record Decision(boolean shouldLog, int suppressedCount) {}

    private static final class KeyState {
        private long lastEmittedMillis;
        private int suppressedSinceLastEmit;

        private KeyState(long lastEmittedMillis) {
            this.lastEmittedMillis = lastEmittedMillis;
        }
    }

    private final LongSupplier clockMillis;
    private final long intervalMillis;
    private final Map<String, KeyState> states;

    LogThrottle() {
        this(LogThrottle::monotonicMillis, DEFAULT_INTERVAL, DEFAULT_MAX_KEYS);
    }

    LogThrottle(LongSupplier clockMillis, TimeValue interval, int maxKeys) {
        if (maxKeys < 1) {
            throw new IllegalArgumentException("maxKeys must be positive, got [" + maxKeys + "]");
        }
        this.clockMillis = clockMillis;
        this.intervalMillis = interval.millis();
        // Access-ordered so that the key evicted at capacity is the least recently seen one.
        this.states = new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, KeyState> eldest) {
                return size() > maxKeys;
            }
        };
    }

    static long monotonicMillis() {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
    }

    synchronized Decision recordOccurrence(String key) {
        long now = clockMillis.getAsLong();
        KeyState state = states.get(key);
        if (state == null) {
            states.put(key, new KeyState(now));
            return new Decision(true, 0);
        }
        if (now - state.lastEmittedMillis >= intervalMillis) {
            int suppressed = state.suppressedSinceLastEmit;
            state.lastEmittedMillis = now;
            state.suppressedSinceLastEmit = 0;
            return new Decision(true, suppressed);
        }
        state.suppressedSinceLastEmit++;
        return new Decision(false, 0);
    }
}
