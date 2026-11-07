/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.utils;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.xpack.ml.utils.MlInvariantLogger.Kind;

import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.equalTo;

public class MlInvariantLoggerTests extends ESTestCase {

    private static final Logger logger = LogManager.getLogger(MlInvariantLoggerTests.class);
    private static final String LOGGER_NAME = MlInvariantLoggerTests.class.getCanonicalName();
    private static final long INTERVAL_MILLIS = 60_000;

    private final AtomicLong clockMillis = new AtomicLong(randomLongBetween(0, 1_000_000));
    private final MlInvariantLogger invariantLogger = new MlInvariantLogger(clockMillis::get);

    private static MockLog.PatternSeenEventExpectation seen(String name, Level level, String regex) {
        return new MockLog.PatternSeenEventExpectation(name, LOGGER_NAME, level, regex);
    }

    private static MockLog.PatternNotSeenEventExpectation notSeen(String name, Level level, String regex) {
        return new MockLog.PatternNotSeenEventExpectation(name, LOGGER_NAME, level, regex);
    }

    public void testKindTokensShouldBeStable() {
        assertThat(Kind.CALENDAR_UPDATE_DROPPED.token(), equalTo("[ml-invariant:calendar_update_dropped]"));
    }

    public void testViolatedShouldLogErrorWithKindToken() {
        MockLog.assertThatLogger(
            () -> invariantLogger.violated(logger, Kind.CALENDAR_UPDATE_DROPPED, "df-1", "datafeed [df-1] has no credential"),
            MlInvariantLoggerTests.class,
            seen(
                "violation at ERROR with token",
                Level.ERROR,
                Pattern.quote("[ml-invariant:calendar_update_dropped] datafeed [df-1] has no credential")
            )
        );
    }

    public void testDegradedShouldLogWarnWithKindToken() {
        MockLog.assertThatLogger(
            () -> invariantLogger.degraded(logger, Kind.CALENDAR_UPDATE_DROPPED, MlInvariantLogger.KIND_ONLY, "queue full"),
            MlInvariantLoggerTests.class,
            seen("degraded at WARN with token", Level.WARN, Pattern.quote("[ml-invariant:calendar_update_dropped] queue full")),
            notSeen("degraded never at ERROR", Level.ERROR, ".*")
        );
    }

    public void testRepeatWithinIntervalShouldBeSuppressedPerKey() {
        MockLog.assertThatLogger(() -> {
            invariantLogger.violated(logger, Kind.CALENDAR_UPDATE_DROPPED, "df-1", "first df-1");
            invariantLogger.violated(logger, Kind.CALENDAR_UPDATE_DROPPED, "df-1", "second df-1");
            invariantLogger.violated(logger, Kind.CALENDAR_UPDATE_DROPPED, "df-2", "first df-2");
        },
            MlInvariantLoggerTests.class,
            seen("first df-1 emitted", Level.ERROR, ".*first df-1"),
            notSeen("repeat df-1 suppressed", Level.ERROR, ".*second df-1.*"),
            seen("other key emitted", Level.ERROR, ".*first df-2")
        );
    }

    public void testOccurrenceAfterIntervalShouldReportSuppressedCount() {
        MockLog.assertThatLogger(() -> {
            invariantLogger.degraded(logger, Kind.CALENDAR_UPDATE_DROPPED, MlInvariantLogger.KIND_ONLY, "drop 1");
            invariantLogger.degraded(logger, Kind.CALENDAR_UPDATE_DROPPED, MlInvariantLogger.KIND_ONLY, "drop 2");
            invariantLogger.degraded(logger, Kind.CALENDAR_UPDATE_DROPPED, MlInvariantLogger.KIND_ONLY, "drop 3");
            clockMillis.addAndGet(INTERVAL_MILLIS);
            invariantLogger.degraded(logger, Kind.CALENDAR_UPDATE_DROPPED, MlInvariantLogger.KIND_ONLY, "drop 4");
        },
            MlInvariantLoggerTests.class,
            seen("first emitted without count", Level.WARN, "\\Q[ml-invariant:calendar_update_dropped] drop 1\\E"),
            notSeen("middle drops suppressed", Level.WARN, ".*drop [23].*"),
            seen(
                "next emission carries suppressed count",
                Level.WARN,
                Pattern.quote("[ml-invariant:calendar_update_dropped] drop 4; suppressed [2] similar events since the previous report")
            )
        );
    }

    public void testKeysBeyondTrackingBoundShouldEvictOldestKey() {
        int distinctKeys = LogThrottle.DEFAULT_MAX_KEYS + 1;
        MockLog.assertThatLogger(() -> {
            for (int i = 0; i < distinctKeys; i++) {
                invariantLogger.violated(logger, Kind.CALENDAR_UPDATE_DROPPED, "df-" + i, "key " + i);
            }
            invariantLogger.violated(logger, Kind.CALENDAR_UPDATE_DROPPED, "df-0", "evicted key again");
            invariantLogger.violated(logger, Kind.CALENDAR_UPDATE_DROPPED, "df-" + (distinctKeys - 1), "tracked key again");
        },
            MlInvariantLoggerTests.class,
            seen("evicted key is treated as new", Level.ERROR, ".*evicted key again"),
            notSeen("tracked key stays throttled", Level.ERROR, ".*tracked key again")
        );
    }
}
