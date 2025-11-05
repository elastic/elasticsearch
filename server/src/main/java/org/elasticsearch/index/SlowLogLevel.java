/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import java.util.Locale;

/**
 * Legacy enum class for slow log level settings in index operations.
 * Kept for 7.x backwards compatibility. Do not use in new code.
 *
 * @deprecated This class is deprecated and will be removed in version 9.0.
 *             Use standard logging levels from log4j instead.
 * TODO: Remove in 9.0
 */
@Deprecated
public enum SlowLogLevel {
    /** Warning level - most specific, minimal logging */
    WARN(3),
    /** Info level - moderate logging */
    INFO(2),
    /** Debug level - detailed logging */
    DEBUG(1),
    /** Trace level - least specific, maximum logging */
    TRACE(0);

    private final int specificity;

    SlowLogLevel(int specificity) {
        this.specificity = specificity;
    }

    /**
     * Parses a string into a SlowLogLevel enum value.
     *
     * @param level the string representation of the log level (case-insensitive)
     * @return the corresponding SlowLogLevel
     * @throws IllegalArgumentException if the level string doesn't match any enum value
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SlowLogLevel level = SlowLogLevel.parse("warn");
     * // level is SlowLogLevel.WARN
     * }</pre>
     */
    public static SlowLogLevel parse(String level) {
        return valueOf(level.toUpperCase(Locale.ROOT));
    }

    /**
     * Determines if this log level is enabled for the given log level to be used.
     * A level is enabled if its specificity is less than or equal to the level to be used.
     *
     * @param levelToBeUsed the log level to check against
     * @return true if this level is enabled for the given level, false otherwise
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // INFO tries to log with WARN level - should allow
     * boolean enabled = SlowLogLevel.INFO.isLevelEnabledFor(SlowLogLevel.WARN);
     * // returns true because INFO (2) <= WARN (3)
     * }</pre>
     */
    boolean isLevelEnabledFor(SlowLogLevel levelToBeUsed) {
        // example: this.info(2) tries to log with levelToBeUsed.warn(3) - should allow
        return this.specificity <= levelToBeUsed.specificity;
    }
}
