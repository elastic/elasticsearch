/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.logging.internal;

import static org.apache.logging.log4j.Level.ALL;
import static org.apache.logging.log4j.Level.DEBUG;
import static org.apache.logging.log4j.Level.ERROR;
import static org.apache.logging.log4j.Level.FATAL;
import static org.apache.logging.log4j.Level.INFO;
import static org.apache.logging.log4j.Level.OFF;
import static org.apache.logging.log4j.Level.TRACE;
import static org.apache.logging.log4j.Level.WARN;

public final class LevelUtil {

    private LevelUtil() {}

    public static org.apache.logging.log4j.Level log4jLevel(final org.elasticsearch.logging.Level level) {
        return switch (level) {
            case OFF -> OFF;
            case FATAL -> FATAL;
            case ERROR -> org.apache.logging.log4j.Level.ERROR;
            case WARN -> WARN;
            case INFO -> org.apache.logging.log4j.Level.INFO;
            case DEBUG -> org.apache.logging.log4j.Level.DEBUG;
            case TRACE -> TRACE;
            case ALL -> org.apache.logging.log4j.Level.ALL;
        };
    }

    public static org.elasticsearch.logging.Level elasticsearchLevel(final org.apache.logging.log4j.Level log4jLevel) {
        // we can't use a switch because log4j levels are not an enum
        if (log4jLevel == OFF) {
            return org.elasticsearch.logging.Level.OFF;
        } else if (log4jLevel == FATAL) {
            return org.elasticsearch.logging.Level.FATAL;
        } else if (log4jLevel == ERROR) {
            return org.elasticsearch.logging.Level.ERROR;
        } else if (log4jLevel == WARN) {
            return org.elasticsearch.logging.Level.WARN;
        } else if (log4jLevel == INFO) {
            return org.elasticsearch.logging.Level.INFO;
        } else if (log4jLevel == DEBUG) {
            return org.elasticsearch.logging.Level.DEBUG;
        } else if (log4jLevel == TRACE) {
            return org.elasticsearch.logging.Level.TRACE;
        } else if (log4jLevel == ALL) {
            return org.elasticsearch.logging.Level.ALL;
        }
        throw new AssertionError("unknown log4j level [" + log4jLevel + "]");
    }
}
