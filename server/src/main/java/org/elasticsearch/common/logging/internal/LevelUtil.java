/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.logging.internal;

public final class LevelUtil {

    private LevelUtil() {}

    public static org.apache.logging.log4j.Level log4jLevel(final org.elasticsearch.logging.Level level) {
        return switch (level) {
            case OFF -> org.apache.logging.log4j.Level.OFF;
            case FATAL -> org.apache.logging.log4j.Level.FATAL;
            case ERROR -> org.apache.logging.log4j.Level.ERROR;
            case WARN -> org.apache.logging.log4j.Level.WARN;
            case INFO -> org.apache.logging.log4j.Level.INFO;
            case DEBUG -> org.apache.logging.log4j.Level.DEBUG;
            case TRACE -> org.apache.logging.log4j.Level.TRACE;
            case ALL -> org.apache.logging.log4j.Level.ALL;
        };
    }
}
