/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging.internal;

public final class Util {

    private Util() {}

    static org.apache.logging.log4j.Level log4jLevel(final org.elasticsearch.logging.Level level) {
        return switch (level.getSeverity()) {
            case StandardLevels.OFF -> org.apache.logging.log4j.Level.OFF;
            case StandardLevels.FATAL -> org.apache.logging.log4j.Level.FATAL;
            case StandardLevels.ERROR -> org.apache.logging.log4j.Level.ERROR;
            case StandardLevels.WARN -> org.apache.logging.log4j.Level.WARN;
            case StandardLevels.INFO -> org.apache.logging.log4j.Level.INFO;
            case StandardLevels.DEBUG -> org.apache.logging.log4j.Level.DEBUG;
            case StandardLevels.TRACE -> org.apache.logging.log4j.Level.TRACE;
            case StandardLevels.ALL -> org.apache.logging.log4j.Level.ALL;
            default -> throw new IllegalStateException("unexpected level:" + level);
        };
    }

    static org.apache.logging.log4j.Logger log4jLogger(final org.elasticsearch.logging.Logger logger) {
        if (logger instanceof org.apache.logging.log4j.Logger log4jLogger) {
            return log4jLogger;
        }
        throw new IllegalArgumentException("unknown logger: " + logger);
    }

    // TODO: move to core Strings?
    public static boolean isNullOrEmpty(CharSequence str) {
        return str == null || str.isEmpty();
    }

    public static boolean isEmpty(CharSequence str) {
        return isNullOrEmpty(str);
    }
}
