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

    // TODO PG make sure we don't create too many levels..

    public static org.apache.logging.log4j.Level log4jLevel(final org.elasticsearch.logging.Level level) {
        return switch (level.getSeverity()) {
            case StandardLevels.OFF -> org.apache.logging.log4j.Level.OFF;
            case StandardLevels.FATAL -> org.apache.logging.log4j.Level.FATAL;
            case StandardLevels.ERROR -> org.apache.logging.log4j.Level.ERROR;
            case StandardLevels.WARN -> org.apache.logging.log4j.Level.WARN;
            case StandardLevels.INFO -> org.apache.logging.log4j.Level.INFO;
            case StandardLevels.DEBUG -> org.apache.logging.log4j.Level.DEBUG;
            case StandardLevels.TRACE -> org.apache.logging.log4j.Level.TRACE;
            case StandardLevels.ALL -> org.apache.logging.log4j.Level.ALL;
            default -> org.apache.logging.log4j.Level.forName(level.name(), level.getSeverity());
        };
    }

    // TODO PG make sure we don't create too many levels..
    static org.elasticsearch.logging.Level elasticsearchLevel(final org.apache.logging.log4j.Level level) {
        return switch (level.getStandardLevel().intLevel()) {
            case StandardLevels.OFF -> org.elasticsearch.logging.Level.OFF;
            case StandardLevels.FATAL -> org.elasticsearch.logging.Level.FATAL;
            case StandardLevels.ERROR -> org.elasticsearch.logging.Level.ERROR;
            case StandardLevels.WARN -> org.elasticsearch.logging.Level.WARN;
            case StandardLevels.INFO -> org.elasticsearch.logging.Level.INFO;
            case StandardLevels.DEBUG -> org.elasticsearch.logging.Level.DEBUG;
            case StandardLevels.TRACE -> org.elasticsearch.logging.Level.TRACE;
            case StandardLevels.ALL -> org.elasticsearch.logging.Level.ALL;
            default -> org.elasticsearch.logging.Level.of(level.name(), level.getStandardLevel().intLevel());
        };
    }

    static org.apache.logging.log4j.Logger log4jLogger(final org.elasticsearch.logging.Logger logger) {
        if (logger instanceof org.apache.logging.log4j.Logger log4jLogger) {
            return log4jLogger;
        }
        if (logger instanceof org.elasticsearch.logging.internal.LoggerImpl) {
            return ((org.elasticsearch.logging.internal.LoggerImpl) logger).log4jLogger();
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
