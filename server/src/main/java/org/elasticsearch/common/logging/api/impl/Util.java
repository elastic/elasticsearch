/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.logging.api.impl;

import org.elasticsearch.logging.Level;

public final class Util {

    private Util() {}

    public static org.apache.logging.log4j.Level log4jLevel(final org.elasticsearch.logging.Level level) {
        return switch (level.getSeverity()) {
            case Level.StandardLevels.OFF -> org.apache.logging.log4j.Level.OFF;
            case Level.StandardLevels.FATAL -> org.apache.logging.log4j.Level.FATAL;
            case Level.StandardLevels.ERROR -> org.apache.logging.log4j.Level.ERROR;
            case Level.StandardLevels.WARN -> org.apache.logging.log4j.Level.WARN;
            case Level.StandardLevels.INFO -> org.apache.logging.log4j.Level.INFO;
            case Level.StandardLevels.DEBUG -> org.apache.logging.log4j.Level.DEBUG;
            case Level.StandardLevels.TRACE -> org.apache.logging.log4j.Level.TRACE;
            case Level.StandardLevels.ALL -> org.apache.logging.log4j.Level.ALL;
            default -> org.apache.logging.log4j.Level.forName(level.name(), level.getSeverity());
        };
    }

    static org.elasticsearch.logging.Level elasticsearchLevel(final org.apache.logging.log4j.Level level) {
        return switch (level.getStandardLevel().intLevel()) {
            case Level.StandardLevels.OFF -> org.elasticsearch.logging.Level.OFF;
            case Level.StandardLevels.FATAL -> org.elasticsearch.logging.Level.FATAL;
            case Level.StandardLevels.ERROR -> org.elasticsearch.logging.Level.ERROR;
            case Level.StandardLevels.WARN -> org.elasticsearch.logging.Level.WARN;
            case Level.StandardLevels.INFO -> org.elasticsearch.logging.Level.INFO;
            case Level.StandardLevels.DEBUG -> org.elasticsearch.logging.Level.DEBUG;
            case Level.StandardLevels.TRACE -> org.elasticsearch.logging.Level.TRACE;
            case Level.StandardLevels.ALL -> org.elasticsearch.logging.Level.ALL;
            default -> org.elasticsearch.logging.Level.forName(level.name(), level.getStandardLevel().intLevel());
        };
    }

}
