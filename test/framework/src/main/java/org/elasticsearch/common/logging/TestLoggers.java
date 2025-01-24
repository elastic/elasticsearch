/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.elasticsearch.common.util.Maps;

import java.util.Collection;
import java.util.NavigableMap;
import java.util.function.Predicate;

public class TestLoggers {

    // visible for testing
    static NavigableMap<String, Level> getLogLevels() {
        Configuration config = LoggerContext.getContext(false).getConfiguration();
        return getLogLevels(config.getLoggers().values());
    }

    private static NavigableMap<String, Level> getLogLevels(Collection<LoggerConfig> configs) {
        return configs.stream().collect(Maps.toUnmodifiableSortedMap(LoggerConfig::getName, LoggerConfig::getLevel));
    }

    /**
     * Util to run a task with the original logger context restored afterwards.
     * This will reset loggers to their previous log levels and remove any additional loggers configured while running task.
     */
    public static void runWithLoggersRestored(Runnable task) {
        Configuration config = LoggerContext.getContext(false).getConfiguration();
        Collection<LoggerConfig> configs = config.getLoggers().values();
        var levels = getLogLevels(configs);
        try {
            task.run();
        } finally {
            // remove any added logger
            configs.stream().map(LoggerConfig::getName).filter(Predicate.not(levels::containsKey)).forEach(config::removeLogger);
            // restore original levels (in the right order)
            levels.forEach((logger, level) -> Loggers.setLevel(LogManager.getLogger(logger), level));
        }
    }
}
