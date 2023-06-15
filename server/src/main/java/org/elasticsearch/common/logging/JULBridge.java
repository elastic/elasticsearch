/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.logging;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.logging.Level;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.Map;
import java.util.NavigableMap;
import java.util.logging.Handler;
import java.util.logging.LogRecord;

/**
 * A Java Util Logging handler that writes log messages to the Elasticsearch logging framework.
 */
class JULBridge extends Handler {

    private static final Map<java.util.logging.Level, Level> levelMap = Map.of(
        java.util.logging.Level.OFF,
        Level.OFF,
        java.util.logging.Level.SEVERE,
        Level.ERROR,
        java.util.logging.Level.WARNING,
        Level.WARN,
        java.util.logging.Level.INFO,
        Level.INFO,
        java.util.logging.Level.FINE,
        Level.DEBUG,
        java.util.logging.Level.FINEST,
        Level.TRACE,
        java.util.logging.Level.ALL,
        Level.ALL
    );

    private static final NavigableMap<Integer, Level> sortedLevelMap = levelMap.entrySet()
        .stream()
        .collect(Maps.toUnmodifiableSortedMap(e -> e.getKey().intValue(), Map.Entry::getValue));

    public static void install() {
        var rootJulLogger = java.util.logging.LogManager.getLogManager().getLogger("");
        // clear out any other handlers, so eg we don't also print to stdout
        for (var existingHandler : rootJulLogger.getHandlers()) {
            rootJulLogger.removeHandler(existingHandler);
        }
        rootJulLogger.addHandler(new JULBridge());
    }

    private JULBridge() {}

    @Override
    public void publish(LogRecord record) {
        Logger logger = LogManager.getLogger(record.getLoggerName());
        Level level = translateJulLevel(record.getLevel());
        Throwable thrown = record.getThrown();
        if (thrown == null) {
            logger.log(level, record.getMessage());
        } else {
            logger.log(level, record::getMessage, thrown);
        }
    }

    private Level translateJulLevel(java.util.logging.Level julLevel) {
        Level log4jLevel = levelMap.get(julLevel);
        if (log4jLevel != null) {
            return log4jLevel;
        }
        // no matching known level, so find the closest level by int value
        var closestEntry = sortedLevelMap.lowerEntry(julLevel.intValue());
        assert closestEntry != null; // not possible since ALL is min int
        return closestEntry.getValue();
    }

    @Override
    public void flush() {}

    @Override
    public void close() {}
}
