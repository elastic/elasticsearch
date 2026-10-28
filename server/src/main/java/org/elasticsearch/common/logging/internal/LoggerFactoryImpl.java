/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.logging.internal;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.logging.Level;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.logging.internal.spi.LoggerFactory;

public class LoggerFactoryImpl extends LoggerFactory {
    @Override
    public Logger getLogger(String name) {
        // TODO PG logger impl instance caching https://github.com/elastic/elasticsearch/issues/87511
        return new LoggerImpl(LogManager.getLogger(name));
    }

    @Override
    public Logger getLogger(Class<?> clazz) {
        // Elasticsearch configures logging at the root level, it does not support
        // programmatic configuration at the logger level. Log4j's method for
        // getting a logger by Class doesn't just use the class name, but also
        // scans the classloader hierarchy for programmatic configuration. Here we
        // just delegate to use the String class name so that regardless of which
        // classloader a class comes from, we will use the root logging config.
        return getLogger(clazz.getName());
    }

    @Override
    public void setRootLevel(Level level) {
        var log4jLevel = LevelUtil.log4jLevel(level);
        Loggers.setLevel(LogManager.getRootLogger(), log4jLevel);
    }

    @Override
    public Level getRootLevel() {
        return LevelUtil.elasticsearchLevel(LogManager.getRootLogger().getLevel());
    }
}
