/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.internal;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.config.Configurator;
import org.elasticsearch.logging.Level;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.logging.internal.spi.LoggerFactory;

/**
 * Log4j-backed {@link LoggerFactory} local to {@code benchmarks/common}.
 *
 * <p>Mirrors the shape of server's {@code LoggerFactoryImpl} but avoids depending
 * on server. When server's log4j impl classes are eventually extracted into a
 * shared library, this class can be dropped and {@link BenchmarkLogging#configure()}
 * can install that shared implementation instead.
 */
final class BenchmarkLoggerFactory extends LoggerFactory {

    @Override
    public Logger getLogger(String name) {
        return new BenchmarkLogger(LogManager.getLogger(name));
    }

    @Override
    public Logger getLogger(Class<?> clazz) {
        // Mirrors server's LoggerFactoryImpl: use the class name string so that
        // the root logging configuration applies regardless of which classloader
        // the class came from (log4j's Class-based getLogger scans the loader
        // hierarchy for programmatic configuration, which ES does not use).
        return getLogger(clazz.getName());
    }

    @Override
    public void setRootLevel(Level level) {
        Configurator.setRootLevel(org.apache.logging.log4j.Level.valueOf(level.name()));
    }

    @Override
    public Level getRootLevel() {
        return Level.valueOf(LogManager.getRootLogger().getLevel().name());
    }
}
