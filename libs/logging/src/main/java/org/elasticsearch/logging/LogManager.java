/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging;

import org.elasticsearch.logging.internal.spi.LoggerFactory;

/**
 * A Manager of {@code Loggers}. This class consists of factory methods for creating and retrieving Loggers.
 */
public final class LogManager {

    /**
     * Returns a Logger with the specified name.
     *
     * @param name The logger name.
     * @return The Logger.
     */
    public static Logger getLogger(final String name) {
        return LoggerFactory.provider().getLogger(name);
    }

    /**
     * Returns a Logger using the fully qualified name of the Class as the Logger name.
     *
     * @param clazz The Class whose name should be used as the Logger name.
     * @return The Logger.
     */
    public static Logger getLogger(final Class<?> clazz) {
        return LoggerFactory.provider().getLogger(clazz);
    }

    private LogManager() {}
}
