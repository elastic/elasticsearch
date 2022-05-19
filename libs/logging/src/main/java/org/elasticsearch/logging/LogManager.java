/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging;

import org.elasticsearch.logging.impl.LoggerImpl;

/**
 * A class used for creating loggers.
 */
public class LogManager {

    /**
     * Returns a Logger with the specified name.
     *
     * @param name The logger name. If null the name of the calling class will be used.
     * @return The Logger.
     */
    public static Logger getLogger(final String name) {
        return new LoggerImpl(org.apache.logging.log4j.LogManager.getLogger(name));
    }

    /**
     * Returns a Logger using the fully qualified name of the Class as the Logger name.
     *
     * @param clazz The Class whose name should be used as the Logger name. If null it will default to the calling
     *            class.
     * @return The Logger.
     */
    public static Logger getLogger(final Class<?> clazz) {
        return new LoggerImpl(org.apache.logging.log4j.LogManager.getLogger(clazz));
    }

    private LogManager() {}
}
