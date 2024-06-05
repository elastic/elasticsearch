/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging.internal.spi;

import org.elasticsearch.logging.Logger;

/**
 * A factory used by LogManager to create loggers.
 */
public abstract class LoggerFactory {

    private static volatile LoggerFactory INSTANCE;

    public static LoggerFactory provider() {
        return INSTANCE;
    }

    public abstract Logger getLogger(String name);

    public abstract Logger getLogger(Class<?> clazz);

    public static void setInstance(LoggerFactory INSTANCE) {
        LoggerFactory.INSTANCE = INSTANCE;
    }
}
