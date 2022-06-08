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
 * A static holder for LogManagerFactory
 */
public abstract class LogManagerFactory {

    private static volatile LogManagerFactory INSTANCE;

    public static LogManagerFactory provider() {
        return INSTANCE;
    }

    public abstract Logger getLogger(String name);

    public abstract Logger getLogger(Class<?> clazz);

    public static void setInstance(LogManagerFactory INSTANCE) {
        LogManagerFactory.INSTANCE = INSTANCE;
    }
}
