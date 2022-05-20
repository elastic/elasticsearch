/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging.spi;


import org.elasticsearch.logging.Logger;

import java.util.ServiceLoader;

/**
 * SPI for creating new loggers
 */
public interface LogManagerFactory {

    LogManagerFactory INSTANCE = loadProvider();

    static LogManagerFactory loadProvider() {
        ServiceLoader<LogManagerFactory> sl = ServiceLoader.load(LogManagerFactory.class, ClassLoader.getSystemClassLoader());
        return sl.findFirst().orElseThrow();
    }

    static LogManagerFactory provider() {
       return INSTANCE;
    }

    Logger getLogger(String name);

    Logger getLogger(Class<?> clazz);

}
