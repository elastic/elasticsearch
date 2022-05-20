/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.logging.api.impl;


import org.apache.logging.log4j.LogManager;
import org.elasticsearch.logging.Logger;

public class LogManagerFactory implements org.elasticsearch.logging.spi.LogManagerFactory {
    @Override
    public Logger getLogger(String name) {
        //TODO PG logger impl instance caching
        return new LoggerImpl(LogManager.getLogger(name));
    }

    @Override
    public Logger getLogger(Class<?> clazz) {
        return new LoggerImpl(LogManager.getLogger(clazz));
    }
}
