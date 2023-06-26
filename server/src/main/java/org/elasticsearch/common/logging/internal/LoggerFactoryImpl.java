/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.logging.internal;

import org.apache.logging.log4j.LogManager;
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
        return new LoggerImpl(LogManager.getLogger(clazz));
    }
}
