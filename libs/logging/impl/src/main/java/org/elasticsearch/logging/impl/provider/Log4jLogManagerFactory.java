/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging.impl.provider;

import org.elasticsearch.logging.Logger;
import org.elasticsearch.logging.impl.LoggerImpl;
import org.elasticsearch.logging.spi.LogManagerFactory;

public class Log4jLogManagerFactory implements LogManagerFactory {
    @Override
    public Logger getLogger(String name) {

        // org.apache.logging.log4j.Logger logger = org.apache.logging.log4j.LogManager.getLogger(name);
        org.apache.logging.log4j.Logger logger = getLogger1(name);
        return new LoggerImpl(logger); // TODO caching
    }

    private org.apache.logging.log4j.Logger getLogger1(String name) {
        org.apache.logging.log4j.Logger logger = org.apache.logging.log4j.LogManager.getContext(
            Log4jLogManagerFactory.class.getClassLoader(),
            false
        ).getLogger(name);
        return logger;
    }

    @Override
    public Logger getLogger(Class<?> clazz) {
        org.apache.logging.log4j.Logger logger = getLogger1(clazz);

        return new LoggerImpl(logger);
    }

    private org.apache.logging.log4j.Logger getLogger1(Class<?> clazz) {
        org.apache.logging.log4j.Logger logger = org.apache.logging.log4j.LogManager.getContext(
            Log4jLogManagerFactory.class.getClassLoader(),
            false
        ).getLogger(clazz);
        return logger;
    }

    @Override
    public Logger getPrefixLogger(String loggerName, String prefix) {
        return new LoggerImpl(new org.elasticsearch.logging.impl.PrefixLogger(getLogger1(loggerName), prefix));
    }

    @Override
    public Logger getPrefixLogger(Class<?> clazz, String prefix) {
        return new LoggerImpl(new org.elasticsearch.logging.impl.PrefixLogger(getLogger1(clazz), prefix));
    }
}
