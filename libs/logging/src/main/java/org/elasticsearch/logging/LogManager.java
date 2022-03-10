/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging;

import org.elasticsearch.logging.internal.LoggerImpl;

public class LogManager {

    public static Logger getLogger(final String name) {
        org.apache.logging.log4j.Logger logger = org.apache.logging.log4j.LogManager.getLogger(name);
        return new LoggerImpl(logger);
    }

    public static Logger getLogger(final Class<?> clazz) {
        org.apache.logging.log4j.Logger logger = org.apache.logging.log4j.LogManager.getLogger(clazz);
        return new LoggerImpl(logger);
    }

    private LogManager() {}

    public static Logger getRootLogger() {
        return getLogger("");
    }

    // getRootLogger do we want it?
}
