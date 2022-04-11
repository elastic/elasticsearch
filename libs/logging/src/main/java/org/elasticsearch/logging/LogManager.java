/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging;

import org.elasticsearch.logging.spi.LogManagerFactory;

public class LogManager {

    public static Logger getLogger(final String name) {
        return LogManagerFactory.provider().getLogger(name);
    }

    public static Logger getLogger(final Class<?> clazz) {
        return LogManagerFactory.provider().getLogger(clazz);
    }

    private LogManager() {}

    // TODO PG getRootLogger do we want it?
    public static Logger getRootLogger() {
        return getLogger("");
    }

}
