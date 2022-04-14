/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.apache.logging.es;

import org.apache.logging.log4j.spi.LoggerContext;
import org.apache.logging.log4j.spi.LoggerContextFactory;

import java.net.URI;

public class ESLoggerContextFactory implements LoggerContextFactory {
    private static final LoggerContext context = new ESLoggerContext();

    @Override
    public LoggerContext getContext(String fqcn, ClassLoader loader, Object externalContext, boolean currentContext) {
        return context;
    }

    @Override
    public LoggerContext getContext(
        String fqcn,
        ClassLoader loader,
        Object externalContext,
        boolean currentContext,
        URI configLocation,
        String name
    ) {
        return context;
    }

    @Override
    public void removeContext(LoggerContext context) {

    }
}
