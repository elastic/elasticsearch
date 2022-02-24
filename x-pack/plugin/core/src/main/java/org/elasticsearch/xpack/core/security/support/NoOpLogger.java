/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.support;

import org.elasticsearch.logging.Level;
import org.elasticsearch.logging.Logger;

/**
 * A logger that doesn't log anything.
 */
public class NoOpLogger implements Logger {

    public static NoOpLogger INSTANCE = new NoOpLogger();

    private NoOpLogger() {

    }


    @Override
    public String getName() {
        return null;
    }

    @Override
    public boolean isLoggable(Level level) {
        return false;
    }

    @Override
    public void log(Level level, String message, Object... params) {

    }

    @Override
    public void log(Level level, String message, Throwable throwable) {

    }
}
