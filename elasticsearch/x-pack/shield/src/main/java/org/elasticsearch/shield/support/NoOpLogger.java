/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.support;

import org.elasticsearch.common.logging.ESLogger;

/**
 * A logger that doesn't log anything.
 */
public class NoOpLogger extends ESLogger {

    public static final ESLogger INSTANCE = new NoOpLogger();

    private NoOpLogger() {
        super(null, null);
    }

    @Override
    public String getPrefix() {
        return "";
    }

    @Override
    public String getName() {
        return "_no_op";
    }

    @Override
    public void setLevel(String level) {
    }

    @Override
    public String getLevel() {
        return "NONE";
    }

    @Override
    public boolean isTraceEnabled() {
        return false;
    }

    @Override
    public boolean isDebugEnabled() {
        return false;
    }

    @Override
    public boolean isInfoEnabled() {
        return false;
    }

    @Override
    public boolean isWarnEnabled() {
        return false;
    }

    @Override
    public boolean isErrorEnabled() {
        return false;
    }

    @Override
    public void trace(String msg, Object... params) {
    }

    @Override
    public void trace(String msg, Throwable cause, Object... params) {
    }

    @Override
    public void debug(String msg, Object... params) {
    }

    @Override
    public void debug(String msg, Throwable cause, Object... params) {
    }

    @Override
    public void info(String msg, Object... params) {
    }

    @Override
    public void info(String msg, Throwable cause, Object... params) {
    }

    @Override
    public void warn(String msg, Object... params) {
    }

    @Override
    public void warn(String msg, Throwable cause, Object... params) {
    }

    @Override
    public void error(String msg, Object... params) {
    }

    @Override
    public void error(String msg, Throwable cause, Object... params) {
    }
}
