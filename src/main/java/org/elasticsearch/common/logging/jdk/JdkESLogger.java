/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.logging.jdk;

import org.elasticsearch.common.logging.support.AbstractESLogger;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 */
public class JdkESLogger extends AbstractESLogger {

    private final Logger logger;

    private final String name;

    public JdkESLogger(String prefix, String name, Logger logger) {
        super(prefix);
        this.logger = logger;
        this.name = name;
    }

    @Override
    public void setLevel(String level) {
        if (level == null) {
            logger.setLevel(null);
        } else if ("error".equalsIgnoreCase(level)) {
            logger.setLevel(Level.SEVERE);
        } else if ("warn".equalsIgnoreCase(level)) {
            logger.setLevel(Level.WARNING);
        } else if ("info".equalsIgnoreCase(level)) {
            logger.setLevel(Level.INFO);
        } else if ("debug".equalsIgnoreCase(level)) {
            logger.setLevel(Level.FINE);
        } else if ("trace".equalsIgnoreCase(level)) {
            logger.setLevel(Level.FINE);
        }
    }

    @Override
    public String getLevel() {
        if (logger.getLevel() == null) {
            return null;
        }
        return logger.getLevel().toString();
    }

    @Override
    public String getName() {
        return logger.getName();
    }

    @Override
    public boolean isTraceEnabled() {
        return logger.isLoggable(Level.FINEST);
    }

    @Override
    public boolean isDebugEnabled() {
        return logger.isLoggable(Level.FINE);
    }

    @Override
    public boolean isInfoEnabled() {
        return logger.isLoggable(Level.INFO);
    }

    @Override
    public boolean isWarnEnabled() {
        return logger.isLoggable(Level.WARNING);
    }

    @Override
    public boolean isErrorEnabled() {
        return logger.isLoggable(Level.SEVERE);
    }

    @Override
    protected void internalTrace(String msg) {
        logger.logp(Level.FINEST, name, null, msg);
    }

    @Override
    protected void internalTrace(String msg, Throwable cause) {
        logger.logp(Level.FINEST, name, null, msg, cause);
    }

    @Override
    protected void internalDebug(String msg) {
        logger.logp(Level.FINE, name, null, msg);
    }

    @Override
    protected void internalDebug(String msg, Throwable cause) {
        logger.logp(Level.FINE, name, null, msg, cause);
    }

    @Override
    protected void internalInfo(String msg) {
        logger.logp(Level.INFO, name, null, msg);
    }

    @Override
    protected void internalInfo(String msg, Throwable cause) {
        logger.logp(Level.INFO, name, null, msg, cause);
    }

    @Override
    protected void internalWarn(String msg) {
        logger.logp(Level.WARNING, name, null, msg);
    }

    @Override
    protected void internalWarn(String msg, Throwable cause) {
        logger.logp(Level.WARNING, name, null, msg, cause);
    }

    @Override
    protected void internalError(String msg) {
        logger.logp(Level.SEVERE, name, null, msg);
    }

    @Override
    protected void internalError(String msg, Throwable cause) {
        logger.logp(Level.SEVERE, name, null, msg, cause);
    }
}
