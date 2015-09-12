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

package org.elasticsearch.common.logging.log4j;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.elasticsearch.common.logging.support.AbstractESLogger;

/**
 *
 */
public class Log4jESLogger extends AbstractESLogger {

    private final org.apache.log4j.Logger logger;
    private final String FQCN = AbstractESLogger.class.getName();

    public Log4jESLogger(String prefix, Logger logger) {
        super(prefix);
        this.logger = logger;
    }

    public Logger logger() {
        return logger;
    }

    @Override
    public void setLevel(String level) {
        if (level == null) {
            logger.setLevel(null);
        } else if ("error".equalsIgnoreCase(level)) {
            logger.setLevel(Level.ERROR);
        } else if ("warn".equalsIgnoreCase(level)) {
            logger.setLevel(Level.WARN);
        } else if ("info".equalsIgnoreCase(level)) {
            logger.setLevel(Level.INFO);
        } else if ("debug".equalsIgnoreCase(level)) {
            logger.setLevel(Level.DEBUG);
        } else if ("trace".equalsIgnoreCase(level)) {
            logger.setLevel(Level.TRACE);
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
        return logger.isTraceEnabled();
    }

    @Override
    public boolean isDebugEnabled() {
        return logger.isDebugEnabled();
    }

    @Override
    public boolean isInfoEnabled() {
        return logger.isInfoEnabled();
    }

    @Override
    public boolean isWarnEnabled() {
        return logger.isEnabledFor(Level.WARN);
    }

    @Override
    public boolean isErrorEnabled() {
        return logger.isEnabledFor(Level.ERROR);
    }

    @Override
    protected void internalTrace(String msg) {
        logger.log(FQCN, Level.TRACE, msg, null);
    }

    @Override
    protected void internalTrace(String msg, Throwable cause) {
        logger.log(FQCN, Level.TRACE, msg, cause);
    }

    @Override
    protected void internalDebug(String msg) {
        logger.log(FQCN, Level.DEBUG, msg, null);
    }

    @Override
    protected void internalDebug(String msg, Throwable cause) {
        logger.log(FQCN, Level.DEBUG, msg, cause);
    }

    @Override
    protected void internalInfo(String msg) {
        logger.log(FQCN, Level.INFO, msg, null);
    }

    @Override
    protected void internalInfo(String msg, Throwable cause) {
        logger.log(FQCN, Level.INFO, msg, cause);
    }

    @Override
    protected void internalWarn(String msg) {
        logger.log(FQCN, Level.WARN, msg, null);
    }

    @Override
    protected void internalWarn(String msg, Throwable cause) {
        logger.log(FQCN, Level.WARN, msg, cause);
    }

    @Override
    protected void internalError(String msg) {
        logger.log(FQCN, Level.ERROR, msg, null);
    }

    @Override
    protected void internalError(String msg, Throwable cause) {
        logger.log(FQCN, Level.ERROR, msg, cause);
    }
}
