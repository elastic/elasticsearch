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

package org.elasticsearch.common.logging;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;

/**
 * Elasticsearch's logger wrapper.
 */
public class ESLogger {
    private static final String FQCN = ESLogger.class.getName();

    private final String prefix;
    private final Logger logger;

    public ESLogger(String prefix, Logger logger) {
        this.prefix = prefix;
        this.logger = logger;
    }

    /**
     * The prefix of the log.
     */
    public String getPrefix() {
        return this.prefix;
    }

    /**
     * Fetch the underlying logger so we can look at it. Only exists for testing.
     */
    Logger getLogger() {
        return logger;
    }

    /**
     * Set the level of the logger. If the new level is null, the logger will inherit it's level from its nearest ancestor with a non-null
     * level.
     */
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

    /**
     * The level of this logger. If null then the logger is inheriting it's level from its nearest ancestor with a non-null level.
     */
    public String getLevel() {
        if (logger.getLevel() == null) {
            return null;
        }
        return logger.getLevel().toString();
    }

    /**
     * The name of this logger.
     */
    public String getName() {
        return logger.getName();
    }

    /**
     * Returns {@code true} if a TRACE level message should be logged.
     */
    public boolean isTraceEnabled() {
        return logger.isTraceEnabled();
    }

    /**
     * Returns {@code true} if a DEBUG level message should be logged.
     */
    public boolean isDebugEnabled() {
        return logger.isDebugEnabled();
    }

    /**
     * Returns {@code true} if an INFO level message should be logged.
     */
    public boolean isInfoEnabled() {
        return logger.isInfoEnabled();
    }

    /**
     * Returns {@code true} if a WARN level message should be logged.
     */
    public boolean isWarnEnabled() {
        return logger.isEnabledFor(Level.WARN);
    }

    /**
     * Returns {@code true} if an ERROR level message should be logged.
     */
    public boolean isErrorEnabled() {
        return logger.isEnabledFor(Level.ERROR);
    }

    /**
     * Logs a TRACE level message.
     */
    public void trace(String msg, Object... params) {
        trace(msg, null, params);
    }

    /**
     * Logs a TRACE level message with an exception.
     */
    public void trace(String msg, Throwable cause, Object... params) {
        if (isTraceEnabled()) {
            logger.log(FQCN, Level.TRACE, format(prefix, msg, params), cause);
        }
    }

    /**
     * Logs a DEBUG level message.
     */
    public void debug(String msg, Object... params) {
        debug(msg, null, params);
    }

    /**
     * Logs a DEBUG level message with an exception.
     */
    public void debug(String msg, Throwable cause, Object... params) {
        if (isDebugEnabled()) {
            logger.log(FQCN, Level.DEBUG, format(prefix, msg, params), cause);
        }
    }

    /**
     * Logs a INFO level message.
     */
    public void info(String msg, Object... params) {
        info(msg, null, params);
    }

    /**
     * Logs a INFO level message with an exception.
     */
    public void info(String msg, Throwable cause, Object... params) {
        if (isInfoEnabled()) {
            logger.log(FQCN, Level.INFO, format(prefix, msg, params), cause);
        }
    }

    /**
     * Logs a WARN level message.
     */
    public void warn(String msg, Object... params) {
        warn(msg, null, params);
    }

    /**
     * Logs a WARN level message with an exception.
     */
    public void warn(String msg, Throwable cause, Object... params) {
        if (isWarnEnabled()) {
            logger.log(FQCN, Level.WARN, format(prefix, msg, params), cause);
        }
    }

    /**
     * Logs a ERROR level message.
     */
    public void error(String msg, Object... params) {
        error(msg, null, params);
    }

    /**
     * Logs a ERROR level message with an exception.
     */
    public void error(String msg, Throwable cause, Object... params) {
        if (isErrorEnabled()) {
            logger.log(FQCN, Level.ERROR, format(prefix, msg, params), cause);
        }
    }
}
