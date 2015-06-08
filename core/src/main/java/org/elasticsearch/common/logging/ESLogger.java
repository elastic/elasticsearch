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

/**
 * Contract for all elasticsearch loggers.
 */
public interface ESLogger {

    String getPrefix();

    String getName();

    /**
     * Allows to set the logger level
     * If the new level is null, the logger will inherit its level
     * from its nearest ancestor with a specific (non-null) level value.
     * @param level the new level
     */
    void setLevel(String level);

    /**
     * Returns the current logger level
     * If the level is null, it means that the logger inherits its level
     * from its nearest ancestor with a specific (non-null) level value.
     * @return the logger level
     */
    String getLevel();

    /**
     * Returns {@code true} if a TRACE level message is logged.
     */
    boolean isTraceEnabled();

    /**
     * Returns {@code true} if a DEBUG level message is logged.
     */
    boolean isDebugEnabled();

    /**
     * Returns {@code true} if an INFO level message is logged.
     */
    boolean isInfoEnabled();

    /**
     * Returns {@code true} if a WARN level message is logged.
     */
    boolean isWarnEnabled();

    /**
     * Returns {@code true} if an ERROR level message is logged.
     */
    boolean isErrorEnabled();

    /**
     * Logs a DEBUG level message.
     */
    void trace(String msg, Object... params);

    /**
     * Logs a DEBUG level message.
     */
    void trace(String msg, Throwable cause, Object... params);

    /**
     * Logs a DEBUG level message.
     */
    void debug(String msg, Object... params);

    /**
     * Logs a DEBUG level message.
     */
    void debug(String msg, Throwable cause, Object... params);

    /**
     * Logs an INFO level message.
     */
    void info(String msg, Object... params);

    /**
     * Logs an INFO level message.
     */
    void info(String msg, Throwable cause, Object... params);

    /**
     * Logs a WARN level message.
     */
    void warn(String msg, Object... params);

    /**
     * Logs a WARN level message.
     */
    void warn(String msg, Throwable cause, Object... params);

    /**
     * Logs an ERROR level message.
     */
    void error(String msg, Object... params);

    /**
     * Logs an ERROR level message.
     */
    void error(String msg, Throwable cause, Object... params);

}
