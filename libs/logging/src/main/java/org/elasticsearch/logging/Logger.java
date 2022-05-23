/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging;

/**
 * Main interface for logging. Most operations are done through this interface.
 * This is interface heavily influenced by org.apache.logging.log4j2.Logger.
 * The most notable difference is lack of methods with Marker, LogBuilder and
 * the message supplier is {@code java.util.function.Supplier<String>}
 */
public interface Logger {
    /**
     * Logs a message object with the given level.
     *
     * @param level   the logging level
     * @param message the message to log.
     */
    void log(Level level, String message);

    /**
     * Logs a String message which is only to be constructed if the logging level is the specified level.
     *
     * @param level       the logging level
     * @param msgSupplier A function, which when called, produces the desired log String message;
     */
    void log(Level level, java.util.function.Supplier<String> msgSupplier, Throwable thrown);

    /**
     * Returns the Level associated with the Logger.
     *
     * @return the Level associate with the Logger.
     */
    Level getLevel();

    /**
     * Returns the logger name.
     *
     * @return the logger name.
     */
    String getName();

    /**
     * Checks whether this Logger is enabled for the {@link org.elasticsearch.logging.Level#FATAL FATAL} Level.
     *
     * @return boolean - {@code true} if this Logger is enabled for level {@link org.elasticsearch.logging.Level#FATAL FATAL}, {@code false}
     * otherwise.
     */
    boolean isFatalEnabled();

    /**
     * Checks whether this Logger is enabled for the {@link org.elasticsearch.logging.Level#ERROR ERROR} Level.
     *
     * @return boolean - {@code true} if this Logger is enabled for level {@link org.elasticsearch.logging.Level#ERROR ERROR}, {@code false}
     * otherwise.
     */
    boolean isErrorEnabled();

    /**
     * Checks whether this Logger is enabled for the {@link org.elasticsearch.logging.Level#WARN WARN} Level.
     *
     * @return boolean - {@code true} if this Logger is enabled for level {@link org.elasticsearch.logging.Level#WARN WARN}, {@code false}
     * otherwise.
     */
    boolean isWarnEnabled();

    /**
     * Checks whether this Logger is enabled for the {@link org.elasticsearch.logging.Level#INFO INFO} Level.
     *
     * @return boolean - {@code true} if this Logger is enabled for level {@link org.elasticsearch.logging.Level#INFO INFO}, {@code false}
     * otherwise.
     */
    boolean isInfoEnabled();

    /**
     * Checks whether this Logger is enabled for the {@link org.elasticsearch.logging.Level#DEBUG DEBUG} Level.
     *
     * @return boolean - {@code true} if this Logger is enabled for level {@link org.elasticsearch.logging.Level#DEBUG DEBUG}, {@code false}
     * otherwise.
     */
    boolean isDebugEnabled();

    /**
     * Checks whether this Logger is enabled for the {@link org.elasticsearch.logging.Level#TRACE TRACE} Level.
     *
     * @return boolean - {@code true} if this Logger is enabled for level {@link org.elasticsearch.logging.Level#TRACE TRACE}, {@code false}
     * otherwise.
     */
    boolean isTraceEnabled();

    boolean isEnabled(Level level);

    // -- fatal
    void fatal(java.util.function.Supplier<String> msgSupplier);

    void fatal(java.util.function.Supplier<String> msgSupplier, Throwable thrown);

    void fatal(String message);

    void fatal(String message, Throwable thrown);

    void fatal(String message, Object p0);

    void fatal(String message, Object p0, Object p1);

    void fatal(String message, Object p0, Object p1, Object p2);

    void fatal(String message, Object p0, Object p1, Object p2, Object p3);

    void fatal(String message, Object p0, Object p1, Object p2, Object p3, Object p4);

    void fatal(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5);

    void fatal(String message, Object... params);

    // -- error
    void error(java.util.function.Supplier<String> msgSupplier);

    void error(java.util.function.Supplier<String> msgSupplier, Throwable thrown);

    void error(String message);

    void error(String message, Throwable thrown);

    void error(String message, Object p0);

    void error(String message, Object p0, Object p1);

    void error(String message, Object p0, Object p1, Object p2);

    void error(String message, Object p0, Object p1, Object p2, Object p3);

    void error(String message, Object p0, Object p1, Object p2, Object p3, Object p4);

    void error(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5);

    void error(String message, Object... params);

    // -- warn
    void warn(java.util.function.Supplier<String> msgSupplier);

    void warn(java.util.function.Supplier<String> msgSupplier, Throwable thrown);

    void warn(String message);

    void warn(String message, Throwable thrown);

    void warn(String message, Object p0);

    void warn(String message, Object p0, Object p1);

    void warn(String message, Object p0, Object p1, Object p2);

    void warn(String message, Object p0, Object p1, Object p2, Object p3);

    void warn(String message, Object p0, Object p1, Object p2, Object p3, Object p4);

    void warn(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5);

    void warn(String message, Object... params);

    // -- info
    void info(java.util.function.Supplier<String> msgSupplier);

    void info(java.util.function.Supplier<String> msgSupplier, Throwable thrown);

    void info(String message);

    void info(String message, Throwable thrown);

    void info(String message, Object p0);

    void info(String message, Object p0, Object p1);

    void info(String message, Object p0, Object p1, Object p2);

    void info(String message, Object p0, Object p1, Object p2, Object p3);

    void info(String message, Object p0, Object p1, Object p2, Object p3, Object p4);

    void info(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5);

    void info(String message, Object... params);

    // -- debug
    void debug(java.util.function.Supplier<String> msgSupplier);

    void debug(java.util.function.Supplier<String> msgSupplier, Throwable thrown);

    void debug(String message);

    void debug(String message, Throwable thrown);

    void debug(String message, Object p0);

    void debug(String message, Object p0, Object p1);

    void debug(String message, Object p0, Object p1, Object p2);

    void debug(String message, Object p0, Object p1, Object p2, Object p3);

    void debug(String message, Object p0, Object p1, Object p2, Object p3, Object p4);

    void debug(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5);

    void debug(String message, Object... params);

    // -- trace
    void trace(java.util.function.Supplier<String> msgSupplier);

    void trace(java.util.function.Supplier<String> msgSupplier, Throwable thrown);

    void trace(String message);

    void trace(String message, Throwable thrown);

    void trace(String message, Object p0);

    void trace(String message, Object p0, Object p1);

    void trace(String message, Object p0, Object p1, Object p2);

    void trace(String message, Object p0, Object p1, Object p2, Object p3);

    void trace(String message, Object p0, Object p1, Object p2, Object p3, Object p4);

    void trace(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5);

    void trace(String message, Object... params);

}
