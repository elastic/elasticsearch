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
     * Logs a message String with the given level.
     *
     * @param level   the logging level
     * @param message the message CharSequence to log.
     */
    void log(Level level, String message);

    /**
     * Logs a String message which is only to be constructed if the logging level is the specified level.
     *
     * @param level           the logging level
     * @param messageSupplier A function, which when called, produces the desired log String message;
     */
    void log(Level level, java.util.function.Supplier<String> messageSupplier, Throwable throwable);

    /**
     * Gets the Level associated with the Logger.
     *
     * @return the Level associate with the Logger.
     */
    Level getLevel();

    /**
     * Gets the logger name.
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
     * Checks whether this Logger is enabled for the {@link org.elasticsearch.logging.Level#FATAL FATAL} Level.
     *
     * @return boolean - {@code true} if this Logger is enabled for level {@link org.elasticsearch.logging.Level#FATAL FATAL}, {@code false}
     * otherwise.
     */
    boolean isErrorEnabled();

    /**
     * Checks whether this Logger is enabled for the {@link org.elasticsearch.logging.Level#FATAL FATAL} Level.
     *
     * @return boolean - {@code true} if this Logger is enabled for level {@link org.elasticsearch.logging.Level#FATAL FATAL}, {@code false}
     * otherwise.
     */
    boolean isWarnEnabled();

    /**
     * Checks whether this Logger is enabled for the {@link org.elasticsearch.logging.Level#FATAL FATAL} Level.
     *
     * @return boolean - {@code true} if this Logger is enabled for level {@link org.elasticsearch.logging.Level#FATAL FATAL}, {@code false}
     * otherwise.
     */
    boolean isInfoEnabled();

    /**
     * Checks whether this Logger is enabled for the {@link org.elasticsearch.logging.Level#FATAL FATAL} Level.
     *
     * @return boolean - {@code true} if this Logger is enabled for level {@link org.elasticsearch.logging.Level#FATAL FATAL}, {@code false}
     * otherwise.
     */
    boolean isDebugEnabled();

    /**
     * Checks whether this Logger is enabled for the {@link org.elasticsearch.logging.Level#FATAL FATAL} Level.
     *
     * @return boolean - {@code true} if this Logger is enabled for level {@link org.elasticsearch.logging.Level#FATAL FATAL}, {@code false}
     * otherwise.
     */
    boolean isTraceEnabled();

    /**
     * Checks whether this Logger is enabled for the given Level.
     * <p>
     * Note that passing in {@link org.elasticsearch.logging.Level#OFF OFF} always returns {@code true}.
     * </p>
     *
     * @param level the Level to check
     * @return boolean - {@code true} if this Logger is enabled for level, {@code false} otherwise.
     */
    boolean isEnabled(Level level);

    // -- fatal

    /**
     * //modified
     * Logs a message which is only to be constructed if the logging level is the {@link org.elasticsearch.logging.Level#FATAL FATAL} level
     *
     * @param messageSupplier A function, which when called, produces the desired log string.
     */
    void fatal(java.util.function.Supplier<String> messageSupplier);

    /**
     * Logs a message which is only to be constructed if the logging level is the {@link org.elasticsearch.logging.Level#FATAL FATAL} level
     *
     * @param messageSupplier A function, which when called, produces the desired log string.
     * @param throwable       A Throwable or null.
     */
    void fatal(java.util.function.Supplier<String> messageSupplier, Throwable throwable);

    /**
     * Logs a message at the {@link org.elasticsearch.logging.Level#FATAL FATAL} level.
     *
     * @param message the message string to be logged
     */
    void fatal(String message);

    /**
     * Logs a message at the {@link org.elasticsearch.logging.Level#FATAL FATAL} level.
     *
     * @param message   the message string to be logged
     * @param throwable A Throwable or null.
     */
    void fatal(String message, Throwable throwable);

    /**
     * Logs a message with parameters at fatal level.
     *
     * @param message the message to log
     * @param p0      parameter to the message.
     */
    void fatal(String message, Object p0);

    /**
     * Logs a message with parameters at fatal level.
     *
     * @param message the message to log
     * @param p0      parameter to the message.
     * @param p1      parameter to the message.
     */
    void fatal(String message, Object p0, Object p1);

    /**
     * Logs a message with parameters at fatal level.
     *
     * @param message the message to log
     * @param p0      parameter to the message.
     * @param p1      parameter to the message.
     * @param p2      parameter to the message.
     */
    void fatal(String message, Object p0, Object p1, Object p2);

    /**
     * Logs a message with parameters at fatal level.
     *
     * @param message the message to log
     * @param p0      parameter to the message.
     * @param p1      parameter to the message.
     * @param p2      parameter to the message.
     * @param p3      parameter to the message.
     */
    void fatal(String message, Object p0, Object p1, Object p2, Object p3);

    /**
     * Logs a message with parameters at fatal level.
     *
     * @param message the message to log
     * @param p0      parameter to the message.
     * @param p1      parameter to the message.
     * @param p2      parameter to the message.
     * @param p3      parameter to the message.
     * @param p4      parameter to the message.
     */
    void fatal(String message, Object p0, Object p1, Object p2, Object p3, Object p4);

    /**
     * Logs a message with parameters at fatal level.
     *
     * @param message the message to log
     * @param p0      parameter to the message.
     * @param p1      parameter to the message.
     * @param p2      parameter to the message.
     * @param p3      parameter to the message.
     * @param p4      parameter to the message.
     * @param p5      parameter to the message.
     */
    void fatal(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5);

    /**
     * Logs a message with parameters at the {@link org.elasticsearch.logging.Level#FATAL FATAL} level.
     *
     * @param message the message to log
     * @param params  parameters to the message.
     */
    void fatal(String message, Object... params);

    // -- error

    /**
     * //modified
     * Logs a message which is only to be constructed if the logging level is the {@link org.elasticsearch.logging.Level#ERROR ERROR} level
     *
     * @param messageSupplier A function, which when called, produces the desired log string.
     */
    void error(java.util.function.Supplier<String> messageSupplier);

    /**
     * Logs a message which is only to be constructed if the logging level is the {@link org.elasticsearch.logging.Level#ERROR ERROR} level
     *
     * @param messageSupplier A function, which when called, produces the desired log string.
     * @param throwable       A Throwable or null.
     */
    void error(java.util.function.Supplier<String> messageSupplier, Throwable throwable);

    /**
     * Logs a message at the {@link org.elasticsearch.logging.Level#ERROR ERROR} level.
     *
     * @param message the message string to be logged
     */
    void error(String message);

    /**
     * Logs a message at the {@link org.elasticsearch.logging.Level#ERROR ERROR} level.
     *
     * @param message   the message string to be logged
     * @param throwable A Throwable or null.
     */
    void error(String message, Throwable throwable);

    /**
     * Logs a message with parameters at error level.
     *
     * @param message the message to log
     * @param p0      parameter to the message.
     */
    void error(String message, Object p0);

    /**
     * Logs a message with parameters at error level.
     *
     * @param message the message to log
     * @param p0      parameter to the message.
     * @param p1      parameter to the message.
     */
    void error(String message, Object p0, Object p1);

    /**
     * Logs a message with parameters at error level.
     *
     * @param message the message to log
     * @param p0      parameter to the message.
     * @param p1      parameter to the message.
     * @param p2      parameter to the message.
     */
    void error(String message, Object p0, Object p1, Object p2);

    /**
     * Logs a message with parameters at error level.
     *
     * @param message the message to log
     * @param p0      parameter to the message.
     * @param p1      parameter to the message.
     * @param p2      parameter to the message.
     * @param p3      parameter to the message.
     */
    void error(String message, Object p0, Object p1, Object p2, Object p3);

    /**
     * Logs a message with parameters at error level.
     *
     * @param message the message to log
     * @param p0      parameter to the message.
     * @param p1      parameter to the message.
     * @param p2      parameter to the message.
     * @param p3      parameter to the message.
     * @param p4      parameter to the message.
     */
    void error(String message, Object p0, Object p1, Object p2, Object p3, Object p4);

    /**
     * Logs a message with parameters at error level.
     *
     * @param message the message to log
     * @param p0      parameter to the message.
     * @param p1      parameter to the message.
     * @param p2      parameter to the message.
     * @param p3      parameter to the message.
     * @param p4      parameter to the message.
     * @param p5      parameter to the message.
     */
    void error(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5);

    /**
     * Logs a message with parameters at the {@link org.elasticsearch.logging.Level#ERROR ERROR} level.
     *
     * @param message the message to log
     * @param params  parameters to the message.
     */
    void error(String message, Object... params);

    // -- warn

    /**
     * //modified
     * Logs a message which is only to be constructed if the logging level is the {@link org.elasticsearch.logging.Level#WARN WARN} level
     *
     * @param messageSupplier A function, which when called, produces the desired log string.
     */
    void warn(java.util.function.Supplier<String> messageSupplier);

    /**
     * Logs a message which is only to be constructed if the logging level is the {@link org.elasticsearch.logging.Level#WARN WARN} level
     *
     * @param messageSupplier A function, which when called, produces the desired log string.
     * @param throwable       A Throwable or null.
     */
    void warn(java.util.function.Supplier<String> messageSupplier, Throwable throwable);

    /**
     * Logs a message at the {@link org.elasticsearch.logging.Level#WARN WARN} level.
     *
     * @param message the message string to be logged
     */
    void warn(String message);

    /**
     * Logs a message at the {@link org.elasticsearch.logging.Level#WARN WARN} level.
     *
     * @param message   the message string to be logged
     * @param throwable A Throwable or null.
     */
    void warn(String message, Throwable throwable);

    /**
     * Logs a message with parameters at warn level.
     *
     * @param message the message to log
     * @param p0      parameter to the message.
     */
    void warn(String message, Object p0);

    /**
     * Logs a message with parameters at warn level.
     *
     * @param message the message to log
     * @param p0      parameter to the message.
     * @param p1      parameter to the message.
     */
    void warn(String message, Object p0, Object p1);

    /**
     * Logs a message with parameters at warn level.
     *
     * @param message the message to log
     * @param p0      parameter to the message.
     * @param p1      parameter to the message.
     * @param p2      parameter to the message.
     */
    void warn(String message, Object p0, Object p1, Object p2);

    /**
     * Logs a message with parameters at warn level.
     *
     * @param message the message to log
     * @param p0      parameter to the message.
     * @param p1      parameter to the message.
     * @param p2      parameter to the message.
     * @param p3      parameter to the message.
     */
    void warn(String message, Object p0, Object p1, Object p2, Object p3);

    /**
     * Logs a message with parameters at warn level.
     *
     * @param message the message to log
     * @param p0      parameter to the message.
     * @param p1      parameter to the message.
     * @param p2      parameter to the message.
     * @param p3      parameter to the message.
     * @param p4      parameter to the message.
     */
    void warn(String message, Object p0, Object p1, Object p2, Object p3, Object p4);

    /**
     * Logs a message with parameters at warn level.
     *
     * @param message the message to log
     * @param p0      parameter to the message.
     * @param p1      parameter to the message.
     * @param p2      parameter to the message.
     * @param p3      parameter to the message.
     * @param p4      parameter to the message.
     * @param p5      parameter to the message.
     */
    void warn(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5);

    /**
     * Logs a message with parameters at the {@link org.elasticsearch.logging.Level#WARN WARN} level.
     *
     * @param message the message to log
     * @param params  parameters to the message.
     */
    void warn(String message, Object... params);

    // -- info

    /**
     * //modified
     * Logs a message which is only to be constructed if the logging level is the {@link org.elasticsearch.logging.Level#INFO INFO} level
     *
     * @param messageSupplier A function, which when called, produces the desired log string.
     */
    void info(java.util.function.Supplier<String> messageSupplier);

    /**
     * Logs a message which is only to be constructed if the logging level is the {@link org.elasticsearch.logging.Level#INFO INFO} level
     *
     * @param messageSupplier A function, which when called, produces the desired log string.
     * @param throwable       A Throwable or null.
     */
    void info(java.util.function.Supplier<String> messageSupplier, Throwable throwable);

    /**
     * Logs a message at the {@link org.elasticsearch.logging.Level#INFO INFO} level.
     *
     * @param message the message string to be logged
     */
    void info(String message);

    /**
     * Logs a message at the {@link org.elasticsearch.logging.Level#INFO INFO} level.
     *
     * @param message   the message string to be logged
     * @param throwable A Throwable or null.
     */
    void info(String message, Throwable throwable);

    /**
     * Logs a message with parameters at info level.
     *
     * @param message the message to log
     * @param p0      parameter to the message.
     */
    void info(String message, Object p0);

    /**
     * Logs a message with parameters at info level.
     *
     * @param message the message to log
     * @param p0      parameter to the message.
     * @param p1      parameter to the message.
     */
    void info(String message, Object p0, Object p1);

    /**
     * Logs a message with parameters at info level.
     *
     * @param message the message to log
     * @param p0      parameter to the message.
     * @param p1      parameter to the message.
     * @param p2      parameter to the message.
     */
    void info(String message, Object p0, Object p1, Object p2);

    /**
     * Logs a message with parameters at info level.
     *
     * @param message the message to log
     * @param p0      parameter to the message.
     * @param p1      parameter to the message.
     * @param p2      parameter to the message.
     * @param p3      parameter to the message.
     */
    void info(String message, Object p0, Object p1, Object p2, Object p3);

    /**
     * Logs a message with parameters at info level.
     *
     * @param message the message to log
     * @param p0      parameter to the message.
     * @param p1      parameter to the message.
     * @param p2      parameter to the message.
     * @param p3      parameter to the message.
     * @param p4      parameter to the message.
     */
    void info(String message, Object p0, Object p1, Object p2, Object p3, Object p4);

    /**
     * Logs a message with parameters at info level.
     *
     * @param message the message to log
     * @param p0      parameter to the message.
     * @param p1      parameter to the message.
     * @param p2      parameter to the message.
     * @param p3      parameter to the message.
     * @param p4      parameter to the message.
     * @param p5      parameter to the message.
     */
    void info(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5);

    /**
     * Logs a message with parameters at the {@link org.elasticsearch.logging.Level#INFO INFO} level.
     *
     * @param message the message to log
     * @param params  parameters to the message.
     */
    void info(String message, Object... params);

    // -- debug

    /**
     * //modified
     * Logs a message which is only to be constructed if the logging level is the {@link org.elasticsearch.logging.Level#DEBUG DEBUG} level
     *
     * @param messageSupplier A function, which when called, produces the desired log string.
     */
    void debug(java.util.function.Supplier<String> messageSupplier);

    /**
     * Logs a message which is only to be constructed if the logging level is the {@link org.elasticsearch.logging.Level#DEBUG DEBUG} level
     *
     * @param messageSupplier A function, which when called, produces the desired log string.
     * @param throwable       A Throwable or null.
     */
    void debug(java.util.function.Supplier<String> messageSupplier, Throwable throwable);

    /**
     * Logs a message at the {@link org.elasticsearch.logging.Level#DEBUG DEBUG} level.
     *
     * @param message the message string to be logged
     */
    void debug(String message);

    /**
     * Logs a message at the {@link org.elasticsearch.logging.Level#DEBUG DEBUG} level.
     *
     * @param message   the message string to be logged
     * @param throwable A Throwable or null.
     */
    void debug(String message, Throwable throwable);

    /**
     * Logs a message with parameters at debug level.
     *
     * @param message the message to log
     * @param p0      parameter to the message.
     */
    void debug(String message, Object p0);

    /**
     * Logs a message with parameters at debug level.
     *
     * @param message the message to log
     * @param p0      parameter to the message.
     * @param p1      parameter to the message.
     */
    void debug(String message, Object p0, Object p1);

    /**
     * Logs a message with parameters at debug level.
     *
     * @param message the message to log
     * @param p0      parameter to the message.
     * @param p1      parameter to the message.
     * @param p2      parameter to the message.
     */
    void debug(String message, Object p0, Object p1, Object p2);

    /**
     * Logs a message with parameters at debug level.
     *
     * @param message the message to log
     * @param p0      parameter to the message.
     * @param p1      parameter to the message.
     * @param p2      parameter to the message.
     * @param p3      parameter to the message.
     */
    void debug(String message, Object p0, Object p1, Object p2, Object p3);

    /**
     * Logs a message with parameters at debug level.
     *
     * @param message the message to log
     * @param p0      parameter to the message.
     * @param p1      parameter to the message.
     * @param p2      parameter to the message.
     * @param p3      parameter to the message.
     * @param p4      parameter to the message.
     */
    void debug(String message, Object p0, Object p1, Object p2, Object p3, Object p4);

    /**
     * Logs a message with parameters at debug level.
     *
     * @param message the message to log
     * @param p0      parameter to the message.
     * @param p1      parameter to the message.
     * @param p2      parameter to the message.
     * @param p3      parameter to the message.
     * @param p4      parameter to the message.
     * @param p5      parameter to the message.
     */
    void debug(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5);

    /**
     * Logs a message with parameters at the {@link org.elasticsearch.logging.Level#DEBUG DEBUG} level.
     *
     * @param message the message to log
     * @param params  parameters to the message.
     */
    void debug(String message, Object... params);

    // -- trace

    /**
     * //modified
     * Logs a message which is only to be constructed if the logging level is the {@link org.elasticsearch.logging.Level#TRACE TRACE} level
     *
     * @param messageSupplier A function, which when called, produces the desired log string.
     */
    void trace(java.util.function.Supplier<String> messageSupplier);

    /**
     * Logs a message which is only to be constructed if the logging level is the {@link org.elasticsearch.logging.Level#TRACE TRACE} level
     *
     * @param messageSupplier A function, which when called, produces the desired log string.
     * @param throwable       A Throwable or null.
     */
    void trace(java.util.function.Supplier<String> messageSupplier, Throwable throwable);

    /**
     * Logs a message at the {@link org.elasticsearch.logging.Level#TRACE TRACE} level.
     *
     * @param message the message string to be logged
     */
    void trace(String message);

    /**
     * Logs a message at the {@link org.elasticsearch.logging.Level#TRACE TRACE} level.
     *
     * @param message   the message string to be logged
     * @param throwable A Throwable or null.
     */
    void trace(String message, Throwable throwable);

    /**
     * Logs a message with parameters at trace level.
     *
     * @param message the message to log
     * @param p0      parameter to the message.
     */
    void trace(String message, Object p0);

    /**
     * Logs a message with parameters at trace level.
     *
     * @param message the message to log
     * @param p0      parameter to the message.
     * @param p1      parameter to the message.
     */
    void trace(String message, Object p0, Object p1);

    /**
     * Logs a message with parameters at trace level.
     *
     * @param message the message to log
     * @param p0      parameter to the message.
     * @param p1      parameter to the message.
     * @param p2      parameter to the message.
     */
    void trace(String message, Object p0, Object p1, Object p2);

    /**
     * Logs a message with parameters at trace level.
     *
     * @param message the message to log
     * @param p0      parameter to the message.
     * @param p1      parameter to the message.
     * @param p2      parameter to the message.
     * @param p3      parameter to the message.
     */
    void trace(String message, Object p0, Object p1, Object p2, Object p3);

    /**
     * Logs a message with parameters at trace level.
     *
     * @param message the message to log
     * @param p0      parameter to the message.
     * @param p1      parameter to the message.
     * @param p2      parameter to the message.
     * @param p3      parameter to the message.
     * @param p4      parameter to the message.
     */
    void trace(String message, Object p0, Object p1, Object p2, Object p3, Object p4);

    /**
     * Logs a message with parameters at trace level.
     *
     * @param message the message to log
     * @param p0      parameter to the message.
     * @param p1      parameter to the message.
     * @param p2      parameter to the message.
     * @param p3      parameter to the message.
     * @param p4      parameter to the message.
     * @param p5      parameter to the message.
     */
    void trace(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5);

    /**
     * Logs a message with parameters at the {@link org.elasticsearch.logging.Level#TRACE TRACE} level.
     *
     * @param message the message to log
     * @param params  parameters to the message.
     */
    void trace(String message, Object... params);

}
