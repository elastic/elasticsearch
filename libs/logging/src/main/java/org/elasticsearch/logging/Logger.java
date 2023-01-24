/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging;

import java.util.function.Supplier;

/**
 * Main interface for logging. Most operations are done through this interface.
 * This interface is heavily influenced by org.apache.logging.log4j2.Logger.
 * The most notable difference is lack of methods with Marker, LogBuilder and
 * the message supplier is {@code java.util.function.Supplier<String>}
 */
public interface Logger {
    /**
     * Logs a message String with the given level.
     *
     * @param level   the logging level
     * @param message the message to log.
     */
    void log(Level level, String message);

    /**
     * Logs a lazily supplied String message associated with a given throwable. If the logger is currently
     * enabled for the specified log message level, then a message is logged that is the result produced
     * by the given supplier function.
     * @param level           the logging level
     * @param messageSupplier A function, which when called, produces the desired log String message;
     * @param throwable       A Throwable associated with the log message.
     */
    void log(Level level, Supplier<String> messageSupplier, Throwable throwable);

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
     * Logs a lazily supplied String message. If the logger is currently enabled for
     * {@link org.elasticsearch.logging.Level#FATAL FATAL} level, then a message is logged
     * that is the result produced by the given supplier function.
     * @param messageSupplier A function, which when called, produces the desired log String message;
     */
    void fatal(Supplier<String> messageSupplier);

    /**
     * Logs a lazily supplied String message associated with a given throwable. If the logger is currently
     * enabled for {@link org.elasticsearch.logging.Level#FATAL FATAL}  level, then a message is logged
     * that is the result produced by the given supplier function.
     * @param messageSupplier A function, which when called, produces the desired log String message;
     * @param throwable       A Throwable associated with the log message.
     */
    void fatal(Supplier<String> messageSupplier, Throwable throwable);

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
     * @param throwable A Throwable associated with the log message.
     */
    void fatal(String message, Throwable throwable);

    /**
     * Logs a message with parameters at the {@link org.elasticsearch.logging.Level#FATAL FATAL} level.
     *
     * @param message the message to log
     * @param params  parameters to the message.
     */
    void fatal(String message, Object... params);

    // -- error

    /**
     * Logs a lazily supplied String message. If the logger is currently enabled for
     * {@link org.elasticsearch.logging.Level#ERROR ERROR} level, then a message is logged
     * that is the result produced by the given supplier function.
     * @param messageSupplier A function, which when called, produces the desired log String message;
     */
    void error(Supplier<String> messageSupplier);

    /**
     * Logs a lazily supplied String message associated with a given throwable. If the logger is currently
     * enabled for {@link org.elasticsearch.logging.Level#ERROR ERROR}  level, then a message is logged
     * that is the result produced by the given supplier function.
     * @param messageSupplier A function, which when called, produces the desired log String message;
     * @param throwable       A Throwable associated with the log message.
     */
    void error(Supplier<String> messageSupplier, Throwable throwable);

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
     * @param throwable A Throwable associated with the log message.
     */
    void error(String message, Throwable throwable);

    /**
     * Logs a message with parameters at the {@link org.elasticsearch.logging.Level#ERROR ERROR} level.
     *
     * @param message the message to log
     * @param params  parameters to the message.
     */
    void error(String message, Object... params);

    // -- warn

    /**
     * Logs a lazily supplied String message. If the logger is currently enabled for
     * {@link org.elasticsearch.logging.Level#WARN WARN} level, then a message is logged
     * that is the result produced by the given supplier function.
     * @param messageSupplier A function, which when called, produces the desired log String message;
     */
    void warn(Supplier<String> messageSupplier);

    /**
     * Logs a lazily supplied String message associated with a given throwable. If the logger is currently
     * enabled for {@link org.elasticsearch.logging.Level#WARN WARN}  level, then a message is logged
     * that is the result produced by the given supplier function.
     * @param messageSupplier A function, which when called, produces the desired log String message;
     * @param throwable       A Throwable associated with the log message.
     */
    void warn(Supplier<String> messageSupplier, Throwable throwable);

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
     * @param throwable A Throwable associated with the log message.
     */
    void warn(String message, Throwable throwable);

    /**
     * Logs a message with parameters at the {@link org.elasticsearch.logging.Level#WARN WARN} level.
     *
     * @param message the message to log
     * @param params  parameters to the message.
     */
    void warn(String message, Object... params);

    // -- info

    /**
     * Logs a lazily supplied String message. If the logger is currently enabled for
     * {@link org.elasticsearch.logging.Level#INFO INFO} level, then a message is logged
     * that is the result produced by the given supplier function.
     * @param messageSupplier A function, which when called, produces the desired log String message;
     */
    void info(Supplier<String> messageSupplier);

    /**
     * Logs a lazily supplied String message associated with a given throwable. If the logger is currently
     * enabled for {@link org.elasticsearch.logging.Level#INFO INFO}  level, then a message is logged
     * that is the result produced by the given supplier function.
     * @param messageSupplier A function, which when called, produces the desired log String message;
     * @param throwable       A Throwable associated with the log message.
     */
    void info(Supplier<String> messageSupplier, Throwable throwable);

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
     * @param throwable A Throwable associated with the log message.
     */
    void info(String message, Throwable throwable);

    /**
     * Logs a message with parameters at the {@link org.elasticsearch.logging.Level#INFO INFO} level.
     *
     * @param message the message to log
     * @param params  parameters to the message.
     */
    void info(String message, Object... params);

    // -- debug

    /**
     * Logs a lazily supplied String message. If the logger is currently enabled for
     * {@link org.elasticsearch.logging.Level#DEBUG DEBUG} level, then a message is logged
     * that is the result produced by the given supplier function.
     * @param messageSupplier A function, which when called, produces the desired log String message;
     */
    void debug(Supplier<String> messageSupplier);

    /**
     * Logs a lazily supplied String message associated with a given throwable. If the logger is currently
     * enabled for {@link org.elasticsearch.logging.Level#DEBUG DEBUG}  level, then a message is logged
     * that is the result produced by the given supplier function.
     * @param messageSupplier A function, which when called, produces the desired log String message;
     * @param throwable       A Throwable associated with the log message.
     */
    void debug(Supplier<String> messageSupplier, Throwable throwable);

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
     * @param throwable A Throwable associated with the log message.
     */
    void debug(String message, Throwable throwable);

    /**
     * Logs a message with parameters at the {@link org.elasticsearch.logging.Level#DEBUG DEBUG} level.
     *
     * @param message the message to log
     * @param params  parameters to the message.
     */
    void debug(String message, Object... params);

    // -- trace

    /**
     * Logs a lazily supplied String message. If the logger is currently enabled for
     * {@link org.elasticsearch.logging.Level#FATAL FATAL} level, then a message is logged
     * that is the result produced by the given supplier function.
     * @param messageSupplier A function, which when called, produces the desired log String message;
     */
    void trace(Supplier<String> messageSupplier);

    /**
     * Logs a lazily supplied String message associated with a given throwable. If the logger is currently
     * enabled for {@link org.elasticsearch.logging.Level#FATAL FATAL}  level, then a message is logged
     * that is the result produced by the given supplier function.
     * @param messageSupplier A function, which when called, produces the desired log String message;
     * @param throwable       A Throwable associated with the log message.
     */
    void trace(Supplier<String> messageSupplier, Throwable throwable);

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
     * @param throwable A Throwable associated with the log message.
     */
    void trace(String message, Throwable throwable);

    /**
     * Logs a message with parameters at the {@link org.elasticsearch.logging.Level#TRACE TRACE} level.
     *
     * @param message the message to log
     * @param params  parameters to the message.
     */
    void trace(String message, Object... params);

}
