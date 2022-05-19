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

    void log(Level level, Object message);

    void log(Level level, java.util.function.Supplier<String> msgSupplier, Throwable thrown);

    Level getLevel();

    String getName();

    boolean isErrorEnabled();

    boolean isWarnEnabled();

    boolean isInfoEnabled();

    boolean isDebugEnabled();

    boolean isTraceEnabled();

    boolean isEnabled(Level level);

    // -- fatal
    void fatal(Object message);

    void fatal(Object message, Throwable thrown);

    void fatal(java.util.function.Supplier<String> msgSupplier);

    void fatal(java.util.function.Supplier<String> msgSupplier, Throwable thrown);

    void fatal(String messagePattern, java.util.function.Supplier<String> paramSupplier);

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
    void error(Object message);

    void error(Object message, Throwable thrown);

    void error(java.util.function.Supplier<String> msgSupplier);

    void error(java.util.function.Supplier<String> msgSupplier, Throwable thrown);

    void error(String messagePattern, java.util.function.Supplier<String> paramSupplier);

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
    void warn(Object message);

    void warn(Object message, Throwable thrown);

    void warn(java.util.function.Supplier<String> msgSupplier);

    void warn(java.util.function.Supplier<String> msgSupplier, Throwable thrown);

    void warn(String messagePattern, java.util.function.Supplier<String> paramSupplier);

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
    void info(Object message);

    void info(Object message, Throwable thrown);

    void info(java.util.function.Supplier<String> msgSupplier);

    void info(java.util.function.Supplier<String> msgSupplier, Throwable thrown);

    void info(String messagePattern, java.util.function.Supplier<String> paramSupplier);

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
    void debug(Object message);

    void debug(Object message, Throwable thrown);

    void debug(java.util.function.Supplier<String> msgSupplier);

    void debug(java.util.function.Supplier<String> msgSupplier, Throwable thrown);

    void debug(String messagePattern, java.util.function.Supplier<String> paramSupplier);

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
    void trace(Object message);

    void trace(Object message, Throwable thrown);

    void trace(java.util.function.Supplier<String> msgSupplier);

    void trace(java.util.function.Supplier<String> msgSupplier, Throwable thrown);

    void trace(String messagePattern, java.util.function.Supplier<String> paramSupplier);

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
