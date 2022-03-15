/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging;

public interface Logger {

    void log(Level level, Object message, Object... params);

    void log(Level level, Object message);

    void log(Level level, Message message, Throwable thrown);

    void log(Level level, java.util.function.Supplier<?> msgSupplier, Throwable thrown);

    Level getLevel();

    String getName();

    boolean isInfoEnabled();

    boolean isTraceEnabled();

    boolean isDebugEnabled();

    boolean isErrorEnabled();

    boolean isWarnEnabled();

    void log(Level level, Message message);

    // -- debug
    void debug(Message message);

    void debug(Message message, Throwable thrown);

    void debug(java.util.function.Supplier<?> msgSupplier, Throwable thrown);

    void debug(String messagePattern, java.util.function.Supplier<?> paramSupplier);

    void debug(String message);

    void debug(String message, Object p0);

    void debug(String message, Object p0, Object p1);

    void debug(String message, Object p0, Object p1, Object p2);

    void debug(String message, Object p0, Object p1, Object p2, Object p3);

    void debug(String message, Object p0, Object p1, Object p2, Object p3, Object p4);

    void debug(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5);

    void debug(String message, Object... params);

    void debug(java.util.function.Supplier<?> msgSupplier);

    // -- error
    void error(Object message);

    void error(Message message);

    void error(Throwable e);

    void error(Message message, Throwable thrown);

    void error(java.util.function.Supplier<?> msgSupplier);

    void error(java.util.function.Supplier<?> msgSupplier, Throwable thrown);

    void error(String message);

    void error(String message, Object p0);

    void error(String message, Object p0, Object p1);

    void error(String message, Object p0, Object p1, Object p2);

    void error(String message, Object p0, Object p1, Object p2, Object p3);

    void error(String message, Object p0, Object p1, Object p2, Object p3, Object p4);

    void error(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5);

    void error(String message, Object... params);

    // -- info
    void info(Object message);

    void info(Message message);

    void info(Message message, Throwable thrown);

    void info(java.util.function.Supplier<?> msgSupplier);

    void info(java.util.function.Supplier<?> msgSupplier, Throwable thrown);

    void info(String message);

    void info(String message, Object p0);

    void info(String message, Object p0, Object p1);

    void info(String message, Object p0, Object p1, Object p2);

    void info(String message, Object p0, Object p1, Object p2, Object p3);

    void info(String message, Object p0, Object p1, Object p2, Object p3, Object p4);

    void info(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5);

    void info(String message, Object... params);

    // -- trace
    void trace(Message message);

    void trace(Message message, Throwable thrown);

    void trace(java.util.function.Supplier<?> msgSupplier);

    void trace(java.util.function.Supplier<?> msgSupplier, Throwable thrown);

    void trace(String message);

    void trace(String message, Object p0);

    void trace(String message, Object p0, Object p1);

    void trace(String message, Object p0, Object p1, Object p2);

    void trace(String message, Object p0, Object p1, Object p2, Object p3);

    void trace(String message, Object p0, Object p1, Object p2, Object p3, Object p4);

    void trace(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5);

    void trace(String message, Object... params);

    // -- warn
    void warn(Object message);

    void warn(Message message);

    void warn(Message message, Throwable thrown);

    void warn(java.util.function.Supplier<?> msgSupplier);

    void warn(java.util.function.Supplier<?> msgSupplier, Throwable thrown);

    void warn(String message);

    void warn(String message, Object p0);

    void warn(String message, Object p0, Object p1);

    void warn(String message, Object p0, Object p1, Object p2);

    void warn(String message, Object p0, Object p1, Object p2, Object p3);

    void warn(String message, Object p0, Object p1, Object p2, Object p3, Object p4);

    void warn(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5);

    void warn(String message, Object... params);

    void warn(Throwable e);

    // -- fatal
    void fatal(String message, Throwable thrown);

    boolean isLoggable(Level level);

    // TODO:
}
