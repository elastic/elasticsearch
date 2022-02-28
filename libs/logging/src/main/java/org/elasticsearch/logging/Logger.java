/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging;

import java.io.IOException;
import java.util.Objects;

public interface Logger {

    String getName();

    boolean isLoggable(Level level);

    void log(Level level, Object message, Object... params);

    void log(Level level, String message, Object... params);

    void log(Level level, String message, Throwable throwable);

    // void log(String message, java.util.function.Supplier<?>... paramSuppliers);
    default void log(Level level, Object message) {
        Objects.requireNonNull(message);
        if (isLoggable(Objects.requireNonNull(level))) {
            this.log(level, message, (Throwable) null);
        }
    }

    default void log(Level level, Message message) {
        Objects.requireNonNull(message);
        if (isLoggable(Objects.requireNonNull(level))) {
            this.log(level, message.getFormattedMessage(), (Throwable) null);
        }
    }

    default void log(Level level, Message message, Throwable thrown) {
        Objects.requireNonNull(message);
        if (isLoggable(Objects.requireNonNull(level))) {
            this.log(level, message.getFormattedMessage(), thrown);
        }
    }

    default void log(Level level, java.util.function.Supplier<?> msgSupplier, Throwable thrown) {
        Objects.requireNonNull(msgSupplier);
        if (isLoggable(Objects.requireNonNull(level))) {
            this.log(level, msgSupplier.get(), thrown);
        }
    }

    // default void log(Level level, Supplier<String> msgSupplier, Throwable thrown) {
    // Objects.requireNonNull(msgSupplier);
    // if (isLoggable(Objects.requireNonNull(level))) {
    // this.log(level, msgSupplier.get(), thrown);
    // }
    // }

    // ---

    default boolean isInfoEnabled() {
        return isLoggable(Level.INFO);
    }

    default boolean isTraceEnabled() {
        return isLoggable(Level.TRACE);
    }

    default boolean isDebugEnabled() {
        return isLoggable(Level.DEBUG);
    }

    default boolean isErrorEnabled() {
        return isLoggable(Level.ERROR);
    }

    default boolean isWarnEnabled() {
        return isLoggable(Level.WARN);
    }

    // -- debug
    default void debug(Message message) {
        log(Level.DEBUG, message);
    }

    default void debug(Message message, Throwable thrown) {
        log(Level.DEBUG, message, thrown);
    }

    default void debug(java.util.function.Supplier<?> msgSupplier, Throwable thrown) {
        log(Level.DEBUG, msgSupplier.get(), thrown);
    }

    default void debug(String messagePattern, java.util.function.Supplier<?> paramSupplier) {
        log(Level.DEBUG, messagePattern, paramSupplier);
    }

    default void debug(String message) {
        log(Level.DEBUG, message);
    }

    default void debug(String message, Object p0) {
        log(Level.DEBUG, message, p0);
    }

    default void debug(String message, Object p0, Object p1) {
        log(Level.DEBUG, message, p0, p1);
    }

    default void debug(String message, Object p0, Object p1, Object p2) {
        log(Level.DEBUG, message, p0, p1, p2);
    }

    default void debug(String message, Object p0, Object p1, Object p2, Object p3) {
        log(Level.DEBUG, message, p0, p1, p2, p3);
    }

    default void debug(String message, Object p0, Object p1, Object p2, Object p3, Object p4) {
        log(Level.DEBUG, message, p0, p1, p2, p3, p4);
    }

    default void debug(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5) {
        log(Level.DEBUG, message, p0, p1, p2, p3, p4, p5);
    }

    default void debug(String message, Object... params) {
        log(Level.DEBUG, message, params);
    }

    default void debug(java.util.function.Supplier<?> msgSupplier) {
        log(Level.ERROR, msgSupplier.get());
    }

    // -- error
    default void error(Message message) {
        log(Level.ERROR, message);
    }

    default void error(Message message, Throwable thrown) {
        log(Level.ERROR, message, thrown);
    }

    default void error(java.util.function.Supplier<?> msgSupplier) {
        log(Level.ERROR, msgSupplier.get());
    }

    default void error(java.util.function.Supplier<?> msgSupplier, Throwable thrown) {
        log(Level.ERROR, msgSupplier.get(), thrown);
    }

    default void error(String message) {
        log(Level.ERROR, message);
    }

    default void error(String message, Object p0) {
        log(Level.ERROR, message, p0);
    }

    default void error(String message, Object p0, Object p1) {
        log(Level.ERROR, message, p0, p1);
    }

    default void error(String message, Object p0, Object p1, Object p2) {
        log(Level.ERROR, message, p0, p1, p2);
    }

    default void error(String message, Object p0, Object p1, Object p2, Object p3) {
        log(Level.ERROR, message, p0, p1, p2, p3);
    }

    default void error(String message, Object p0, Object p1, Object p2, Object p3, Object p4) {
        log(Level.ERROR, message, p0, p1, p2, p3, p4);
    }

    default void error(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5) {
        log(Level.ERROR, message, p0, p1, p2, p3, p4, p5);
    }

    default void error(String message, Object... params) {
        log(Level.ERROR, message, params);
    }

    // -- info
    default void info(Message message) {
        log(Level.INFO, message);
    }

    default void info(Message message, Throwable thrown) {
        log(Level.INFO, message, thrown);
    }

    default void info(java.util.function.Supplier<?> msgSupplier) {
        log(Level.INFO, msgSupplier.get());
    }

    default void info(java.util.function.Supplier<?> msgSupplier, Throwable thrown) {
        log(Level.INFO, msgSupplier.get(), thrown);
    }

    default void info(String message) {
        log(Level.INFO, message);
    }

    default void info(String message, Object p0) {
        log(Level.INFO, message, p0);
    }

    default void info(String message, Object p0, Object p1) {
        log(Level.INFO, message, p0, p1);
    }

    default void info(String message, Object p0, Object p1, Object p2) {
        log(Level.INFO, message, p0, p1, p2);
    }

    default void info(String message, Object p0, Object p1, Object p2, Object p3) {
        log(Level.INFO, message, p0, p1, p2, p3);
    }

    default void info(String message, Object p0, Object p1, Object p2, Object p3, Object p4) {
        log(Level.INFO, message, p0, p1, p2, p3, p4);
    }

    default void info(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5) {
        log(Level.INFO, message, p0, p1, p2, p3, p4, p5);
    }

    default void info(String message, Object... params) {
        log(Level.INFO, message, params);
    }

    // -- trace
    default void trace(Message message) {
        log(Level.TRACE, message);
    }

    default void trace(Message message, Throwable thrown) {
        log(Level.TRACE, message, thrown);
    }

    default void trace(java.util.function.Supplier<?> msgSupplier) {
        log(Level.TRACE, msgSupplier.get());
    }

    default void trace(java.util.function.Supplier<?> msgSupplier, Throwable thrown) {
        log(Level.TRACE, msgSupplier.get(), thrown);
    }

    default void trace(String message) {
        log(Level.TRACE, message);
    }

    default void trace(String message, Object p0) {
        log(Level.TRACE, message, p0);
    }

    default void trace(String message, Object p0, Object p1) {
        log(Level.TRACE, message, p0, p1);
    }

    default void trace(String message, Object p0, Object p1, Object p2) {
        log(Level.TRACE, message, p0, p1, p2);
    }

    default void trace(String message, Object p0, Object p1, Object p2, Object p3) {
        log(Level.TRACE, message, p0, p1, p2, p3);
    }

    default void trace(String message, Object p0, Object p1, Object p2, Object p3, Object p4) {
        log(Level.TRACE, message, p0, p1, p2, p3, p4);
    }

    default void trace(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5) {
        log(Level.TRACE, message, p0, p1, p2, p3, p4, p5);
    }

    default void trace(String message, Object... params) {
        log(Level.TRACE, message, params);
    }

    // -- warn
    default void warn(Message message) {
        log(Level.WARN, message);
    }

    default void warn(Message message, Throwable thrown) {
        log(Level.WARN, message, thrown);
    }

    default void warn(java.util.function.Supplier<?> msgSupplier) {
        log(Level.WARN, msgSupplier.get());
    }

    default void warn(java.util.function.Supplier<?> msgSupplier, Throwable thrown) {
        log(Level.WARN, msgSupplier.get(), thrown);
    }

    default void warn(String message) {
        log(Level.WARN, message);
    }

    default void warn(String message, Object p0) {
        log(Level.WARN, message, p0);
    }

    default void warn(String message, Object p0, Object p1) {
        log(Level.WARN, message, p0, p1);
    }

    default void warn(String message, Object p0, Object p1, Object p2) {
        log(Level.WARN, message, p0, p1, p2);
    }

    default void warn(String message, Object p0, Object p1, Object p2, Object p3) {
        log(Level.WARN, message, p0, p1, p2, p3);
    }

    default void warn(String message, Object p0, Object p1, Object p2, Object p3, Object p4) {
        log(Level.WARN, message, p0, p1, p2, p3, p4);
    }

    default void warn(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5) {
        log(Level.WARN, message, p0, p1, p2, p3, p4, p5);
    }

    default void warn(String message, Object... params) {
        log(Level.WARN, message, params);
    }
    // -- fatal
    default void fatal(String message, Throwable thrown) {
        log(Level.TRACE, message, thrown);
    }



    // TODO:
}
