/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.support;

import org.elasticsearch.logging.Level;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.logging.Message;

import java.util.function.Supplier;

/**
 * A logger that doesn't log anything.
 */
public class NoOpLogger implements Logger {

    public static NoOpLogger INSTANCE = new NoOpLogger();

    private NoOpLogger() {

    }

    @Override
    public void log(Level level, Object message, Object... params) {

    }

    @Override
    public void log(Level level, Object message) {

    }

    @Override
    public void log(Level level, Message message, Throwable thrown) {

    }

    @Override
    public void log(Level level, Supplier<?> msgSupplier, Throwable thrown) {

    }

    @Override
    public Level getLevel() {
        return null;
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public boolean isInfoEnabled() {
        return false;
    }

    @Override
    public boolean isTraceEnabled() {
        return false;
    }

    @Override
    public boolean isDebugEnabled() {
        return false;
    }

    @Override
    public boolean isErrorEnabled() {
        return false;
    }

    @Override
    public boolean isWarnEnabled() {
        return false;
    }

    @Override
    public void log(Level level, Message message) {

    }

    @Override
    public void debug(Message message) {

    }

    @Override
    public void debug(Message message, Throwable thrown) {

    }

    @Override
    public void debug(Supplier<?> msgSupplier, Throwable thrown) {

    }

    @Override
    public void debug(String messagePattern, Supplier<?> paramSupplier) {

    }

    @Override
    public void debug(String message) {

    }

    @Override
    public void debug(String message, Object p0) {

    }

    @Override
    public void debug(String message, Object p0, Object p1) {

    }

    @Override
    public void debug(String message, Object p0, Object p1, Object p2) {

    }

    @Override
    public void debug(String message, Object p0, Object p1, Object p2, Object p3) {

    }

    @Override
    public void debug(String message, Object p0, Object p1, Object p2, Object p3, Object p4) {

    }

    @Override
    public void debug(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5) {

    }

    @Override
    public void debug(String message, Object... params) {

    }

    @Override
    public void debug(Supplier<?> msgSupplier) {

    }

    @Override
    public void error(Object message) {

    }

    @Override
    public void error(Message message) {

    }

    @Override
    public void error(Throwable e) {

    }

    @Override
    public void error(Message message, Throwable thrown) {

    }

    @Override
    public void error(Supplier<?> msgSupplier) {

    }

    @Override
    public void error(Supplier<?> msgSupplier, Throwable thrown) {

    }

    @Override
    public void error(String message) {

    }

    @Override
    public void error(String message, Object p0) {

    }

    @Override
    public void error(String message, Object p0, Object p1) {

    }

    @Override
    public void error(String message, Object p0, Object p1, Object p2) {

    }

    @Override
    public void error(String message, Object p0, Object p1, Object p2, Object p3) {

    }

    @Override
    public void error(String message, Object p0, Object p1, Object p2, Object p3, Object p4) {

    }

    @Override
    public void error(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5) {

    }

    @Override
    public void error(String message, Object... params) {

    }

    @Override
    public void info(Object message) {

    }

    @Override
    public void info(Message message) {

    }

    @Override
    public void info(Message message, Throwable thrown) {

    }

    @Override
    public void info(Supplier<?> msgSupplier) {

    }

    @Override
    public void info(Supplier<?> msgSupplier, Throwable thrown) {

    }

    @Override
    public void info(String message) {

    }

    @Override
    public void info(String message, Object p0) {

    }

    @Override
    public void info(String message, Object p0, Object p1) {

    }

    @Override
    public void info(String message, Object p0, Object p1, Object p2) {

    }

    @Override
    public void info(String message, Object p0, Object p1, Object p2, Object p3) {

    }

    @Override
    public void info(String message, Object p0, Object p1, Object p2, Object p3, Object p4) {

    }

    @Override
    public void info(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5) {

    }

    @Override
    public void info(String message, Object... params) {

    }

    @Override
    public void trace(Message message) {

    }

    @Override
    public void trace(Message message, Throwable thrown) {

    }

    @Override
    public void trace(Supplier<?> msgSupplier) {

    }

    @Override
    public void trace(Supplier<?> msgSupplier, Throwable thrown) {

    }

    @Override
    public void trace(String message) {

    }

    @Override
    public void trace(String message, Object p0) {

    }

    @Override
    public void trace(String message, Object p0, Object p1) {

    }

    @Override
    public void trace(String message, Object p0, Object p1, Object p2) {

    }

    @Override
    public void trace(String message, Object p0, Object p1, Object p2, Object p3) {

    }

    @Override
    public void trace(String message, Object p0, Object p1, Object p2, Object p3, Object p4) {

    }

    @Override
    public void trace(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5) {

    }

    @Override
    public void trace(String message, Object... params) {

    }

    @Override
    public void warn(Object message) {

    }

    @Override
    public void warn(Message message) {

    }

    @Override
    public void warn(Message message, Throwable thrown) {

    }

    @Override
    public void warn(Supplier<?> msgSupplier) {

    }

    @Override
    public void warn(Supplier<?> msgSupplier, Throwable thrown) {

    }

    @Override
    public void warn(String message) {

    }

    @Override
    public void warn(String message, Object p0) {

    }

    @Override
    public void warn(String message, Object p0, Object p1) {

    }

    @Override
    public void warn(String message, Object p0, Object p1, Object p2) {

    }

    @Override
    public void warn(String message, Object p0, Object p1, Object p2, Object p3) {

    }

    @Override
    public void warn(String message, Object p0, Object p1, Object p2, Object p3, Object p4) {

    }

    @Override
    public void warn(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5) {

    }

    @Override
    public void warn(String message, Object... params) {

    }

    @Override
    public void warn(Throwable e) {

    }

    @Override
    public void fatal(String message, Throwable thrown) {

    }

    @Override
    public boolean isLoggable(Level level) {
        return false;
    }
}
