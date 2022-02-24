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
    public String getName() {
        return null;
    }

    @Override
    public boolean isLoggable(Level level) {
        return false;
    }

    @Override
    public void log(Level level, Object message, Object... params) {

    }

    @Override
    public void log(Level level, String message, Object... params) {

    }

    @Override
    public void log(Level level, String message, Throwable throwable) {

    }

    @Override
    public void log(Level level, Object message) {
        Logger.super.log(level, message);
    }

    @Override
    public void log(Level level, Message message) {
        Logger.super.log(level, message);
    }

    @Override
    public void log(Level level, Message message, Throwable thrown) {
        Logger.super.log(level, message, thrown);
    }

    @Override
    public void log(Level level, java.util.function.Supplier<?> msgSupplier, Throwable thrown) {
        Logger.super.log(level, msgSupplier, thrown);
    }

    @Override
    public boolean isInfoEnabled() {
        return Logger.super.isInfoEnabled();
    }

    @Override
    public boolean isTraceEnabled() {
        return Logger.super.isTraceEnabled();
    }

    @Override
    public boolean isDebugEnabled() {
        return Logger.super.isDebugEnabled();
    }

    @Override
    public boolean isErrorEnabled() {
        return Logger.super.isErrorEnabled();
    }

    @Override
    public boolean isWarnEnabled() {
        return Logger.super.isWarnEnabled();
    }

    @Override
    public void debug(Message message) {
        Logger.super.debug(message);
    }

    @Override
    public void debug(Message message, Throwable thrown) {
        Logger.super.debug(message, thrown);
    }

    @Override
    public void debug(java.util.function.Supplier<?> msgSupplier, Throwable thrown) {
        Logger.super.debug(msgSupplier, thrown);
    }

    @Override
    public void debug(String message) {
        Logger.super.debug(message);
    }

    @Override
    public void debug(String message, Object p0) {
        Logger.super.debug(message, p0);
    }

    @Override
    public void debug(String message, Object p0, Object p1) {
        Logger.super.debug(message, p0, p1);
    }

    @Override
    public void debug(String message, Object p0, Object p1, Object p2) {
        Logger.super.debug(message, p0, p1, p2);
    }

    @Override
    public void debug(String message, Object p0, Object p1, Object p2, Object p3) {
        Logger.super.debug(message, p0, p1, p2, p3);
    }

    @Override
    public void debug(String message, Object p0, Object p1, Object p2, Object p3, Object p4) {
        Logger.super.debug(message, p0, p1, p2, p3, p4);
    }

    @Override
    public void debug(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5) {
        Logger.super.debug(message, p0, p1, p2, p3, p4, p5);
    }

    @Override
    public void debug(String message, Object... params) {
        Logger.super.debug(message, params);
    }

    @Override
    public void debug(java.util.function.Supplier<?> msgSupplier) {
        Logger.super.debug(msgSupplier);
    }

    @Override
    public void error(Message message) {
        Logger.super.error(message);
    }

    @Override
    public void error(Message message, Throwable thrown) {
        Logger.super.error(message, thrown);
    }

    @Override
    public void error(java.util.function.Supplier<?> msgSupplier) {
        Logger.super.error(msgSupplier);
    }

    @Override
    public void error(java.util.function.Supplier<?> msgSupplier, Throwable thrown) {
        Logger.super.error(msgSupplier, thrown);
    }

    @Override
    public void error(String message) {
        Logger.super.error(message);
    }

    @Override
    public void error(String message, Object p0) {
        Logger.super.error(message, p0);
    }

    @Override
    public void error(String message, Object p0, Object p1) {
        Logger.super.error(message, p0, p1);
    }

    @Override
    public void error(String message, Object p0, Object p1, Object p2) {
        Logger.super.error(message, p0, p1, p2);
    }

    @Override
    public void error(String message, Object p0, Object p1, Object p2, Object p3) {
        Logger.super.error(message, p0, p1, p2, p3);
    }

    @Override
    public void error(String message, Object p0, Object p1, Object p2, Object p3, Object p4) {
        Logger.super.error(message, p0, p1, p2, p3, p4);
    }

    @Override
    public void error(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5) {
        Logger.super.error(message, p0, p1, p2, p3, p4, p5);
    }

    @Override
    public void error(String message, Object... params) {
        Logger.super.error(message, params);
    }

    @Override
    public void info(Message message) {
        Logger.super.info(message);
    }

    @Override
    public void info(Message message, Throwable thrown) {
        Logger.super.info(message, thrown);
    }

    @Override
    public void info(java.util.function.Supplier<?> msgSupplier) {
        Logger.super.info(msgSupplier);
    }

    @Override
    public void info(java.util.function.Supplier<?> msgSupplier, Throwable thrown) {
        Logger.super.info(msgSupplier, thrown);
    }

    @Override
    public void info(String message) {
        Logger.super.info(message);
    }

    @Override
    public void info(String message, Object p0) {
        Logger.super.info(message, p0);
    }

    @Override
    public void info(String message, Object p0, Object p1) {
        Logger.super.info(message, p0, p1);
    }

    @Override
    public void info(String message, Object p0, Object p1, Object p2) {
        Logger.super.info(message, p0, p1, p2);
    }

    @Override
    public void info(String message, Object p0, Object p1, Object p2, Object p3) {
        Logger.super.info(message, p0, p1, p2, p3);
    }

    @Override
    public void info(String message, Object p0, Object p1, Object p2, Object p3, Object p4) {
        Logger.super.info(message, p0, p1, p2, p3, p4);
    }

    @Override
    public void info(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5) {
        Logger.super.info(message, p0, p1, p2, p3, p4, p5);
    }

    @Override
    public void info(String message, Object... params) {
        Logger.super.info(message, params);
    }

    @Override
    public void trace(Message message) {
        Logger.super.trace(message);
    }

    @Override
    public void trace(Message message, Throwable thrown) {
        Logger.super.trace(message, thrown);
    }

    @Override
    public void trace(java.util.function.Supplier<?> msgSupplier) {
        Logger.super.trace(msgSupplier);
    }

    @Override
    public void trace(java.util.function.Supplier<?> msgSupplier, Throwable thrown) {
        Logger.super.trace(msgSupplier, thrown);
    }

    @Override
    public void trace(String message) {
        Logger.super.trace(message);
    }

    @Override
    public void trace(String message, Object p0) {
        Logger.super.trace(message, p0);
    }

    @Override
    public void trace(String message, Object p0, Object p1) {
        Logger.super.trace(message, p0, p1);
    }

    @Override
    public void trace(String message, Object p0, Object p1, Object p2) {
        Logger.super.trace(message, p0, p1, p2);
    }

    @Override
    public void trace(String message, Object p0, Object p1, Object p2, Object p3) {
        Logger.super.trace(message, p0, p1, p2, p3);
    }

    @Override
    public void trace(String message, Object p0, Object p1, Object p2, Object p3, Object p4) {
        Logger.super.trace(message, p0, p1, p2, p3, p4);
    }

    @Override
    public void trace(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5) {
        Logger.super.trace(message, p0, p1, p2, p3, p4, p5);
    }

    @Override
    public void trace(String message, Object... params) {
        Logger.super.trace(message, params);
    }

    @Override
    public void warn(Message message) {
        Logger.super.warn(message);
    }

    @Override
    public void warn(Message message, Throwable thrown) {
        Logger.super.warn(message, thrown);
    }

    @Override
    public void warn(java.util.function.Supplier<?> msgSupplier) {
        Logger.super.warn(msgSupplier);
    }

    @Override
    public void warn(java.util.function.Supplier<?> msgSupplier, Throwable thrown) {
        Logger.super.warn(msgSupplier, thrown);
    }

    @Override
    public void warn(String message) {
        Logger.super.warn(message);
    }

    @Override
    public void warn(String message, Object p0) {
        Logger.super.warn(message, p0);
    }

    @Override
    public void warn(String message, Object p0, Object p1) {
        Logger.super.warn(message, p0, p1);
    }

    @Override
    public void warn(String message, Object p0, Object p1, Object p2) {
        Logger.super.warn(message, p0, p1, p2);
    }

    @Override
    public void warn(String message, Object p0, Object p1, Object p2, Object p3) {
        Logger.super.warn(message, p0, p1, p2, p3);
    }

    @Override
    public void warn(String message, Object p0, Object p1, Object p2, Object p3, Object p4) {
        Logger.super.warn(message, p0, p1, p2, p3, p4);
    }

    @Override
    public void warn(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5) {
        Logger.super.warn(message, p0, p1, p2, p3, p4, p5);
    }

    @Override
    public void warn(String message, Object... params) {
        Logger.super.warn(message, params);
    }
}
