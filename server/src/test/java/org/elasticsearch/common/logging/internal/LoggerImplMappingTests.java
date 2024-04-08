/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.logging.internal;

import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.logging.Level;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.ESTestCase;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LoggerImplMappingTests extends ESTestCase {
    org.apache.logging.log4j.Logger log4jLogger = Mockito.mock(org.apache.logging.log4j.Logger.class);
    Logger esLogger = new LoggerImpl(log4jLogger);

    ArgumentCaptor<org.apache.logging.log4j.util.Supplier<?>> msgSupplierCaptor = ArgumentCaptor.forClass(
        org.apache.logging.log4j.util.Supplier.class
    );
    ArgumentCaptor<Throwable> exceptionCaptor = ArgumentCaptor.forClass(Throwable.class);

    RuntimeException thrown = new RuntimeException();

    private void assertArguments() {
        assertThat(msgSupplierCaptor.getAllValues().stream().map(Supplier::get).collect(Collectors.toList()), contains("msg", "msg1"));
        assertThat(exceptionCaptor.getValue(), equalTo(thrown));
    }

    public void testFatalMethodsWithSupplier() {
        esLogger.fatal(() -> "msg");
        esLogger.fatal(() -> "msg1", thrown);

        verify(log4jLogger).fatal(msgSupplierCaptor.capture());
        verify(log4jLogger).fatal(msgSupplierCaptor.capture(), exceptionCaptor.capture());

        assertArguments();
    }

    public void testErrorMethodsWithSupplier() {
        esLogger.error(() -> "msg");
        esLogger.error(() -> "msg1", thrown);

        verify(log4jLogger).error(msgSupplierCaptor.capture());
        verify(log4jLogger).error(msgSupplierCaptor.capture(), exceptionCaptor.capture());

        assertArguments();
    }

    public void testWarnMethodsWithSupplier() {
        esLogger.warn(() -> "msg");
        esLogger.warn(() -> "msg1", thrown);

        verify(log4jLogger).warn(msgSupplierCaptor.capture());
        verify(log4jLogger).warn(msgSupplierCaptor.capture(), exceptionCaptor.capture());

        assertArguments();
    }

    public void testInfoMethodsWithSupplier() {
        esLogger.info(() -> "msg");
        esLogger.info(() -> "msg1", thrown);

        verify(log4jLogger).info(msgSupplierCaptor.capture());
        verify(log4jLogger).info(msgSupplierCaptor.capture(), exceptionCaptor.capture());

        assertArguments();
    }

    public void testDebugMethodsWithSupplier() {
        esLogger.debug(() -> "msg");
        esLogger.debug(() -> "msg1", thrown);

        verify(log4jLogger).debug(msgSupplierCaptor.capture());
        verify(log4jLogger).debug(msgSupplierCaptor.capture(), exceptionCaptor.capture());

        assertArguments();
    }

    public void testTraceMethodsWithSupplier() {
        esLogger.trace(() -> "msg");
        esLogger.trace(() -> "msg1", thrown);

        verify(log4jLogger).trace(msgSupplierCaptor.capture());
        verify(log4jLogger).trace(msgSupplierCaptor.capture(), exceptionCaptor.capture());

        assertArguments();
    }

    public void testLogMethodsDelegationAndLevelMapping() {
        ArgumentCaptor<org.apache.logging.log4j.Level> log4jLevelCaptor = ArgumentCaptor.forClass(org.apache.logging.log4j.Level.class);
        ArgumentCaptor<String> msgCaptor = ArgumentCaptor.forClass(String.class);

        for (Level value : Level.values()) {
            esLogger.log(value, "msg");
        }
        esLogger.log(Level.DEBUG, () -> "msg1", thrown);

        verify(log4jLogger, times(Level.values().length)).log(log4jLevelCaptor.capture(), msgCaptor.capture());
        verify(log4jLogger).log(log4jLevelCaptor.capture(), msgSupplierCaptor.capture(), exceptionCaptor.capture());

        assertThat(log4jLevelCaptor.getAllValues(), contains(log4jValues()));
        assertThat(msgCaptor.getValue(), equalTo("msg"));

        assertThat(msgSupplierCaptor.getAllValues().stream().map(Supplier::get).collect(Collectors.toList()), contains("msg1"));
        assertThat(exceptionCaptor.getValue(), equalTo(thrown));
    }

    private org.apache.logging.log4j.Level[] log4jValues() {
        return new org.apache.logging.log4j.Level[] {
            org.apache.logging.log4j.Level.OFF,
            org.apache.logging.log4j.Level.FATAL,
            org.apache.logging.log4j.Level.ERROR,
            org.apache.logging.log4j.Level.WARN,
            org.apache.logging.log4j.Level.INFO,
            org.apache.logging.log4j.Level.DEBUG,
            org.apache.logging.log4j.Level.TRACE,
            org.apache.logging.log4j.Level.ALL,
            org.apache.logging.log4j.Level.DEBUG // a call with exception
        };
    }

    public void testIsLevelEnabled() {
        when(log4jLogger.isEnabled(eq(org.apache.logging.log4j.Level.INFO))).thenReturn(true);
        esLogger.isEnabled(Level.INFO);
        verify(log4jLogger).isEnabled(org.apache.logging.log4j.Level.INFO);
    }
}
