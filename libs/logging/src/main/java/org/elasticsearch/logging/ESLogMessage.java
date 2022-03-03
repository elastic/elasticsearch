/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging;

import org.elasticsearch.logging.internal.ESLogMessageImpl;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Custom logger messages. Carries additional fields which will populate JSON fields in logs.
 */
// TODO: PG the same as deprecationmessage. probably an implementation detail
public final class ESLogMessage implements Message {

    private final ESLogMessageImpl impl;

    public ESLogMessage() {
        impl = new ESLogMessageImpl();
    }

    public ESLogMessage(String messagePattern, Object... args) {
        impl = new ESLogMessageImpl(messagePattern, args);

    }

    public ESLogMessage argAndField(String key, Object value) {
        impl.argAndField(key, value);
        return this;
    }

    public ESLogMessage withFields(Map<String, Object> jsonFields) {
        impl.withFields(jsonFields);
        return this;
    }

    public ESLogMessage field(String key, Object value) {
        impl.field(key, value);
        return this;
    }

    public String get(String key) {
        return impl.get(key);
    }

    @Override
    public String getFormattedMessage() {
        return impl.getFormattedMessage();
    }

    @Override
    public String getFormat() {
        return impl.getFormat();
    }

    @Override
    public Object[] getParameters() {
        return impl.getParameters();
    }

    @Override
    public Throwable getThrowable() {
        return impl.getThrowable();
    }

    public static String asJsonArray(Stream<String> stream) {
        return "[" + stream.map(ESLogMessageImpl::inQuotes).collect(Collectors.joining(", ")) + "]";
    }

    // static ESLogMessage of() {
    // return new ESLogMessageImpl();
    // }
    //
    // static ESLogMessage of(String messagePattern, Object... args) {
    // return new ESLogMessageImpl(messagePattern, args);
    // }
}
