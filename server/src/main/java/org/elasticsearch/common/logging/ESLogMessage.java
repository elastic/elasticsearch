/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.common.SuppressLoggerChecks;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A base class for custom log4j logger messages. Carries additional fields which will populate JSON fields in logs.
 */
@SuppressLoggerChecks(reason = "Safe as this is abstract class")
public abstract class ESLogMessage extends ParameterizedMessage {
    private final Map<String, Object> fields;

    /**
     * This is an abstract class, so this is safe. The check is done on DeprecationMessage.
     * Other subclasses are not allowing varargs
     */
    public ESLogMessage(Map<String, Object> fields, String messagePattern, Object... args) {
        super(messagePattern, args);
        this.fields = fields;
    }

    public String getValueFor(String key) {
        Object value = fields.get(key);
        return value != null ? value.toString() : null;
    }

    public static String inQuotes(String s) {
        if (s == null) return inQuotes("");
        return "\"" + s + "\"";
    }

    public static String inQuotes(Object s) {
        if (s == null) return inQuotes("");
        return inQuotes(s.toString());
    }

    public static String asJsonArray(Stream<String> stream) {
        return "[" + stream.map(ESLogMessage::inQuotes).collect(Collectors.joining(", ")) + "]";
    }

    public Object[] getArguments() {
        return super.getParameters();
    }

    public String getMessagePattern() {
        return super.getFormat();
    }
}
