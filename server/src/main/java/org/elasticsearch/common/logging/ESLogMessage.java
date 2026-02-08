/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.common.logging;

import org.apache.logging.log4j.message.MapMessage;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.logging.LogMessage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A base class for custom log4j logger messages. Carries additional fields which will populate JSON fields in logs.
 */
public class ESLogMessage extends MapMessage<ESLogMessage, Object> implements LogMessage {
    private final List<Object> arguments = new ArrayList<>();
    private String messagePattern;

    @SuppressWarnings("this-escape")
    public ESLogMessage(String messagePattern, Object... args) {
        this();
        Collections.addAll(this.arguments, args);
        this.messagePattern = messagePattern;

        Object message = new Object() {
            @Override
            public String toString() {
                return ParameterizedMessage.format(messagePattern, arguments.toArray());
            }
        };
        with("message", message);
    }

    public ESLogMessage(Map<String, Object> fields) {
        super(fields);
    }

    public ESLogMessage() {
        super();
    }

    public ESLogMessage argAndField(String key, Object value) {
        this.arguments.add(value);
        super.with(key, value);
        return this;
    }

    public ESLogMessage field(String key, Object value) {
        return with(key, value);
    }

    public Object[] getArguments() {
        return arguments.toArray();
    }

    public String getMessagePattern() {
        return messagePattern;
    }

    public ESLogMessage withFields(Map<String, Object> prepareMap) {
        prepareMap.forEach(this::with);
        return this;
    }

}
