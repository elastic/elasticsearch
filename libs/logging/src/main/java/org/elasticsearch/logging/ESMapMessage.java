/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging;

import org.elasticsearch.logging.internal.ESLogMessage;

import java.util.Map;

public class ESMapMessage implements Message {
    private ESLogMessage esLogMessage;

    public ESMapMessage(String pattern) {
        this.esLogMessage = new ESLogMessage(pattern);
    }

    public ESMapMessage argAndField(String key, Object value) {
        esLogMessage.argAndField(key, value);
        return this;
    }

    public ESMapMessage field(String key, Object value) {
        esLogMessage.field(key, value);
        return this;
    }

    public ESMapMessage withFields(Map<String, Object> prepareMap) {
        esLogMessage.withFields(prepareMap);
        return this;
    }

    public Object[] getArguments() {
        return esLogMessage.getArguments();
    }

    public String getMessagePattern() {
        return esLogMessage.getMessagePattern();
    }

    @Override
    public String getFormattedMessage() {
        return esLogMessage.getFormattedMessage();
    }

    @Override
    public String getFormat() {
        return esLogMessage.getFormat();
    }

    @Override
    public Object[] getParameters() {
        return esLogMessage.getParameters();
    }

    @Override
    public Throwable getThrowable() {
        return esLogMessage.getThrowable();
    }
}
