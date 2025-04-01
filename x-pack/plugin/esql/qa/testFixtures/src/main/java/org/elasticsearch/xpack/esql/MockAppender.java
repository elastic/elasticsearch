/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.filter.RegexFilter;
import org.apache.logging.log4j.message.Message;

public class MockAppender extends AbstractAppender {
    public LogEvent lastEvent;

    public MockAppender(final String name) throws IllegalAccessException {
        super(name, RegexFilter.createFilter(".*(\n.*)*", new String[0], false, null, null), null, false);
    }

    @Override
    public void append(LogEvent event) {
        lastEvent = event.toImmutable();
    }

    public Message lastMessage() {
        return lastEvent.getMessage();
    }

    public LogEvent lastEvent() {
        return lastEvent;
    }

    public LogEvent getLastEventAndReset() {
        LogEvent toReturn = lastEvent;
        lastEvent = null;
        return toReturn;
    }
}
