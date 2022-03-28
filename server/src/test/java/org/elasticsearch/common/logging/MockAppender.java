/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.logging;

import org.elasticsearch.logging.Message;
import org.elasticsearch.logging.core.Appender;
import org.elasticsearch.logging.core.Filter;
import org.elasticsearch.logging.core.Layout;
import org.elasticsearch.logging.core.LogEvent;

public class MockAppender implements Appender {
    public LogEvent lastEvent;

    public MockAppender(final String name) throws IllegalAccessException {
        // super(name, RegexFilter.createFilter(".*(\n.*)*", new String[0], false, null, null), null, false);
    }

    // @Override
    // public void append(LogEvent event) {
    // lastEvent = event.toImmutable();
    // }

    Message lastParameterizedMessage() {
        return lastEvent.getMessage();
    }

    public LogEvent getLastEventAndReset() {
        LogEvent toReturn = lastEvent;
        lastEvent = null;
        return toReturn;
    }

    @Override
    public void append(LogEvent event) {
        lastEvent = event;
    }

    @Override
    public Filter filter() {
        return null;
    }

    @Override
    public Layout layout() {
        return null;
    }

    @Override
    public String name() {
        return null;
    }
}
