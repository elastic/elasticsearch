/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging.impl.testing;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.filter.RegexFilter;
import org.elasticsearch.logging.core.Appender;
import org.elasticsearch.logging.core.Filter;
import org.elasticsearch.logging.core.Layout;
import org.elasticsearch.logging.core.MockLogAppender;
import org.elasticsearch.logging.impl.LogEventImpl;

import java.util.List;

public class MockLogAppenderImpl extends AbstractAppender implements Appender {

    private List<MockLogAppender.LoggingExpectation> expectations;

    public MockLogAppenderImpl(List<MockLogAppender.LoggingExpectation> expectations) throws IllegalAccessException {
        super("mock", RegexFilter.createFilter(".*(\n.*)*", new String[0], false, null, null), null, false);
        this.expectations = expectations;
    }

    @Override
    public void append(LogEvent event) {
        this.append(new LogEventImpl(event));
    }

    @Override
    public void append(org.elasticsearch.logging.core.LogEvent event) {
        for (MockLogAppender.LoggingExpectation expectation : expectations) {
            expectation.match(event);
        }
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
