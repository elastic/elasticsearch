/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging.internal.testing;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.filter.RegexFilter;
import org.elasticsearch.logging.api.core.MockLogAppender;
import org.elasticsearch.logging.internal.LogEventImpl;

import java.util.List;

public class MockLogAppenderImpl extends AbstractAppender {

    private List<MockLogAppender.LoggingExpectation> expectations;

    public MockLogAppenderImpl(List<MockLogAppender.LoggingExpectation> expectations) throws IllegalAccessException {
        super("mock", RegexFilter.createFilter(".*(\n.*)*", new String[0], false, null, null), null, false);
        this.expectations = expectations;
    }

    @Override
    public void append(LogEvent event) {
        for (MockLogAppender.LoggingExpectation expectation : expectations) {
            expectation.match(new LogEventImpl(event));
        }
    }

}
