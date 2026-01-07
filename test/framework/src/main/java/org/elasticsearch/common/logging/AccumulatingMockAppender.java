/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.filter.RegexFilter;

import java.util.ArrayList;
import java.util.List;

/**
 * Mock log appender class that will capture all the events and store them.
 */
public class AccumulatingMockAppender extends AbstractAppender {
    public final List<LogEvent> events = new ArrayList<>();

    public AccumulatingMockAppender(final String name) throws IllegalAccessException {
        super(name, RegexFilter.createFilter(".*(\n.*)*", new String[0], false, null, null), null, false);
    }

    @Override
    public void append(LogEvent event) {
        events.add(event.toImmutable());
    }

    public void reset() {
        events.clear();
    }
}
