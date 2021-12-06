/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.pattern.LogEventPatternConverter;

import java.util.Objects;

/**
 * An abstract class that formats a header value from THREAD_CONTEXT
 */
public abstract class ThreadContextBasedConverter extends LogEventPatternConverter {
    private String headerName;

    public ThreadContextBasedConverter(String key, String headerName) {
        super(key, key);
        this.headerName = headerName;
    }

    /**
     * Formats the header value into json fields.
     *
     * @param event - a log event is ignored in this method as it uses the value
     *              from ThreadContext
     */
    @Override
    public void format(LogEvent event, StringBuilder toAppendTo) {
        String value = getValue();
        if (value != null) {
            toAppendTo.append(value);
        }
    }

    private String getValue() {
        return HeaderWarning.THREAD_CONTEXT.stream()
            .map(t -> t.<String>getHeader(headerName))
            .filter(Objects::nonNull)
            .findFirst()
            .orElse(null);
    }
}
