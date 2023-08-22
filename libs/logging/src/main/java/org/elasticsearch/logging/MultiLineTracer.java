/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Helper class to consolidate multiple trace level statements to a single line with lazy evaluation.
 * If trace level is not enabled, then no work is performed.
 */
public class MultiLineTracer {

    private final Logger logger;
    final List<Object> params = new ArrayList<>();
    final StringBuffer buffer = new StringBuffer();

    public MultiLineTracer(Logger logger){
        this.logger = logger;
    }

    public void push(String s, Object... args) {
        if (logger.isTraceEnabled()) {
            buffer.append(s).append(System.lineSeparator());
            params.addAll(Arrays.asList(args));
        }
    }

    public void flush(){
        logger.trace(buffer.toString(), params);
    }
}
