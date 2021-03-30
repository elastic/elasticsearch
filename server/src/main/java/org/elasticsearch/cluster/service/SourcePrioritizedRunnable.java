/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.service;

import org.elasticsearch.common.Priority;
import org.elasticsearch.common.util.concurrent.PrioritizedRunnable;

/**
 * PrioritizedRunnable that also has a source string
 */
public abstract class SourcePrioritizedRunnable extends PrioritizedRunnable {
    protected final String source;

    public SourcePrioritizedRunnable(Priority priority, String source) {
        super(priority);
        this.source = source;
    }

    public String source() {
        return source;
    }

    @Override
    public String toString() {
        return "[" + source + "]";
    }
}
