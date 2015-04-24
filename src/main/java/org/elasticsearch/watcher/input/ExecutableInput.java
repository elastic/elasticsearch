/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.input;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.watcher.execution.WatchExecutionContext;

import java.io.IOException;

/**
 *
 */
public abstract class ExecutableInput<I extends Input, R extends Input.Result> implements ToXContent {

    protected final I input;
    protected final ESLogger logger;

    protected ExecutableInput(I input, ESLogger logger) {
        this.input = input;
        this.logger = logger;
    }

    /**
     * @return the type of this input
     */
    public final String type() {
        return input.type();
    }

    public I input() {
        return input;
    }

    /**
     * Executes this input
     */
    public abstract R execute(WatchExecutionContext ctx) throws IOException;

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return input.toXContent(builder, params);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ExecutableInput<?, ?> that = (ExecutableInput<?, ?>) o;

        return input.equals(that.input);
    }

    @Override
    public int hashCode() {
        return input.hashCode();
    }
}
