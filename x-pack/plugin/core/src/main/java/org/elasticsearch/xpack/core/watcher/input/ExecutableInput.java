/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.watcher.input;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.watch.Payload;

import java.io.IOException;

public abstract class ExecutableInput<I extends Input, R extends Input.Result> implements ToXContentObject {

    protected final I input;

    protected ExecutableInput(I input) {
        this.input = input;
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
    public abstract R execute(WatchExecutionContext ctx, @Nullable Payload payload);

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
