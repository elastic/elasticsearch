/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transform;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.watcher.execution.WatchExecutionContext;
import org.elasticsearch.watcher.watch.Payload;

import java.io.IOException;

/**
 *
 */
public abstract class ExecutableTransform<T extends Transform, R extends Transform.Result> implements ToXContent {

    protected final T transform;
    protected final ESLogger logger;

    public ExecutableTransform(T transform, ESLogger logger) {
        this.transform = transform;
        this.logger = logger;
    }

    public final String type() {
        return transform.type();
    }

    public T transform() {
        return transform;
    }

    public abstract R execute(WatchExecutionContext ctx, Payload payload);

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return transform.toXContent(builder, params);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ExecutableTransform<?, ?> that = (ExecutableTransform<?, ?>) o;

        return transform.equals(that.transform);
    }

    @Override
    public int hashCode() {
        return transform.hashCode();
    }

}
