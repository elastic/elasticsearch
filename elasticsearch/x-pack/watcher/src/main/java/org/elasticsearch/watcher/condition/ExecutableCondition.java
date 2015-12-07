/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.condition;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.watcher.execution.WatchExecutionContext;

import java.io.IOException;

/**
 *
 */
public abstract class ExecutableCondition<C extends Condition, R extends Condition.Result> implements ToXContent {

    protected final C condition;
    protected final ESLogger logger;

    protected ExecutableCondition(C condition, ESLogger logger) {
        this.condition = condition;
        this.logger = logger;
    }

    /**
     * @return the type of this condition
     */
    public final String type() {
        return condition.type();
    }

    public C condition() {
        return condition;
    }

    /**
     * Executes this condition
     */
    public abstract R execute(WatchExecutionContext ctx);

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return condition.toXContent(builder, params);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ExecutableCondition<?, ?> that = (ExecutableCondition<?, ?>) o;

        return condition.equals(that.condition);
    }

    @Override
    public int hashCode() {
        return condition.hashCode();
    }
}
