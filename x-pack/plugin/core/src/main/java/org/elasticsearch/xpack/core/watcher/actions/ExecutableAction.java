/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.watcher.actions;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.watch.Payload;

import java.io.IOException;

public abstract class ExecutableAction<A extends Action> implements ToXContentObject {

    protected final A action;
    protected final Logger logger;

    protected ExecutableAction(A action, Logger logger) {
        this.action = action;
        this.logger = logger;
    }

    /**
     * @return the type of this action
     */
    public String type() {
        return action.type();
    }

    public A action() {
        return action;
    }

    /**
     * yack... needed to expose that for testing purposes
     */
    public Logger logger() {
        return logger;
    }

    public abstract Action.Result execute(String actionId, WatchExecutionContext context, Payload payload) throws Exception;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ExecutableAction<? extends Action> that = (ExecutableAction<? extends Action>) o;

        return action.equals(that.action);
    }

    @Override
    public int hashCode() {
        return action.hashCode();
    }

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return action.toXContent(builder, params);
    }

}
