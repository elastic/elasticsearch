/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.watcher.execution.WatchExecutionContext;
import org.elasticsearch.watcher.watch.Payload;

import java.io.IOException;

/**
 */
public abstract class ExecutableAction<A extends Action> implements ToXContent {

    protected final A action;
    protected final ESLogger logger;

    protected ExecutableAction(A action, ESLogger logger) {
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
    public ESLogger logger() {
        return logger;
    }

    public abstract Action.Result execute(String actionId, WatchExecutionContext context, Payload payload) throws Exception;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ExecutableAction that = (ExecutableAction) o;

        return action.equals(that.action);
    }

    @Override
    public int hashCode() {
        return action.hashCode();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return action.toXContent(builder, params);
    }

}
