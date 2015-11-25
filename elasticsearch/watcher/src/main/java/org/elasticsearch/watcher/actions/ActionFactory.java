/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 * Parses xcontent to a concrete action of the same type.
 */
public abstract class ActionFactory<A extends Action, E extends ExecutableAction<A>> {

    protected final ESLogger actionLogger;

    protected ActionFactory(ESLogger actionLogger) {
        this.actionLogger = actionLogger;
    }

    /**
     * @return  The type of the action
     */
    public abstract String type();

    public abstract A parseAction(String watchId, String actionId, XContentParser parser) throws IOException;

    public abstract E createExecutable(A action);

    /**
     * Parses the given xcontent and creates a concrete action
     */
    public E parseExecutable(String watchId, String actionId, XContentParser parser) throws IOException {
        A action = parseAction(watchId, actionId, parser);
        return createExecutable(action);
    }
}
