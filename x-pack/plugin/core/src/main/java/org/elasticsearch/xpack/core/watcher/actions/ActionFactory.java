/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.watcher.actions;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

/**
 * Parses xcontent to a concrete action of the same type.
 */
public abstract class ActionFactory {

    protected final Logger actionLogger;

    protected ActionFactory(Logger actionLogger) {
        this.actionLogger = actionLogger;
    }

    /**
     * Parses the given xcontent and creates a concrete action
     */
    public abstract ExecutableAction<? extends Action> parseExecutable(String watchId, String actionId, XContentParser parser)
        throws IOException;
}
