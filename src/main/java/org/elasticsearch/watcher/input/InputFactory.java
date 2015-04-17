/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.input;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 * Parses xcontent to a concrete input of the same type.
 */
public abstract class InputFactory<I extends Input, R extends Input.Result, E extends ExecutableInput<I, R>> {

    protected final ESLogger inputLogger;

    public InputFactory(ESLogger inputLogger) {
        this.inputLogger = inputLogger;
    }

    /**
     * @return  The type of the input
     */
    public abstract String type();

    /**
     * Parses the given xcontent and creates a concrete input
     */
    public abstract I parseInput(String watchId, XContentParser parser) throws IOException;

    /**
     * Parses the given xContent and creates a concrete result
     */
    public abstract R parseResult(String watchId, XContentParser parser) throws IOException;

    /**
     * Creates an executable input out of the given input.
     */
    public abstract E createExecutable(I input);

    public ExecutableInput parseExecutable(String watchId, XContentParser parser) throws IOException {
        I input = parseInput(watchId, parser);
        return createExecutable(input);
    }
}
