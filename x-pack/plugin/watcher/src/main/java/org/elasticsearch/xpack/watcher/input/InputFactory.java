/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.input;

import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.watcher.input.ExecutableInput;
import org.elasticsearch.xpack.core.watcher.input.Input;

import java.io.IOException;

/**
 * Parses xcontent to a concrete input of the same type.
 */
public abstract class InputFactory<I extends Input, R extends Input.Result, E extends ExecutableInput<I, R>> {
    /**
     * @return  The type of the input
     */
    public abstract String type();

    /**
     * Parses the given xcontent and creates a concrete input
     *
     * @param watchId               The id of the watch
     * @param parser                The parser containing the input content of the watch
     */
    public abstract I parseInput(String watchId, XContentParser parser) throws IOException;

    /**
     * Creates an executable input out of the given input.
     */
    public abstract E createExecutable(I input);

    public E parseExecutable(String watchId, XContentParser parser) throws IOException {
        I input = parseInput(watchId, parser);
        return createExecutable(input);
    }
}
