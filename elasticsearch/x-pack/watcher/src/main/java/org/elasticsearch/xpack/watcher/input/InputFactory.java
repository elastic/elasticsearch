/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.input;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 * Parses xcontent to a concrete input of the same type.
 */
public abstract class InputFactory<I extends Input, R extends Input.Result, E extends ExecutableInput<I, R>> {

    protected final Logger inputLogger;

    public InputFactory(Logger inputLogger) {
        this.inputLogger = inputLogger;
    }

    /**
     * @return  The type of the input
     */
    public abstract String type();

    /**
     * Parses the given xcontent and creates a concrete input
     *
     * @param watchId               The id of the watch
     * @param parser                The parser containing the input content of the watch
     * @param upgradeInputSource    Whether to upgrade the source related to the inpit if that source is in legacy format
     *                              Note: depending on the version, only input implementations that have a known legacy
     *                              format will support this option, otherwise this is a noop.
     */
    public abstract I parseInput(String watchId, XContentParser parser, boolean upgradeInputSource) throws IOException;

    /**
     * Creates an executable input out of the given input.
     */
    public abstract E createExecutable(I input);

    public E parseExecutable(String watchId, XContentParser parser, boolean upgradeInputSource) throws IOException {
        I input = parseInput(watchId, parser, upgradeInputSource);
        return createExecutable(input);
    }
}
