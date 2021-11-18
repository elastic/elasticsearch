/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.watcher.transform;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public abstract class TransformFactory<T extends Transform, R extends Transform.Result, E extends ExecutableTransform<T, R>> {

    protected final Logger transformLogger;

    public TransformFactory(Logger transformLogger) {
        this.transformLogger = transformLogger;
    }

    /**
     * @return  The type of the transform
     */
    public abstract String type();

    /**
     * Parses the given xcontent and creates a concrete transform
     *
     * @param watchId                   The id of the watch
     * @param parser                    The parsing that contains the condition content
     */
    public abstract T parseTransform(String watchId, XContentParser parser) throws IOException;

    /**
     * Creates an executable transform out of the given transform.
     */
    public abstract E createExecutable(T transform);

    public E parseExecutable(String watchId, XContentParser parser) throws IOException {
        T transform = parseTransform(watchId, parser);
        return createExecutable(transform);
    }
}
