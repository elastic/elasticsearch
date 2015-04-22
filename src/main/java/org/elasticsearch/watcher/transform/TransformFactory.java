/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transform;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 *
 */
public abstract class TransformFactory<T extends Transform, R extends Transform.Result, E extends ExecutableTransform<T, R>> {

    protected final ESLogger transformLogger;

    public TransformFactory(ESLogger transformLogger) {
        this.transformLogger = transformLogger;
    }

    /**
     * @return  The type of the transform
     */
    public abstract String type();

    /**
     * Parses the given xcontent and creates a concrete transform
     */
    public abstract T parseTransform(String watchId, XContentParser parser) throws IOException;

    /**
     * Parses the given xcontent and creates a concrete transform result
     */
    public abstract R parseResult(String watchId, XContentParser parser) throws IOException;

    /**
     * Creates an executable transform out of the given transform.
     */
    public abstract E createExecutable(T transform);

    public E parseExecutable(String watchId, XContentParser parser) throws IOException {
        T transform = parseTransform(watchId, parser);
        return createExecutable(transform);
    }
}
