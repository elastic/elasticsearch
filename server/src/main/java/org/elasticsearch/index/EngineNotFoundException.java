/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

public final class EngineNotFoundException extends ResourceNotFoundException {
    public EngineNotFoundException(String engine) {
        this(engine, (Throwable) null);
    }

    public EngineNotFoundException(String engine, Throwable cause) {
        super("no such engine [" + engine + "]", cause);
    }

    public EngineNotFoundException(StreamInput in) throws IOException {
        super(in);
    }
}
