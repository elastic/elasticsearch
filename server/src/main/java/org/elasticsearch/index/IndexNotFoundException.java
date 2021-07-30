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

public final class IndexNotFoundException extends ResourceNotFoundException {
    /**
     * Construct with a custom message.
     */
    public IndexNotFoundException(String message, String index) {
        super("no such index [" + index + "] and " + message);
        setIndex(index);
    }

    public IndexNotFoundException(String index) {
        this(index, (Throwable) null);
    }

    public IndexNotFoundException(String index, Throwable cause) {
        super("no such index [" + index + "]", cause);
        setIndex(index);
    }

    public IndexNotFoundException(Index index) {
        this(index, null);
    }

    public IndexNotFoundException(Index index, Throwable cause) {
        super("no such index [" + index.getName() + "]", cause);
        setIndex(index);
    }

    public IndexNotFoundException(StreamInput in) throws IOException {
        super(in);
    }
}
