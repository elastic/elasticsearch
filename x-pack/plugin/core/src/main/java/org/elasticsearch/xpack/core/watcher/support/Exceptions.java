/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.watcher.support;

import java.io.IOException;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;

public class Exceptions {

    private Exceptions() {
    }

    public static IllegalArgumentException illegalArgument(String msg, Object... args) {
        return new IllegalArgumentException(format(msg, args));
    }

    public static IllegalArgumentException illegalArgument(String msg, Throwable cause, Object... args) {
        return new IllegalArgumentException(format(msg, args), cause);
    }

    public static IllegalStateException illegalState(String msg, Object... args) {
        return new IllegalStateException(format(msg, args));
    }

    public static IllegalStateException illegalState(String msg, Throwable cause, Object... args) {
        return new IllegalStateException(format(msg, args), cause);
    }

    public static IOException ioException(String msg, Object... args) {
        return new IOException(format(msg, args));
    }

    public static IOException ioException(String msg, Throwable cause, Object... args) {
        return new IOException(format(msg, args), cause);
    }
}
