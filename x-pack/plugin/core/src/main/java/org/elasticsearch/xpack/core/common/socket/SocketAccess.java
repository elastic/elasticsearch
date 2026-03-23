/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.common.socket;

import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.core.CheckedRunnable;

import java.io.IOException;

/**
 * X-pack uses various libraries that establish socket connections. This class provides
 * a consistent calling convention for those operations.
 */
public final class SocketAccess {

    private SocketAccess() {}

    public static <R> R doPrivileged(CheckedSupplier<R, IOException> supplier) throws IOException {
        return supplier.get();
    }

    public static void doPrivileged(CheckedRunnable<IOException> action) throws IOException {
        action.run();
    }
}
