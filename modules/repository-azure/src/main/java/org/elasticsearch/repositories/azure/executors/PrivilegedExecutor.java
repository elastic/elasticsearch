/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.azure.executors;

import org.elasticsearch.repositories.azure.SocketAccess;

import java.util.concurrent.Executor;

/**
 * Executor that grants security permissions to the tasks executed on it.
 */
public class PrivilegedExecutor implements Executor {
    private final Executor delegate;

    public PrivilegedExecutor(Executor delegate) {
        this.delegate = delegate;
    }

    @Override
    public void execute(Runnable command) {
        delegate.execute(() -> SocketAccess.doPrivilegedVoidException(command::run));
    }
}
