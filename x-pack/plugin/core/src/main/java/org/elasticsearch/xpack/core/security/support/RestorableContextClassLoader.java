/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.support;

/**
 * A <em>try-with-resource</em> compatible object for configuring a thread {@link Thread#getContextClassLoader()}.
 * On construction this class will set the current (or provided) thread's context class loader.
 * On {@link #close()}, it restores the previous value of the class loader.
 */
public class RestorableContextClassLoader implements AutoCloseable {

    private final Thread thread;
    private final ClassLoader restore;

    public RestorableContextClassLoader(Class<?> fromClass) {
        this(Thread.currentThread(), fromClass.getClassLoader());
    }

    public RestorableContextClassLoader(Thread thread, ClassLoader setClassLoader) {
        this.thread = thread;
        restore = thread.getContextClassLoader();
        thread.setContextClassLoader(setClassLoader);
    }

    @Override
    public void close() {
        thread.setContextClassLoader(restore);
    }
}
