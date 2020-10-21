/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.support;

import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import org.elasticsearch.SpecialPermission;

/**
 * A <em>try-with-resource</em> compatible object for configuring a thread {@link Thread#getContextClassLoader()}.
 * On construction this class will set the current (or provided) thread's context class loader.
 * On {@link #close()}, it restores the previous value of the class loader.
 */
public class RestorableContextClassLoader implements AutoCloseable {

    private final Thread thread;
    private ClassLoader restore;

    public RestorableContextClassLoader(Class<?> fromClass) throws PrivilegedActionException {
        this(Thread.currentThread(), fromClass.getClassLoader());
    }

    public RestorableContextClassLoader(Thread thread, ClassLoader setClassLoader) throws PrivilegedActionException {
        this.thread = thread;
        SpecialPermission.check();
        AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
            restore = thread.getContextClassLoader();
            thread.setContextClassLoader(setClassLoader);
            return null;
        });
    }

    @Override
    public void close() throws PrivilegedActionException {
        SpecialPermission.check();
        AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
            this.thread.setContextClassLoader(this.restore);
            return null;
        });
    }
}
