/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cloud.gce.util;

import org.elasticsearch.SpecialPermission;

import java.io.IOException;
import java.net.SocketPermission;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

/**
 * GCE's HTTP client changes access levels. Specifically it needs {@link RuntimePermission} {@code
 * accessDeclaredMembers} and {@code setFactory}, and {@link java.lang.reflect.ReflectPermission}
 * {@code suppressAccessChecks}. For remote calls, the plugin needs {@link SocketPermission} for
 * {@code connect}. This class wraps the operations requiring access in
 * {@link AccessController#doPrivileged(PrivilegedAction)} blocks.
 */
public final class Access {

    private Access() {}

    public static <T> T doPrivileged(final PrivilegedAction<T> operation) {
        SpecialPermission.check();
        return AccessController.doPrivileged(operation);
    }

    public static void doPrivilegedVoid(final Runnable action) {
        SpecialPermission.check();
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            action.run();
            return null;
        });
    }

    public static <T> T doPrivilegedIOException(final PrivilegedExceptionAction<T> operation) throws IOException {
        SpecialPermission.check();
        try {
            return AccessController.doPrivileged(operation);
        } catch (final PrivilegedActionException e) {
            throw (IOException) e.getCause();
        }
    }

}
