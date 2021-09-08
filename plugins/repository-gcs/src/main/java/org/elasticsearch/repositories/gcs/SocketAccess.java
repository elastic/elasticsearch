/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.gcs;

import org.elasticsearch.SpecialPermission;
import org.elasticsearch.core.CheckedRunnable;

import java.io.IOException;
import java.net.SocketPermission;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

/**
 * This plugin uses google api/client libraries to connect to google cloud services. For these remote calls the plugin
 * needs {@link SocketPermission} 'connect' to establish connections. This class wraps the operations requiring access
 * in {@link AccessController#doPrivileged(PrivilegedAction)} blocks.
 */
final class SocketAccess {

    private SocketAccess() {}

    public static <T> T doPrivilegedIOException(PrivilegedExceptionAction<T> operation) throws IOException {
        SpecialPermission.check();
        try {
            return AccessController.doPrivileged(operation);
        } catch (PrivilegedActionException e) {
            throw causeAsIOException(e);
        }
    }

    public static void doPrivilegedVoidIOException(CheckedRunnable<IOException> action) throws IOException {
        SpecialPermission.check();
        try {
            AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                action.run();
                return null;
            });
        } catch (PrivilegedActionException e) {
            throw causeAsIOException(e);
        }
    }

    private static IOException causeAsIOException(PrivilegedActionException e) {
        final Throwable cause = e.getCause();
        if (cause instanceof IOException) {
            return (IOException) cause;
        }
        if (cause instanceof RuntimeException) {
            throw (RuntimeException) cause;
        }
        throw new RuntimeException(cause);
    }
}
