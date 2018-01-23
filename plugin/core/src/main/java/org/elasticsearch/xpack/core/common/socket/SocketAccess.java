/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.common.socket;

import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.CheckedRunnable;
import org.elasticsearch.common.CheckedSupplier;

import java.io.IOException;
import java.net.SocketPermission;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

/**
 * X-pack uses various libraries that establish socket connections. For these remote calls the plugin requires
 * {@link SocketPermission} 'connect' to establish connections. This class wraps the operations requiring access in
 * {@link AccessController#doPrivileged(PrivilegedAction)} blocks.
 */
public final class SocketAccess {

    private SocketAccess() {
    }

    public static <R> R doPrivileged(CheckedSupplier<R, IOException> supplier) throws IOException {
        SpecialPermission.check();
        try {
            return AccessController.doPrivileged((PrivilegedExceptionAction<R>) supplier::get);
        } catch (PrivilegedActionException e) {
            throw (IOException) e.getCause();
        }
    }

    public static void doPrivileged(CheckedRunnable<IOException> action) throws IOException {
        SpecialPermission.check();
        try {
            AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                action.run();
                return null;
            });
        } catch (PrivilegedActionException e) {
            throw (IOException) e.getCause();
        }
    }
}
