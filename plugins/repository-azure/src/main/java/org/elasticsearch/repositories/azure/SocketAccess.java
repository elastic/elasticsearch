/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.azure;

import org.apache.logging.log4j.core.util.Throwables;
import org.elasticsearch.SpecialPermission;

import java.io.IOException;
import java.net.SocketPermission;
import java.net.URISyntaxException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

/**
 * This plugin uses azure libraries to connect to azure storage services. For these remote calls the plugin needs
 * {@link SocketPermission} 'connect' to establish connections. This class wraps the operations requiring access in
 * {@link AccessController#doPrivileged(PrivilegedAction)} blocks.
 */
public final class SocketAccess {

    private SocketAccess() {}

    public static <T> T doPrivilegedException(PrivilegedExceptionAction<T> operation) {
        SpecialPermission.check();
        try {
            return AccessController.doPrivileged(operation);
        } catch (PrivilegedActionException e) {
            Throwables.rethrow(e.getCause());
            assert false : "always throws";
            return null;
        }
    }

    public static void doPrivilegedVoidException(StorageRunnable action) {
        SpecialPermission.check();
        try {
            AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                action.executeCouldThrow();
                return null;
            });
        } catch (PrivilegedActionException e) {
            Throwables.rethrow(e.getCause());
        }
    }

    @FunctionalInterface
    public interface StorageRunnable {
        void executeCouldThrow() throws URISyntaxException, IOException;
    }

}
