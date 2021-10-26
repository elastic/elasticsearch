/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.discovery.ec2;

import org.elasticsearch.SpecialPermission;

import java.io.IOException;
import java.net.SocketPermission;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

/**
 * This plugin uses aws libraries to connect to aws services. For these remote calls the plugin needs
 * {@link SocketPermission} 'connect' to establish connections. This class wraps the operations requiring access in
 * {@link AccessController#doPrivileged(PrivilegedAction)} blocks.
 */
final class SocketAccess {

    private SocketAccess() {}

    public static <T> T doPrivileged(PrivilegedAction<T> operation) {
        SpecialPermission.check();
        return AccessController.doPrivileged(operation);
    }

    public static <T> T doPrivilegedIOException(PrivilegedExceptionAction<T> operation) throws IOException {
        SpecialPermission.check();
        try {
            return AccessController.doPrivileged(operation);
        } catch (PrivilegedActionException e) {
            throw (IOException) e.getCause();
        }
    }

}
