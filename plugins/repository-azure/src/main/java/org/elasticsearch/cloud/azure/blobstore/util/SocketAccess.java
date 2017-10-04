/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cloud.azure.blobstore.util;

import com.microsoft.azure.storage.StorageException;
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

    public static <T> T doPrivilegedException(PrivilegedExceptionAction<T> operation) throws StorageException, URISyntaxException {
        SpecialPermission.check();
        try {
            return AccessController.doPrivileged(operation);
        } catch (PrivilegedActionException e) {
            throw (StorageException) e.getCause();
        }
    }

    public static void doPrivilegedVoidException(StorageRunnable action) throws StorageException, URISyntaxException {
        SpecialPermission.check();
        try {
            AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                action.executeCouldThrow();
                return null;
            });
        } catch (PrivilegedActionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof StorageException) {
                throw (StorageException) cause;
            } else {
                throw (URISyntaxException) cause;
            }
        }
    }

    @FunctionalInterface
    public interface StorageRunnable {
        void executeCouldThrow() throws StorageException, URISyntaxException, IOException;
    }

}
