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

    public static <T> T doPrivilegedIOException(final PrivilegedExceptionAction<T> operation)
            throws IOException {
        SpecialPermission.check();
        try {
            return AccessController.doPrivileged(operation);
        } catch (final PrivilegedActionException e) {
            throw (IOException) e.getCause();
        }
    }

}
