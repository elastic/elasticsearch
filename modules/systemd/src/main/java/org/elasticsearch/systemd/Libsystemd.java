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

package org.elasticsearch.systemd;

import com.sun.jna.Native;

import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * Provides access to the native method sd_notify from libsystemd.
 */
class Libsystemd {

    static {
        AccessController.doPrivileged((PrivilegedAction<Object>) () -> {
            Native.register(Libsystemd.class, "libsystemd.so.0");
            return null;
        });
    }

    /**
     * Notify systemd of state changes.
     *
     * @param unset_environment if non-zero, the NOTIFY_SOCKET environment variable will be unset before returning and further calls to
     *                          sd_notify will fail
     * @param state             a new-line separated list of variable assignments; some assignments are understood directly by systemd
     * @return a negative error code on failure, and positive if status was successfully sent
     */
    static native int sd_notify(int unset_environment, String state);

}
